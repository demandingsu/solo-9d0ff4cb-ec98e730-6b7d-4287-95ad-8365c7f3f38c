"""核心数据层：CSV 校验 + SQLite 事务逻辑（纯标准库，不依赖 FastAPI）。

一致性不变量（多进程共享同一 SQLite 文件时也必须成立）：
1. 预算扣减与查询结果写入在同一个事务中提交 —— 要么同时生效，要么同时回滚，
   不会出现"已扣预算却无结果"或"有结果却没扣预算"。
2. BEGIN IMMEDIATE 在事务开始时即取得写锁，使"查缓存 -> 扣预算 -> 写结果"
   整个序列在多个服务进程之间串行化，两个进程同时查询也不会超预算。
3. 同一社区的再次查询只读持久化的缓存结果：不重复扣额、不换噪声。
4. 导入是一次性的：meta.imported 置位后任何后续上传都会被整体拒绝，
   原数据不会被覆盖；household_id 主键提供第二道防线。
"""

import csv
import io
import math
import os
import random
import sqlite3
import time

# 隐私参数（可用环境变量覆盖，便于压测；生产部署使用默认值即可）
EPSILON_TOTAL = float(os.environ.get("DP_EPSILON_TOTAL", "1.0"))      # 全期隐私预算
EPSILON_PER_QUERY = float(os.environ.get("DP_EPSILON_PER_QUERY", "0.25"))  # 每个社区首次查询扣减
NOISE_SCALE = float(os.environ.get("DP_NOISE_SCALE", "4.0"))          # 拉普拉斯噪声尺度 b

_rng = random.SystemRandom()


class BudgetExhausted(Exception):
    """隐私预算已用尽。"""


class AlreadyImported(Exception):
    """数据已成功导入过，不允许覆盖。"""


class NotImported(Exception):
    """尚未导入数据。"""


def laplace_noise(scale=None):
    """逆变换采样 Laplace(0, scale)。敏感度为 1、b=4 时单次查询 epsilon = 1/4 = 0.25。"""
    if scale is None:
        scale = NOISE_SCALE
    while True:
        u = _rng.random() - 0.5
        if abs(u) < 0.5:  # 排除端点，避免 log(0)
            break
    return -scale * math.copysign(1.0, u) * math.log1p(-2.0 * abs(u))


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    imported INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS households (
    household_id TEXT PRIMARY KEY,
    community TEXT NOT NULL,
    participated INTEGER NOT NULL CHECK (participated IN (0, 1))
);
CREATE TABLE IF NOT EXISTS results (
    community TEXT PRIMARY KEY,
    noisy_count REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS budget (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total REAL NOT NULL,
    spent REAL NOT NULL DEFAULT 0.0
);
"""


def connect(db_path):
    """每次请求新建连接：多进程下最安全。WAL + 长 busy_timeout 保证锁等待而非报错。"""
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def init_db(db_path):
    """建表并播种单行 meta/budget。多进程同时启动时重试等待锁。"""
    directory = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(directory, exist_ok=True)
    last = None
    for _ in range(30):
        try:
            conn = connect(db_path)
            try:
                conn.executescript(SCHEMA)
                conn.execute("INSERT OR IGNORE INTO meta (id, imported) VALUES (1, 0)")
                conn.execute(
                    "INSERT OR IGNORE INTO budget (id, total, spent) VALUES (1, ?, 0.0)",
                    (EPSILON_TOTAL,),
                )
            finally:
                conn.close()
            return
        except sqlite3.OperationalError as exc:
            last = exc
            time.sleep(0.2)
    raise RuntimeError(f"数据库初始化失败: {last}")


# ---------------------------------------------------------------- CSV 校验

HEADER_ALIASES = {
    "household_id": {"household_id", "household", "id", "住户号", "住户编号"},
    "community": {"community", "社区", "社区名称"},
    "participated": {"participated", "participate", "是否参与", "参与"},
}
TRUE_VALUES = {"1", "true", "t", "yes", "y", "是", "参与"}
FALSE_VALUES = {"0", "false", "f", "no", "n", "否", "未参与"}


def parse_csv(raw):
    """解析并校验整份 CSV，返回 [(household_id, community, participated)]。

    任一住户号重复 -> ValueError（整份拒收，不写库）。
    """
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("文件必须是 UTF-8 编码") from exc
    rows = [r for r in csv.reader(io.StringIO(text)) if any(c.strip() for c in r)]
    if not rows:
        raise ValueError("CSV 内容为空")
    header = [h.strip().lower() for h in rows[0]]
    col = {}
    for field, aliases in HEADER_ALIASES.items():
        idx = next((i for i, h in enumerate(header) if h in aliases), None)
        if idx is None:
            raise ValueError(
                f"缺少必需列 {field}，可用表头：{' / '.join(sorted(aliases))}"
            )
        col[field] = idx
    width = max(col.values())
    records, seen, dups = [], set(), set()
    for lineno, r in enumerate(rows[1:], start=2):
        if len(r) <= width:
            raise ValueError(f"第 {lineno} 行列数不足")
        hid = r[col["household_id"]].strip()
        comm = r[col["community"]].strip()
        raw_flag = r[col["participated"]].strip().lower()
        if not hid:
            raise ValueError(f"第 {lineno} 行住户号为空")
        if not comm:
            raise ValueError(f"第 {lineno} 行社区名称为空")
        if raw_flag in TRUE_VALUES:
            flag = 1
        elif raw_flag in FALSE_VALUES:
            flag = 0
        else:
            raise ValueError(f"第 {lineno} 行“是否参与”取值无法识别：{raw_flag!r}")
        if hid in seen:
            dups.add(hid)
        else:
            seen.add(hid)
            records.append((hid, comm, flag))
    if dups:
        sample = "、".join(sorted(dups)[:5])
        raise ValueError(f"发现重复住户号（{sample} 等 {len(dups)} 个），整份文件已拒收")
    if not records:
        raise ValueError("CSV 中没有数据行")
    return records


# ---------------------------------------------------------------- 导入

def import_records(db_path, records):
    """一次性导入。已成功导入过则抛 AlreadyImported，绝不覆盖原数据。"""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        imported = conn.execute(
            "SELECT imported FROM meta WHERE id = 1"
        ).fetchone()["imported"]
        if imported:
            conn.execute("ROLLBACK")
            raise AlreadyImported("数据已成功导入过，为防覆盖已拒绝本次上传")
        conn.executemany(
            "INSERT INTO households (household_id, community, participated)"
            " VALUES (?, ?, ?)",
            records,
        )
        conn.execute("UPDATE meta SET imported = 1 WHERE id = 1")
        conn.execute("COMMIT")
        return len(records)
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------- 查询

def query_community(db_path, community):
    """查询某社区参与户数（含拉普拉斯噪声）。

    首次查询：在同一事务内"扣预算 + 写结果"，原子生效；
    再次查询：返回持久化的同一结果，不重复扣额、不换噪声。
    """
    community = (community or "").strip()
    if not community:
        raise ValueError("社区名称不能为空")
    conn = connect(db_path)
    try:
        # BEGIN IMMEDIATE：立即取得写锁，多进程并发时在此串行化
        conn.execute("BEGIN IMMEDIATE")

        if not conn.execute("SELECT imported FROM meta WHERE id = 1").fetchone()["imported"]:
            conn.execute("ROLLBACK")
            raise NotImported("尚未导入数据，无法查询")

        row = conn.execute(
            "SELECT noisy_count FROM results WHERE community = ?", (community,)
        ).fetchone()
        if row is not None:  # 缓存命中：不扣预算，直接返回同一结果
            budget = conn.execute("SELECT total, spent FROM budget WHERE id = 1").fetchone()
            conn.execute("COMMIT")
            return {
                "community": community,
                "noisy_count": row["noisy_count"],
                "cached": True,
                "charged": 0.0,
                "budget_total": budget["total"],
                "budget_spent": budget["spent"],
            }

        # 原子扣预算：仅当剩余预算足够时才更新；0 行受影响即预算不足
        cur = conn.execute(
            "UPDATE budget SET spent = spent + ? WHERE id = 1 AND spent + ? <= total",
            (EPSILON_PER_QUERY, EPSILON_PER_QUERY),
        )
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            raise BudgetExhausted(
                f"隐私预算已用尽（全期 ε={EPSILON_TOTAL}，每个新社区扣 {EPSILON_PER_QUERY}）"
            )

        exact = conn.execute(
            "SELECT COUNT(*) AS c FROM households WHERE community = ? AND participated = 1",
            (community,),
        ).fetchone()["c"]
        noisy = exact + laplace_noise()
        # 与预算扣减同事务写入：要么一起生效，要么一起回滚
        conn.execute(
            "INSERT INTO results (community, noisy_count) VALUES (?, ?)",
            (community, noisy),
        )
        budget = conn.execute("SELECT total, spent FROM budget WHERE id = 1").fetchone()
        conn.execute("COMMIT")
        return {
            "community": community,
            "noisy_count": noisy,
            "cached": False,
            "charged": EPSILON_PER_QUERY,
            "budget_total": budget["total"],
            "budget_spent": budget["spent"],
        }
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def get_status(db_path):
    """仅暴露元信息：导入标志、预算、缓存社区数。不暴露任何原始记录或精确计数。"""
    conn = connect(db_path)
    try:
        imported = bool(
            conn.execute("SELECT imported FROM meta WHERE id = 1").fetchone()["imported"]
        )
        budget = conn.execute("SELECT total, spent FROM budget WHERE id = 1").fetchone()
        cached = conn.execute("SELECT COUNT(*) AS c FROM results").fetchone()["c"]
        return {
            "imported": imported,
            "budget_total": budget["total"],
            "budget_spent": budget["spent"],
            "budget_remaining": round(budget["total"] - budget["spent"], 4),
            "cached_communities": cached,
        }
    finally:
        conn.close()
