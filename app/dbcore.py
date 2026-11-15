"""核心数据层：CSV 校验 + SQLite 事务逻辑（纯标准库，不依赖 FastAPI）。

一致性不变量（多进程共享同一 SQLite 文件时也必须成立）：
1. 预算扣减、带噪结果与发布明细（releases 一行）在同一个事务中提交 ——
   要么三者同时生效，要么同时回滚，不会出现"已扣预算却无结果/无明细"或
   "有结果却没扣预算/没明细"。
2. BEGIN IMMEDIATE 在事务开始时即取得写锁，使"查缓存 -> 扣预算 -> 写结果 ->
   写发布明细"整个序列在多个服务进程之间串行化，两个进程同时查询也不会超预算、
   不会重复扣费或重复添加明细。
3. 同一社区的再次查询只读持久化的缓存结果：不重复扣额、不换噪声、不添明细；
   响应丢失后的重试因此天然幂等。
4. 导入是一次性的：meta.imported 置位后任何后续上传都会被整体拒绝，
   原数据不会被覆盖；household_id 主键提供第二道防线。
5. 旧库迁移只做"基线快照"：已有 results 没有 releases 明细时，保留账本已花费，
   快照其社区/时间并标记为无法逐笔核验，绝不补造扣费明细。
"""

import csv
import io
import math
import os
import random
import sqlite3
import time
from datetime import datetime, timezone

# 隐私参数（可用环境变量覆盖，便于压测；生产部署使用默认值即可）
EPSILON_TOTAL = float(os.environ.get("DP_EPSILON_TOTAL", "1.0"))      # 全期隐私预算
EPSILON_PER_QUERY = float(os.environ.get("DP_EPSILON_PER_QUERY", "0.25"))  # 每个社区首次查询扣减
NOISE_SCALE = float(os.environ.get("DP_NOISE_SCALE", "4.0"))          # 拉普拉斯噪声尺度 b

_rng = random.SystemRandom()


# 发布明细的核验状态
RELEASE_VERIFIED = "verified"          # 正常发布，与预算账本逐笔对账
RELEASE_LEGACY = "legacy_unverifiable"  # 迁移前的旧结果：时间来自迁移时快照，不可逐笔核验


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
-- 发布明细（逐笔），首键社区即"每个社区至多一条"的物理约束：
-- 并发/重试都无法为同一社区插入第二笔，因此不会漏记也不会重复计费。
-- epsilon 可空：旧库迁移出的结果无法核验扣费，此列必须留空而不是补一个数。
CREATE TABLE IF NOT EXISTS releases (
    community TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    rank TEXT NOT NULL,
    released_at TEXT NOT NULL,
    epsilon REAL,
    noisy_count REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'verified',
    note TEXT
);
CREATE INDEX IF NOT EXISTS idx_releases_seq ON releases (seq, community);
"""


def connect(db_path):
    """每次请求新建连接：多进程下最安全。WAL + 长 busy_timeout 保证锁等待而非报错。"""
    conn = sqlite3.connect(
        db_path, timeout=30, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def _seed_in_transaction(conn):
    """在已持有 BEGIN IMMEDIATE 写锁的事务内建表/播种。"""
    for stmt in SCHEMA.split(";"):
        if stmt.strip():
            conn.execute(stmt)
    _ensure_columns(conn)
    conn.execute("INSERT OR IGNORE INTO meta (id, imported) VALUES (1, 0)")
    conn.execute(
        "INSERT OR IGNORE INTO budget (id, total, spent) VALUES (1, ?, 0.0)",
        (EPSILON_TOTAL,),
    )


def _ensure_columns(conn):
    """轻量向前兼容：老库缺的列补上（不重建表、不动数据）。"""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(meta)")}
    if "releases_baselined" not in cols:
        conn.execute(
            "ALTER TABLE meta ADD COLUMN releases_baselined INTEGER NOT NULL DEFAULT 0"
        )
    if "legacy_spent" not in cols:
        conn.execute("ALTER TABLE meta ADD COLUMN legacy_spent REAL")


def init_db(db_path):
    """建表并播种单行 meta/budget。多进程同时启动时重试等待锁。

    建表与播种在同一事务内完成，保证两个实例同时启动时不会出现
    "表已建好、种子行尚未写入"的中间态。
    """
    directory = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(directory, exist_ok=True)
    last = None
    for _ in range(30):
        try:
            conn = connect(db_path)
            try:
                conn.execute("BEGIN IMMEDIATE")
                _seed_in_transaction(conn)
                conn.execute("COMMIT")
            except Exception:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
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


def migrate_release_ledger(db_path):
    """旧库升级：为"已有结果但无发布明细"的社区补一条基线快照。

    只记录迁移这一刻能看到的事实（社区、结果创建时间、当时账本累计花费），
    状态标记为 legacy_unverifiable，单次扣额留空（NULL）；
    **不推算、不补造任何扣费明细**，也不改动 budget.spent。
    """
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        done = conn.execute(
            "SELECT releases_baselined FROM meta WHERE id = 1"
        ).fetchone()["releases_baselined"]
        if done:
            conn.execute("COMMIT")
            return 0
        legacy = conn.execute(
            "SELECT r.community, r.created_at, r.noisy_count"
            " FROM results r LEFT JOIN releases p ON p.community = r.community"
            " WHERE p.community IS NULL ORDER BY r.created_at, r.community"
        ).fetchall()
        spent = conn.execute("SELECT spent FROM budget WHERE id = 1").fetchone()["spent"]
        # 已有可核验明细的扣额不应再算进旧结果基线（防御性，正常启动时 releases 为空）
        verified_spent = conn.execute(
            "SELECT COALESCE(SUM(epsilon), 0.0) AS s FROM releases"
            " WHERE status = ?", (RELEASE_VERIFIED,)
        ).fetchone()["s"]
        legacy_spent = max(0.0, spent - verified_spent)
        max_seq = conn.execute("SELECT COALESCE(MAX(seq), 0) AS s FROM releases").fetchone()["s"]
        offset = 0
        note = "旧结果无逐笔发布记录，无法核验其扣费；已花费预算按账本保留"
        for row in legacy:
            offset += 1
            conn.execute(
                "INSERT INTO releases (community, seq, rank, released_at, epsilon,"
                " noisy_count, status, note) VALUES (?, ?, ?, ?, NULL, ?, ?, ?)",
                (
                    row["community"],
                    max_seq + offset,
                    "LEGACY-%03d" % offset,
                    row["created_at"],
                    row["noisy_count"],
                    RELEASE_LEGACY,
                    note,
                ),
            )
        conn.execute("UPDATE meta SET releases_baselined = 1, legacy_spent = ? WHERE id = 1",
                     (legacy_spent,))
        conn.execute("COMMIT")
        return len(legacy)
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------- 查询

def query_community(db_path, community):
    """查询某社区参与户数（含拉普拉斯噪声）。

    首次查询：在同一事务内"扣预算 + 写结果 + 写发布明细"，原子生效；
    再次查询：返回持久化的同一结果，不重复扣额、不换噪声、不添明细。
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
        released_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        # 预算扣减、结果与发布明细同事务写入：要么一起生效，要么一起回滚
        conn.execute(
            "INSERT INTO results (community, noisy_count, created_at) VALUES (?, ?, ?)",
            (community, noisy, released_at),
        )
        seq = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM releases"
        ).fetchone()["s"]
        conn.execute(
            "INSERT INTO releases (community, seq, rank, released_at, epsilon,"
            " noisy_count, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                community,
                seq,
                "REL-%03d" % seq,
                released_at,
                EPSILON_PER_QUERY,
                noisy,
                RELEASE_VERIFIED,
            ),
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


# ---------------------------------------------------------------- 发布对账

_TOL = 1e-9


def get_reconciliation(db_path):
    """发布对账：逐笔列出新发布的详情，并与预算账本核验。

    只读事务：快照整个账本后计算，保证呈现的是某一个完整提交的状态，
    不会把并发提交拆成"一半明细、一半账本"。

    隐私边界：只输出发布社区、时间、单次扣额、累计花费与告警代码；
    绝不返回 households 中的任何记录，也不返回精确参与户数。
    """
    conn = connect(db_path)
    try:
        conn.execute("BEGIN")  # 读快照（WAL 下看到的是一个一致的时间点）
        budget = conn.execute("SELECT total, spent FROM budget WHERE id = 1").fetchone()
        baseline = conn.execute(
            "SELECT releases_baselined, legacy_spent FROM meta WHERE id = 1"
        ).fetchone()
        releases = conn.execute(
            "SELECT community, seq, rank, released_at, epsilon, noisy_count, status, note"
            " FROM releases ORDER BY seq, community"
        ).fetchall()
        result_rows = {
            row["community"]: row["noisy_count"]
            for row in conn.execute("SELECT community, noisy_count FROM results")
        }
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    entries, flags = [], []
    charged_total = 0.0
    releases_by_community = {r["community"]: r for r in releases}

    for r in releases:
        legacy = r["status"] == RELEASE_LEGACY
        if not legacy:
            charged_total += r["epsilon"] or 0.0
        entries.append({
            "community": r["community"],
            "seq": r["seq"],
            "rank": r["rank"],
            "released_at": r["released_at"],
            "epsilon": None if legacy else r["epsilon"],
            # 旧结果的扣额不可知，累计花费标为 null，绝不显示为 0（那会等于伪造未扣费）
            "cumulative_spent": None if legacy else round(charged_total, 4),
            "status": r["status"],
            "verifiable": not legacy,
            "note": r["note"],
        })
        if legacy:
            flags.append({
                "code": "legacy_unverifiable",
                "community": r["community"],
                "message": (
                    f"{r['community']}：旧结果没有逐笔发布记录，时间仅为迁移快照，"
                    "无法核验其单次扣额；其已花费预算保留在账本中"
                ),
            })

    # 旧结果在迁移时已计入账本的花费：单独列示，不参与逐笔核验。
    # 若账本没有记录这个基线金额（更早版本迁移过），则金额未知，不能当作 0。
    legacy_entries = [e for e in entries if not e["verifiable"]]
    if legacy_entries:
        legacy_spent = (None if baseline["legacy_spent"] is None
                        else float(baseline["legacy_spent"]))
    else:
        legacy_spent = 0.0

    # 明细与账本不一致：可核验明细合计 + 旧结果已花费 应等于账本花费
    spent = budget["spent"]
    if legacy_spent is None:
        expected_spent = None
        difference = None
        consistent = True  # 旧结果花费未知，无法比较；此情形由 legacy 告警说明
        compare_note = "旧结果已花费基础金额未知，无法与账本比较"
    else:
        expected_spent = charged_total + legacy_spent
        difference = round(spent - expected_spent, 6)
        consistent = abs(difference) <= _TOL
        compare_note = (
            f"可核验明细合计 ε={charged_total:.4f}"
            + (f" + 旧结果已花费 ε={legacy_spent:.4f}" if legacy_entries else "")
            + f"，预算账本已花 ε={spent:.4f}，"
            + ("一致" if consistent else f"不一致（差额 {difference:+.4f}）")
        )
    flags.append({
        "code": "ledger_consistent" if consistent else "ledger_mismatch",
        "community": None,
        "message": compare_note,
    })
    orphan_results = sorted(set(result_rows) - set(releases_by_community))
    if orphan_results:
        flags.append({
            "code": "result_without_release",
            "community": None,
            "message": (
                f"{len(orphan_results)} 个社区有结果缓存但没有发布明细："
                + "、".join(orphan_results[:5])
                + ("…" if len(orphan_results) > 5 else "")
            ),
        })
    # 结果与明细双向核对（迁移时已对齐，此处防止人为篡改/异常写入）
    for community, rel in releases_by_community.items():
        if community not in result_rows:
            flags.append({
                "code": "release_without_result",
                "community": community,
                "message": f"{community}：存在发布明细但没有对应的结果缓存",
            })
        elif abs(result_rows[community] - rel["noisy_count"]) > 1e-9:
            flags.append({
                "code": "release_result_mismatch",
                "community": community,
                "message": f"{community}：发布明细记录的噪声结果与结果缓存不一致",
            })

    legacy_entries = [e for e in entries if not e["verifiable"]]
    return {
        "budget_total": budget["total"],
        "budget_spent": budget["spent"],
        "budget_remaining": round(budget["total"] - budget["spent"], 4),
        "ledger_charged_total": round(charged_total, 4),
        "legacy_spent": (None if legacy_spent is None else round(legacy_spent, 4)),
        "expected_spent": (None if expected_spent is None else round(expected_spent, 4)),
        "ledger_difference": difference,
        "ledger_consistent": consistent,
        "release_count": len(entries),
        "verified_release_count": len(entries) - len(legacy_entries),
        "legacy_release_count": len(legacy_entries),
        "all_verifiable": not legacy_entries,
        "entries": entries,
        "flags": flags,
    }
