#!/usr/bin/env python3
"""本地验证脚本（仅标准库，不依赖 FastAPI / Docker）。

直接检验 app/dbcore.py 的关键不变量：
  1. CSV 校验：重复住户号整份拒收、非法取值拒收；
  2. 一次性导入：成功导入后再次上传被拒绝，原数据不被覆盖；
  3. 预算语义：首次查询扣 0.25，再次查询返回同一结果且不扣费，预算耗尽拒绝新社区；
  4. 多进程并发：4 个进程同时狂查，预算绝不超支、结果一致、扣费与结果一一对应；
  5. 崩溃一致性：进程被 kill -9 后，账本仍满足 spent == 0.25 × 已保存结果数；
  6. 发布明细：首次查询写入一条明细，重复查询不添明细，明细与账本逐笔一致；
  7. 旧库迁移：已有结果但无明细时保留已花费预算，只补"不可核验"基线，不补造扣费；
  8. 故障重试：写完即崩溃/异常注入后重试，明细不重复、不漏记，对账恒完整。

用法：python3 scripts/verify_local.py
"""

import json
import os
import random
import shutil
import signal
import sqlite3
import sys
import tempfile
import time
from multiprocessing import Process, Queue

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

import dbcore  # noqa: E402

PASS, FAIL = "\033[32mPASS\033[0m", "\033[31mFAIL\033[0m"
failures = []


def check(name, cond, detail=""):
    print(f"  [{PASS if cond else FAIL}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def fresh_db(tag):
    path = os.path.join(tempfile.gettempdir(), f"dp_verify_{tag}_{os.getpid()}")
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    db = os.path.join(path, "test.db")
    dbcore.init_db(db)
    return db


def ledger(db):
    conn = sqlite3.connect(db)
    spent = conn.execute("SELECT spent FROM budget WHERE id=1").fetchone()[0]
    results = dict(conn.execute("SELECT community, noisy_count FROM results").fetchall())
    imported = conn.execute("SELECT imported FROM meta WHERE id=1").fetchone()[0]
    hh = conn.execute("SELECT COUNT(*) FROM households").fetchone()[0]
    releases = conn.execute(
        "SELECT community, seq, epsilon, status FROM releases ORDER BY seq"
    ).fetchall()
    conn.close()
    return spent, results, imported, hh, releases


# ---------------------------------------------------------------- 1. CSV 校验
def test_csv():
    print("\n■ 测试 1：CSV 校验")
    good = "住户号,社区,是否参与\nA1,阳光社区,是\nA2,阳光社区,否\n".encode()
    recs = dbcore.parse_csv(good)
    check("合法 CSV 解析", recs == [("A1", "阳光社区", 1), ("A2", "阳光社区", 0)])

    dup = "住户号,社区,是否参与\nA1,阳光社区,是\nA1,河畔社区,否\n".encode()
    try:
        dbcore.parse_csv(dup)
        check("重复住户号整份拒收", False)
    except ValueError as e:
        check("重复住户号整份拒收", "重复住户号" in str(e), str(e))

    bad = "住户号,社区,是否参与\nA1,阳光社区,也许\n".encode()
    try:
        dbcore.parse_csv(bad)
        check("非法参与取值拒收", False)
    except ValueError:
        check("非法参与取值拒收", True)

    eng = b"household_id,community,participated\nA1,X,1\nA2,Y,0\n"
    check("英文表头兼容", len(dbcore.parse_csv(eng)) == 2)


# ---------------------------------------------------------------- 2. 一次性导入
def test_import():
    print("\n■ 测试 2：一次性导入、不得覆盖")
    db = fresh_db("import")
    n = dbcore.import_records(db, [("A1", "阳光社区", 1), ("A2", "河畔社区", 0)])
    check("首次导入成功", n == 2)
    try:
        dbcore.import_records(db, [("B1", "星辰社区", 1)])
        check("再次导入被拒绝", False)
    except dbcore.AlreadyImported:
        check("再次导入被拒绝", True)
    _, _, imported, hh, _ = ledger(db)
    check("原数据未被覆盖", imported == 1 and hh == 2, f"households={hh}")


# ---------------------------------------------------------------- 3. 预算语义
def test_budget():
    print("\n■ 测试 3：预算扣减与结果缓存（默认 ε=1 / 0.25）")
    db = fresh_db("budget")
    recs = [(f"H{i:03d}", comm, flag) for i, (comm, flag) in enumerate(
        [("阳光社区", 1)] * 5 + [("河畔社区", 1)] * 3 + [("青山社区", 0)] * 2)]
    dbcore.import_records(db, recs)

    r1 = dbcore.query_community(db, "阳光社区")
    check("首次查询扣 0.25", abs(r1["charged"] - 0.25) < 1e-9 and not r1["cached"])
    r2 = dbcore.query_community(db, "阳光社区")
    check("再次查询返回同一结果", r2["cached"] and r2["noisy_count"] == r1["noisy_count"])
    check("再次查询不扣费", r2["charged"] == 0.0)

    dbcore.query_community(db, "河畔社区")
    dbcore.query_community(db, "青山社区")
    dbcore.query_community(db, "梧桐社区")  # 不存在的社区也扣预算并缓存（防止探测差异）
    spent, results, _, _, _ = ledger(db)
    check("4 个社区后预算恰好用尽", abs(spent - 1.0) < 1e-9, f"spent={spent}")
    try:
        dbcore.query_community(db, "星辰社区")
        check("预算耗尽拒绝新社区", False)
    except dbcore.BudgetExhausted:
        check("预算耗尽拒绝新社区", True)
    r = dbcore.query_community(db, "阳光社区")
    check("预算耗尽后缓存社区仍可查", r["cached"] and r["noisy_count"] == r1["noisy_count"])
    check("结果条数与扣费一一对应", len(results) == 4 and abs(spent - 0.25 * len(results)) < 1e-9)


# ---------------------------------------------------------------- 4. 多进程并发
N_COMM = 200
N_PROCS = 4
QUERIES_PER_PROC = 400


def _worker(db, seed, q):
    rng = random.Random(seed)
    out = {}
    charged = 0.0
    for _ in range(QUERIES_PER_PROC):
        comm = f"社区{rng.randrange(N_COMM):03d}"
        try:
            r = dbcore.query_community(db, comm)
            out.setdefault(comm, set()).add(round(r["noisy_count"], 12))
            charged += r["charged"]
        except dbcore.BudgetExhausted:
            pass
    q.put((out, charged))


def test_concurrency():
    print(f"\n■ 测试 4：{N_PROCS} 进程 × {QUERIES_PER_PROC} 次并发查询（测试预算 ε=40）")
    dbcore.EPSILON_TOTAL = 40.0  # 放大预算以充分压测；允许 160 次扣费
    db = fresh_db("conc")
    recs = [(f"H{i:05d}", f"社区{i % N_COMM:03d}", i % 2) for i in range(4000)]
    dbcore.import_records(db, recs)

    q = Queue()
    procs = [Process(target=_worker, args=(db, 1000 + i, q)) for i in range(N_PROCS)]
    for p in procs:
        p.start()
    seen, total_charged_observed = {}, 0.0
    for _ in procs:
        out, charged = q.get()
        total_charged_observed += charged
        for comm, vals in out.items():
            seen.setdefault(comm, set()).update(vals)
    for p in procs:
        p.join()

    spent, results, _, _, _ = ledger(db)
    check("预算绝不超支", spent <= 40.0 + 1e-9, f"spent={spent}")
    check("扣费次数 == 结果条数（无已扣无结果/有结果未扣）",
          abs(spent - 0.25 * len(results)) < 1e-9,
          f"spent={spent}, results={len(results)}")
    check("各进程观察到的扣费总额 == 账本扣费",
          abs(total_charged_observed - spent) < 1e-9,
          f"observed={total_charged_observed}, spent={spent}")
    inconsistent = [c for c, v in seen.items() if len(v) > 1]
    check("同一社区多次查询结果完全一致（不换噪声）", not inconsistent,
          f"不一致社区: {inconsistent[:5]}")
    mismatch = [c for c, v in seen.items() if round(results.get(c, float("nan")), 12) not in v]
    check("进程观察值 == 持久化缓存值", not mismatch, f"不匹配: {mismatch[:5]}")
    print(f"    · 实际扣费社区 {len(results)} 个，spent={spent}")


# ---------------------------------------------------------------- 5. 并发导入竞态
def _importer(db, tag, q):
    try:
        n = dbcore.import_records(db, [(f"{tag}{i}", "阳光社区", 1) for i in range(10)])
        q.put(("ok", n))
    except dbcore.AlreadyImported:
        q.put(("rejected", 0))


def test_import_race():
    print("\n■ 测试 5：两个进程同时导入，只有一个成功")
    db = fresh_db("imgrace")
    q = Queue()
    procs = [Process(target=_importer, args=(db, f"P{k}-", q)) for k in range(2)]
    for p in procs:
        p.start()
    outcomes = [q.get()[0] for _ in procs]
    for p in procs:
        p.join()
    _, _, imported, hh, _ = ledger(db)
    check("恰好一个导入成功", sorted(outcomes) == ["ok", "rejected"], str(outcomes))
    check("库中恰好 10 行（无部分写入、无覆盖）", imported == 1 and hh == 10, f"hh={hh}")


# ---------------------------------------------------------------- 6. 崩溃一致性
def _hammer(db):
    i = 0
    while True:
        try:
            dbcore.query_community(db, f"崩溃社区{i:04d}")
        except dbcore.BudgetExhausted:
            pass
        i += 1
        time.sleep(0.02)  # 放慢节奏，确保 kill 发生在扣费途中


def test_crash():
    print("\n■ 测试 6：进程被 kill -9 后的账本一致性")
    dbcore.EPSILON_TOTAL = 40.0
    db = fresh_db("crash")
    dbcore.import_records(db, [(f"H{i:05d}", f"崩溃社区{i:04d}", 1) for i in range(5000)])
    p = Process(target=_hammer, args=(db,))
    p.start()
    time.sleep(1.0)
    os.kill(p.pid, signal.SIGKILL)
    p.join()
    spent, results, _, _, _ = ledger(db)  # 新连接触发 WAL 恢复
    check("崩溃后 spent == 0.25 × 结果数（同事务原子生效）",
          abs(spent - 0.25 * len(results)) < 1e-9,
          f"spent={spent}, results={len(results)}")
    r = dbcore.query_community(db, next(iter(results)))
    check("崩溃后已缓存结果仍可读且一致", r["cached"])
    print(f"    · 崩溃时已完成 {len(results)} 次扣费，spent={spent}")


# ---------------------------------------------------------------- 7. 发布明细
def test_release_ledger():
    print("\n■ 测试 7：首次查询写一条发布明细，重复查询不添明细")
    dbcore.EPSILON_TOTAL = 1.0
    db = fresh_db("releases")
    dbcore.import_records(db, [(f"H{i:03d}", f"社区{i % 5}", 1) for i in range(50)])

    r1 = dbcore.query_community(db, "社区0")
    spent, results, _, _, releases = ledger(db)
    check("首次查询产生 1 条明细", len(releases) == 1, f"releases={releases}")
    check("明细扣额与账本一致", abs(releases[0][2] - r1["charged"]) < 1e-9)
    check("明细状态为可核验", releases[0][3] == dbcore.RELEASE_VERIFIED)

    for _ in range(5):  # 重复查询（模拟响应丢失后的重试）
        r2 = dbcore.query_community(db, "社区0")
    spent, results, _, _, releases = ledger(db)
    check("5 次重复查询不添明细", len(releases) == 1, f"releases={len(releases)}")
    check("5 次重复查询不多扣费", abs(spent - 0.25) < 1e-9, f"spent={spent}")
    check("重复查询返回同一结果", r2["noisy_count"] == r1["noisy_count"] and r2["cached"])

    for c in ["社区1", "社区2", "社区3"]:
        dbcore.query_community(db, c)
    try:
        dbcore.query_community(db, "社区4")
        check("预算耗尽时不写明细", False)
    except dbcore.BudgetExhausted:
        check("预算耗尽时不写明细", True)
    spent, results, _, _, releases = ledger(db)
    check("预算耗尽后仍为 4 条明细", len(releases) == 4, f"releases={len(releases)}")
    check("seq 连续且唯一", [r[1] for r in releases] == [1, 2, 3, 4], str(releases))

    report = dbcore.get_reconciliation(db)
    check("对账：明细 4 笔、账本一致",
          report["release_count"] == 4 and report["ledger_consistent"]
          and abs(report["ledger_charged_total"] - 1.0) < 1e-9, str(report["ledger_difference"]))
    check("对账：累计花费逐笔递增且终值等于账本",
          [e["cumulative_spent"] for e in report["entries"]] == [0.25, 0.5, 0.75, 1.0])
    check("对账：全部可核验", report["all_verifiable"] and report["legacy_release_count"] == 0)
    keys = set(report["entries"][0])
    check("对账不含原始记录/精确人数字段",
          not ({"households", "household_id", "participated", "exact_count"} & keys), str(keys))
    json.dumps(report, ensure_ascii=False)  # 可被 JSON 序列化（浏览器直接消费）


# ---------------------------------------------------------------- 8. 旧库迁移
def test_legacy_migration():
    print("\n■ 测试 8：旧库已有结果但无逐笔明细（保留已花预算，不补造扣费）")
    db = fresh_db("legacy")
    dbcore.import_records(db, [(f"H{i:03d}", f"旧社区{i % 3}", 1) for i in range(30)])
    # 模拟旧版本：只有 results + budget，没有任何 releases 明细，且已花费 0.5
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO results (community, noisy_count) VALUES ('旧社区0', 7.5)")
    conn.execute("INSERT INTO results (community, noisy_count) VALUES ('旧社区1', 3.25)")
    conn.execute("UPDATE budget SET spent = 0.5 WHERE id = 1")
    conn.execute("DELETE FROM releases")  # 确保 meta 也回到未迁移状态
    conn.execute("UPDATE meta SET releases_baselined = 0, legacy_spent = NULL WHERE id = 1")
    conn.commit()
    conn.close()

    n = dbcore.migrate_release_ledger(db)
    check("迁移补了 2 条基线快照", n == 2, f"n={n}")
    report = dbcore.get_reconciliation(db)
    check("迁移后已花费预算保持不变", abs(report["budget_spent"] - 0.5) < 1e-9,
          f"spent={report['budget_spent']}")
    check("旧结果标记为不可逐笔核验",
          report["legacy_release_count"] == 2 and not report["all_verifiable"])
    check("旧结果单次扣额为 null（不伪造）",
          all(e["epsilon"] is None and not e["verifiable"] for e in report["entries"]))
    check("旧结果累计花费为 null（不显示为 0）",
          all(e["cumulative_spent"] is None for e in report["entries"]))
    check("旧结果有快照时间且被标为 legacy",
          all(e["released_at"] and e["status"] == dbcore.RELEASE_LEGACY for e in report["entries"]))
    check("旧结果不让账本判为不一致", report["ledger_consistent"],
          str(report["ledger_difference"]))
    check("旧结果已花费在账本中保留并单列",
          abs(report["legacy_spent"] - 0.5) < 1e-9 and report["budget_spent"] == 0.5,
          f"legacy_spent={report['legacy_spent']}, spent={report['budget_spent']}")
    check("旧结果产生明确告警",
          any(f["code"] == "legacy_unverifiable" for f in report["flags"]))
    check("重复迁移幂等（不重复补）", dbcore.migrate_release_ledger(db) == 0)

    # 更早版本迁移过旧库（有 legacy 明细但没记录基线金额）：不应把金额当成 0 判定不一致
    conn = sqlite3.connect(db)
    conn.execute("UPDATE meta SET legacy_spent = NULL WHERE id = 1")
    conn.commit()
    conn.close()
    report = dbcore.get_reconciliation(db)
    check("基线金额未知时不误报不一致",
          report["legacy_spent"] is None and report["ledger_consistent"]
          and report["ledger_difference"] is None,
          f"legacy_spent={report['legacy_spent']}, diff={report['ledger_difference']}")
    conn = sqlite3.connect(db)
    conn.execute("UPDATE meta SET legacy_spent = 0.5 WHERE id = 1")
    conn.commit()
    conn.close()

    # 迁移后新查询：正常扣费并写可核验明细
    dbcore.EPSILON_TOTAL = 1.0
    dbcore.query_community(db, "新社区")
    report = dbcore.get_reconciliation(db)
    check("新发布为可核验明细", report["verified_release_count"] == 1)
    check("可核验合计 == 新增扣费",
          abs(report["ledger_charged_total"] - 0.25) < 1e-9,
          str(report["ledger_charged_total"]))
    check("账本仍一致（旧结果 0.5 + 新增 0.25）",
          report["ledger_consistent"]
          and abs(report["budget_spent"] - 0.75) < 1e-9,
          str(report["budget_spent"]))


# ---------------------------------------------------------------- 9. 故障重试
def _retry_after_crash(db_path, community, tag):
    """模拟"扣费+结果+明细已提交，但响应在返回途中丢失"的重试。"""
    return dbcore.query_community(db_path, community)


def test_failure_retry():
    print("\n■ 测试 9：写入失败/响应丢失后的重试（不漏记、不重复扣费）")
    db = fresh_db("retry")
    dbcore.import_records(db, [(f"H{i:03d}", "重试社区", 1) for i in range(20)])

    # ① 事务中途异常：预算已 UPDATE 但结果/明细写入前抛错 -> 整体回滚
    original = dbcore.laplace_noise
    calls = {"n": 0}

    def boom(*a, **kw):
        calls["n"] += 1
        raise RuntimeError("模拟写库过程中进程故障")

    dbcore.laplace_noise = boom
    try:
        dbcore.query_community(db, "重试社区")
        check("异常时查询抛错", False)
    except RuntimeError:
        check("异常时查询抛错", True)
    finally:
        dbcore.laplace_noise = original
    spent, results, _, _, releases = ledger(db)
    check("失败查询完全回滚（未扣费/无结果/无明细）",
          abs(spent) < 1e-9 and not results and not releases,
          f"spent={spent}, results={len(results)}, releases={len(releases)}")

    # ② 重试成功：恰好一次扣费与一条明细
    r = _retry_after_crash(db, "重试社区", "t1")
    spent, results, _, _, releases = ledger(db)
    check("重试后恰好扣费 0.25", abs(spent - 0.25) < 1e-9, f"spent={spent}")
    check("重试后恰好 1 条明细", len(releases) == 1, f"releases={len(releases)}")

    # ③ 已提交但响应丢失，再重试 N 次 == 缓存命中
    for _ in range(10):
        _retry_after_crash(db, "重试社区", "t2")
    spent2, results2, _, _, releases2 = ledger(db)
    check("响应丢失后重试不重复扣费", abs(spent2 - 0.25) < 1e-9, f"spent={spent2}")
    check("响应丢失后重试不添明细", len(releases2) == 1, f"releases={len(releases2)}")
    check("噪声结果不变", results2["重试社区"] == results["重试社区"])

    report = dbcore.get_reconciliation(db)
    check("对账呈现完整的单次提交状态",
          report["release_count"] == 1 and report["ledger_consistent"]
          and abs(report["ledger_charged_total"] - 0.25) < 1e-9)


# ---------------------------------------------------------------- 10. 并发对账一致性
def _recon_worker(db, seed, q):
    rng = random.Random(seed)
    snapshots = []
    for _ in range(300):
        if rng.random() < 0.5:
            try:
                dbcore.query_community(db, f"对账社区{rng.randrange(60):03d}")
            except dbcore.BudgetExhausted:
                pass
        else:
            snapshots.append(dbcore.get_reconciliation(db))
    q.put(snapshots)


def test_concurrent_reconciliation():
    print("\n■ 测试 10：并发查询时对账只呈现完整提交状态")
    dbcore.EPSILON_TOTAL = 60.0
    db = fresh_db("reconconc")
    dbcore.import_records(db, [(f"H{i:04d}", f"对账社区{i % 60:03d}", 1) for i in range(6000)])

    q = Queue()
    procs = [Process(target=_recon_worker, args=(db, 7 + i, q)) for i in range(3)]
    for p in procs:
        p.start()
    snaps = []
    for _ in procs:
        snaps.extend(q.get())
    for p in procs:
        p.join()

    bad = [s for s in snaps
           if not s["ledger_consistent"]
           or s["release_count"] != s["verified_release_count"]
           or s["release_count"] != len({e["community"] for e in s["entries"]})]
    check("并发中每份对账都自洽（明细合计 == 账本、无重复社区、无旧结果）",
          not bad, f"不自洽快照 {len(bad)}/{len(snaps)}")
    spent, results, _, _, releases = ledger(db)
    check("最终 spend == 0.25 × 明细数 == 0.25 × 结果数",
          abs(spent - 0.25 * len(releases)) < 1e-9 and len(releases) == len(results),
          f"spent={spent}, releases={len(releases)}, results={len(results)}")


if __name__ == "__main__":
    test_csv()
    test_import()
    test_budget()
    test_concurrency()
    test_import_race()
    test_crash()
    test_release_ledger()
    test_legacy_migration()
    test_failure_retry()
    test_concurrent_reconciliation()
    print("\n" + ("=" * 50))
    if failures:
        print(f"共 {len(failures)} 项失败: {failures}")
        sys.exit(1)
    print("全部验证通过 ✅")
