#!/usr/bin/env python3
"""本地验证脚本（仅标准库，不依赖 FastAPI / Docker）。

直接检验 app/dbcore.py 的关键不变量：
  1. CSV 校验：重复住户号整份拒收、非法取值拒收；
  2. 一次性导入：成功导入后再次上传被拒绝，原数据不被覆盖；
  3. 预算语义：首次查询扣 0.25，再次查询返回同一结果且不扣费，预算耗尽拒绝新社区；
  4. 多进程并发：4 个进程同时狂查，预算绝不超支、结果一致、扣费与结果一一对应；
  5. 崩溃一致性：进程被 kill -9 后，账本仍满足 spent == 0.25 × 已保存结果数。

用法：python3 scripts/verify_local.py
"""

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
    conn.close()
    return spent, results, imported, hh


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
    _, _, imported, hh = ledger(db)
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
    spent, results, _, _ = ledger(db)
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

    spent, results, _, _ = ledger(db)
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
    _, _, imported, hh = ledger(db)
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
    spent, results, _, _ = ledger(db)  # 新连接触发 WAL 恢复
    check("崩溃后 spent == 0.25 × 结果数（同事务原子生效）",
          abs(spent - 0.25 * len(results)) < 1e-9,
          f"spent={spent}, results={len(results)}")
    r = dbcore.query_community(db, next(iter(results)))
    check("崩溃后已缓存结果仍可读且一致", r["cached"])
    print(f"    · 崩溃时已完成 {len(results)} 次扣费，spent={spent}")


if __name__ == "__main__":
    test_csv()
    test_import()
    test_budget()
    test_concurrency()
    test_import_race()
    test_crash()
    print("\n" + ("=" * 50))
    if failures:
        print(f"共 {len(failures)} 项失败: {failures}")
        sys.exit(1)
    print("全部验证通过 ✅")
