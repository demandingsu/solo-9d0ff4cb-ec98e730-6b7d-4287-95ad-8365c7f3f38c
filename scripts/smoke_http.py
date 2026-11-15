#!/usr/bin/env python3
"""部署冒烟测试（仅标准库）：对 docker compose 启动的两个实例做端到端验证。

前提：docker compose up --build 已启动（app1:8001 / app2:8002，共享同一数据库）。
用法：python3 scripts/smoke_http.py [样例CSV路径]
可用环境变量 DP_BASES="http://host:port,http://host:port" 覆盖两个实例地址。
"""

import concurrent.futures as cf
import json
import os
import sys
import urllib.error
import urllib.request
import uuid

BASES = [b.strip() for b in os.environ.get(
    "DP_BASES", "http://localhost:8001,http://localhost:8002"
).split(",") if b.strip()]
CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "sample_data/survey.csv"
COMMUNITIES = ["阳光社区", "河畔社区", "青山社区", "梧桐社区", "星辰社区", "湖滨社区"]

failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def post_json(base, path, payload):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}


def upload(base, path):
    boundary = uuid.uuid4().hex
    with open(path, "rb") as f:
        data = f.read()
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="survey.csv"\r\n'
        f"Content-Type: text/csv\r\n\r\n"
    ).encode() + data + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        base + "/api/upload",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}


def get_json(base, path):
    with urllib.request.urlopen(base + path, timeout=15) as r:
        return r.status, json.loads(r.read())


def main():
    fresh_deploy = False
    print("■ 上传样例 CSV 到 app1:8001")
    st, body = upload(BASES[0], CSV_PATH)
    if st == 409:
        print("  · 已有数据（409），沿用现有数据继续测试查询语义")
    else:
        check("首次上传成功", st == 200 and body.get("ok"), f"{st} {body}")
        fresh_deploy = st == 200

    print("■ 再次上传必须被拒绝（不得覆盖）")
    st, body = upload(BASES[1], CSV_PATH)
    check("app2 重复上传返回 409", st == 409, f"{st} {body}")

    print("■ 同一社区在两个实例上并发各查 10 次：结果一致且只扣一次费")
    def q(base):
        return post_json(base, "/api/query", {"community": COMMUNITIES[0]})
    with cf.ThreadPoolExecutor(20) as ex:
        results = list(ex.map(q, [BASES[i % 2] for i in range(20)]))
    vals = {r[1].get("noisy_count") for r in results if r[0] == 200}
    charged_total = sum(r[1].get("charged", 0) for r in results if r[0] == 200)
    check("全部返回 200", all(r[0] == 200 for r in results))
    check("噪声结果完全一致", len(vals) == 1, str(vals))
    # 全新部署：并发组里恰好一次扣 0.25；对已有数据的重复运行：全部命中缓存、扣 0
    expected_charge = 0.25 if fresh_deploy else 0.0
    check(f"本次并发组总扣费 = {expected_charge}（只扣一次）",
          abs(charged_total - expected_charge) < 1e-9,
          f"charged_total={charged_total}, fresh_deploy={fresh_deploy}")

    print("■ 轮流查询其余社区，验证预算上限 ε=1")
    for i, comm in enumerate(COMMUNITIES[1:], start=1):
        st, body = post_json(BASES[i % 2], "/api/query", {"community": comm})
        if body.get("cached") or body.get("charged") == 0.0:
            check(f"{comm} 已有缓存，免费返回", st == 200, f"{st} {body}")
        elif i <= 3:
            check(f"{comm} 首次查询成功（第 {i+1} 次扣费）", st == 200, f"{st} {body}")
        else:
            check(f"{comm} 被拒绝（预算已用尽，HTTP 429）", st == 429, f"{st} {body}")
    _, mid_status = get_json(BASES[0], "/api/status")
    check("账本绝不超支（spent ≤ total）",
          mid_status["budget_spent"] <= mid_status["budget_total"] + 1e-9,
          str(mid_status))

    print("■ 预算耗尽后，已缓存社区仍可免费查询")
    st, body = post_json(BASES[1], "/api/query", {"community": COMMUNITIES[0]})
    check("缓存查询返回 200 且不扣费",
          st == 200 and body.get("cached") and body.get("charged") == 0.0, f"{st} {body}")

    with urllib.request.urlopen(BASES[0] + "/api/status", timeout=15) as r:
        status = json.loads(r.read())
    check("账本：已用 ε=1.00 / 缓存 ≥4 个社区",
          abs(status["budget_spent"] - 1.0) < 1e-9 and status["cached_communities"] >= 4,
          str(status))

    print("■ 发布对账：逐笔明细与预算账本一致，重复查询不添明细")
    _, rc = get_json(BASES[0], "/api/reconciliation")
    check("明细 4 笔、全部可核验、无旧结果",
          rc["release_count"] == 4 and rc["verified_release_count"] == 4
          and rc["legacy_release_count"] == 0, str(rc.get("release_count")))
    check("明细合计 = 账本已花，无差额",
          rc["ledger_consistent"] and rc["expected_spent"] == rc["budget_spent"],
          f"detail={rc.get('ledger_charged_total')} ledger={rc.get('budget_spent')}")
    check("逐笔累计花费递增至账本已花",
          [e["cumulative_spent"] for e in rc["entries"]]
          == [0.25, 0.5, 0.75, rc["budget_spent"]],
          str([e["cumulative_spent"] for e in rc["entries"]]))
    check("对账明细与账本最终态一致",
          abs(rc["budget_spent"] - 1.0) < 1e-9 and rc["release_count"] == 4, str(rc["budget_spent"]))
    check("对账只含元信息，不含原始记录/精确人数",
          not any(k in rc for k in ("households", "exact_count", "records")),
          str(list(rc)))
    check("对账包含社区与发布时间",
          all(e.get("community") and e.get("released_at") for e in rc["entries"]))

    before = rc["release_count"]
    for base in BASES:  # 两个实例上重复查询已缓存社区
        post_json(base, "/api/query", {"community": COMMUNITIES[0]})
    _, rc2 = get_json(BASES[1], "/api/reconciliation")
    check("重复查询后明细数不变", rc2["release_count"] == before,
          f"{before} -> {rc2['release_count']}")
    check("两个实例看到同一份对账", rc2["entries"] == rc["entries"])

    st, html = None, ""
    try:
        with urllib.request.urlopen(BASES[0] + "/", timeout=15) as r:
            st, html = r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        st = e.code
    check("查询页可访问且链接到对账页",
          st == 200 and 'href="/reconciliation"' in html, f"status={st}")

    st, html = None, ""
    try:
        with urllib.request.urlopen(BASES[0] + "/reconciliation", timeout=15) as r:
            st, html = r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        st = e.code
    check("独立对账页可访问且含明细表与统计区",
          st == 200 and "发布对账" in html and 'id="rc-body"' in html
          and 'id="rc-verdict"' in html, f"status={st}")
    for asset in ("/static/style.css", "/static/recon.js"):
        code = None
        try:
            with urllib.request.urlopen(BASES[0] + asset, timeout=15) as r:
                code = r.status
        except urllib.error.HTTPError as e:
            code = e.code
        check(f"静态资源 {asset} 可访问", code == 200, f"status={code}")

    print("\n" + ("全部冒烟测试通过 ✅" if not failures else f"失败: {failures}"))
    sys.exit(0 if not failures else 1)


if __name__ == "__main__":
    main()
