#!/usr/bin/env python3
"""部署冒烟测试（仅标准库）：对 docker compose 启动的两个实例做端到端验证。

前提：docker compose up --build 已启动（app1:8001 / app2:8002，共享同一数据库）。
用法：python3 scripts/smoke_http.py [样例CSV路径]
"""

import concurrent.futures as cf
import json
import sys
import urllib.error
import urllib.request
import uuid

BASES = ["http://localhost:8001", "http://localhost:8002"]
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


def main():
    print("■ 上传样例 CSV 到 app1:8001")
    st, body = upload(BASES[0], CSV_PATH)
    if st == 409:
        print("  · 已有数据（409），沿用现有数据继续测试查询语义")
    else:
        check("首次上传成功", st == 200 and body.get("ok"), f"{st} {body}")

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
    check("总扣费恰好 0.25（只扣一次）", abs(charged_total - 0.25) < 1e-9,
          f"charged_total={charged_total}")

    print("■ 轮流查询其余社区，验证预算上限 ε=1")
    for i, comm in enumerate(COMMUNITIES[1:], start=1):
        st, body = post_json(BASES[i % 2], "/api/query", {"community": comm})
        if i <= 3:
            check(f"{comm} 首次查询成功（第 {i+1} 次扣费）", st == 200, f"{st} {body}")
        else:
            check(f"{comm} 被拒绝（预算已用尽，HTTP 429）", st == 429, f"{st} {body}")

    print("■ 预算耗尽后，已缓存社区仍可免费查询")
    st, body = post_json(BASES[1], "/api/query", {"community": COMMUNITIES[0]})
    check("缓存查询返回 200 且不扣费",
          st == 200 and body.get("cached") and body.get("charged") == 0.0, f"{st} {body}")

    with urllib.request.urlopen(BASES[0] + "/api/status", timeout=15) as r:
        status = json.loads(r.read())
    check("账本：已用 ε=1.00 / 缓存 4 个社区",
          abs(status["budget_spent"] - 1.0) < 1e-9 and status["cached_communities"] == 4,
          str(status))

    print("\n" + ("全部冒烟测试通过 ✅" if not failures else f"失败: {failures}"))
    sys.exit(0 if not failures else 1)


if __name__ == "__main__":
    main()
