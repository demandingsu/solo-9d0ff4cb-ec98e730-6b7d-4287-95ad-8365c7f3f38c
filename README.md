# 社区参与户数 · 差分隐私查询服务

社区调查数据员上传单期住户参与 CSV 后，规划人员可按社区查询**带拉普拉斯噪声的参与户数**。
服务以差分隐私方式防止"反复查询还原住户记录"：全期隐私预算 ε=1，每个社区首次查询扣
0.25，再次查询返回持久化的同一结果，不重复扣额、不换噪声。

## 快速启动

```bash
docker compose up --build
```

启动两个**独立服务进程**，共享同一命名卷上的 SQLite 账本：

| 实例 | 地址 |
|------|------|
| app1 | http://localhost:8001 |
| app2 | http://localhost:8002 |

## 浏览器操作流程

1. 打开 http://localhost:8001 （或 8002，界面相同、数据共享）。
2. **上传 CSV**：选择 `sample_data/survey.csv`（6 个社区、85 户），点击"上传导入"。
   - 表头需含 `住户号,社区,是否参与`（兼容 `household_id,community,participated`）；
   - 任一住户号重复 → **整份拒收**（HTTP 400，不写库）；
   - 成功导入后数据锁定，再次上传一律 409 拒绝，**不得覆盖原数据**。
3. **查询**：输入社区名（如 `阳光社区`）→ 显示含噪声户数（拉普拉斯，尺度 4）。
   - 首次查询某社区：扣 ε=0.25，结果持久化；
   - 再次查询（换实例也一样）：返回**同一缓存结果**，扣 0；
   - 样例数据有 6 个社区，查满 4 个后预算 ε=1 用尽，第 5 个新社区返回 429；
     已缓存社区仍可免费查询。
4. **发布对账**：打开 http://localhost:8001/reconciliation （查询页第 ③ 区有入口；
   接口 `GET /api/reconciliation`）逐笔列出每次新发布的社区、发布时间、单次扣额 ε
   与累计花费，并对照预算账本指出不一致之处：
   - 明细合计（可核验发布）与账本已花不一致 → 红色告警，给出差额；
   - 有结果缓存却无发布明细、有明细却无结果缓存、明细噪声值与结果缓存不符 → 分别告警；
   - 只展示发布元信息，**不含住户记录或精确人数**。
   - 旧库中"已有结果但没有逐笔记录"的社区：保留账本中已花费预算，列表里标注
     **旧结果 · 无法逐笔核验**，单次扣额与累计花费留空，绝不补造扣费明细。
5. 页面只展示噪声结果与预算状态，**不提供原始记录或精确计数**。

## 一键验证

```bash
# 部署后端到端冒烟（上传→并发双实例查询→预算耗尽→缓存免费→发布对账）
python3 scripts/smoke_http.py

# 不依赖 Docker/FastAPI 的核心事务验证（CSV 校验、一次性导入、预算语义、
# 4 进程并发不超支、kill -9 崩溃后账本一致、发布明细/旧库迁移/故障重试/并发对账）
python3 scripts/verify_local.py
```

## 一致性与隐私设计

| 需求 | 实现 |
|------|------|
| 重复住户号整份拒收 | 上传时全量校验，任一重复即拒绝；`household_id` 主键兜底 |
| 导入后不得覆盖 | `meta.imported` 标志 + 单事务导入；再次上传 409 |
| 噪声计数 | Laplace(0, 4)，敏感度 1 ⇒ 单次查询 ε=0.25；精确计数不出库 |
| 预算 ε=1、首次扣 0.25 | `budget` 单行账本，`UPDATE ... WHERE spent + 0.25 <= total` 原子扣减 |
| 再次查询同一结果 | `results` 表按社区持久化噪声结果，命中即返回、不扣费、不添明细 |
| 两进程同时查询不超预算 | 共享 SQLite（WAL）；`BEGIN IMMEDIATE` 使"查缓存→扣预算→写结果→写明细"跨进程串行 |
| 扣减与保存一同生效 | 预算 UPDATE、结果 INSERT、发布明细 INSERT 在**同一事务**提交，失败整体回滚 |
| 重试不多扣/不换噪声 | 响应丢失后重试命中缓存；写失败回滚后重试重新计费且此前无扣额 |
| 发布对账不漏记/不重复 | 事务内一次提交；`releases.community` 主键保证同一社区至多一条明细；对账在单个读快照中计算 |
| 旧结果不伪造扣费 | 迁移只记基线快照（社区 + 结果创建时间），`epsilon`/累计花费留空，状态 `legacy_unverifiable`；`budget.spent` 原样保留 |

## 目录结构

```
├── docker-compose.yml      # 两个服务进程 + 共享数据卷
├── app/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dbcore.py           # 核心：CSV 校验 + SQLite 事务（纯标准库）
│   ├── main.py             # FastAPI 接口层
│   └── static/
│       ├── index.html          # 查询页（含对账入口）
│       ├── reconciliation.html # 独立发布对账页
│       ├── recon.js            # 对账渲染（共用）
│       └── style.css           # 共用样式
├── sample_data/survey.csv  # 样例数据（6 社区 / 85 户）
└── scripts/
    ├── verify_local.py     # 核心不变量验证（含多进程并发、崩溃恢复）
    └── smoke_http.py       # 部署后端到端冒烟
```

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|------|------|------|
| `DB_PATH` | `/data/dp.db` | SQLite 路径（compose 中指向共享卷） |
| `DP_EPSILON_TOTAL` | `1.0` | 全期隐私预算 |
| `DP_EPSILON_PER_QUERY` | `0.25` | 每个社区首次查询扣减 |
| `DP_NOISE_SCALE` | `4.0` | 拉普拉斯噪声尺度 b |

## API 摘要

- `GET  /` — 查询页（含对账入口）
- `GET  /reconciliation` — 独立发布对账网页
- `GET  /api/status` — 导入标志、预算用量、缓存社区数（仅元信息）
- `POST /api/upload` — multipart 上传 CSV；重复住户号 400、已导入 409
- `POST /api/query` — `{"community": "..."}`；返回噪声户数、本次扣费、剩余预算；预算耗尽 429
- `GET  /api/reconciliation` — 发布对账：逐笔发布明细 + 账本核验告警（仅元信息）

`/api/reconciliation` 关键字段：

| 字段 | 含义 |
|------|------|
| `entries[]` | 逐笔发布：`community`、`released_at`、`epsilon`、`cumulative_spent`、`status`、`verifiable` |
| `ledger_charged_total` | 可核验明细的扣额合计 |
| `legacy_spent` | 旧结果在迁移时已计入账本的花费（单列，不参与逐笔核验）；未知时为 `null` |
| `expected_spent` / `budget_spent` | 对账期望值 / 账本实际值 |
| `ledger_consistent` / `ledger_difference` | 是否一致 / 差额 |
| `verified_release_count` / `legacy_release_count` | 可核验 / 无法核验的条数 |
| `flags[]` | 告警：`legacy_unverifiable`、`ledger_consistent` / `ledger_mismatch`、`result_without_release`、`release_without_result`、`release_result_mismatch` |
