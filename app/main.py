"""FastAPI 接口层：上传单期 CSV、按社区查询带噪声的参与户数、发布对账。

隐私约束：
- 查询只返回加噪（拉普拉斯，尺度 4）后的户数，绝不返回原始记录或精确计数；
- 全期隐私预算 ε=1，每个社区首次查询扣 0.25，再次查询返回持久化的同一结果；
- 预算扣减、结果保存与发布明细由 dbcore 在同一 SQLite 事务中完成，多进程共享同一账本；
- 对账页只展示发布社区、时间、单次扣额与累计花费，同样不含原始记录或精确人数。
"""

import os
import sqlite3

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel

import dbcore

DB_PATH = os.environ.get("DB_PATH", "/data/dp.db")

dbcore.init_db(DB_PATH)
# 旧库升级：已有结果但没有逐笔明细的，补一条"不可核验"的基线快照（不补造扣费）
dbcore.migrate_release_ledger(DB_PATH)

app = FastAPI(
    title="社区参与户数差分隐私查询服务",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

_STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
with open(os.path.join(_STATIC, "index.html"), encoding="utf-8") as _f:
    INDEX_HTML = _f.read()
with open(os.path.join(_STATIC, "reconciliation.html"), encoding="utf-8") as _f:
    RECONCILIATION_HTML = _f.read()

_STATIC_FILES = {
    "style.css": "text/css; charset=utf-8",
    "recon.js": "application/javascript; charset=utf-8",
}


class QueryIn(BaseModel):
    community: str


@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML


@app.get("/reconciliation", response_class=HTMLResponse)
def reconciliation_page():
    """发布对账网页：逐笔发布明细 + 与预算账本的核验结果。"""
    return RECONCILIATION_HTML


@app.get("/static/{name}")
def static_file(name: str):
    """只允许白名单内的静态文件，避免路径穿越。"""
    if name not in _STATIC_FILES:
        raise HTTPException(404, "文件不存在")
    return FileResponse(
        os.path.join(_STATIC, name),
        media_type=_STATIC_FILES[name],
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/api/status")
def status():
    try:
        return dbcore.get_status(DB_PATH)
    except sqlite3.Error:
        raise HTTPException(500, "数据库读取失败，请重试")


@app.get("/api/reconciliation")
def reconciliation():
    """发布对账：逐笔发布明细 + 与预算账本的核验结果（只含元信息）。"""
    try:
        return dbcore.get_reconciliation(DB_PATH)
    except sqlite3.Error:
        raise HTTPException(500, "数据库读取失败，请重试")


@app.post("/api/upload")
def upload(file: UploadFile = File(...)):
    raw = file.file.read()
    try:
        records = dbcore.parse_csv(raw)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    try:
        n = dbcore.import_records(DB_PATH, records)
    except dbcore.AlreadyImported as exc:
        raise HTTPException(409, str(exc))
    except sqlite3.Error:
        raise HTTPException(500, "数据库写入失败，本次导入已整体回滚，未做任何更改，可安全重试")
    return {
        "ok": True,
        "rows": n,
        "message": f"导入成功，共 {n} 户；数据已锁定，后续上传将被拒绝",
    }


@app.post("/api/query")
def query(body: QueryIn):
    try:
        res = dbcore.query_community(DB_PATH, body.community)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except dbcore.NotImported as exc:
        raise HTTPException(409, str(exc))
    except dbcore.BudgetExhausted as exc:
        raise HTTPException(429, str(exc))
    except sqlite3.Error:
        raise HTTPException(500, "数据库错误，本次查询已回滚（未扣预算、未写结果），可安全重试")
    return {
        "ok": True,
        "community": res["community"],
        "noisy_count": round(res["noisy_count"], 2),
        "cached": res["cached"],
        "charged": res["charged"],
        "budget_total": res["budget_total"],
        "budget_spent": res["budget_spent"],
        "budget_remaining": round(res["budget_total"] - res["budget_spent"], 4),
    }
