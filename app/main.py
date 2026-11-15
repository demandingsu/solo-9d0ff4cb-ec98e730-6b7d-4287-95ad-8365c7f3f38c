"""FastAPI 接口层：上传单期 CSV、按社区查询带噪声的参与户数。

隐私约束：
- 查询只返回加噪（拉普拉斯，尺度 4）后的户数，绝不返回原始记录或精确计数；
- 全期隐私预算 ε=1，每个社区首次查询扣 0.25，再次查询返回持久化的同一结果；
- 预算扣减与结果保存由 dbcore 在同一 SQLite 事务中完成，多进程共享同一账本。
"""

import os
import sqlite3

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

import dbcore

DB_PATH = os.environ.get("DB_PATH", "/data/dp.db")

dbcore.init_db(DB_PATH)

app = FastAPI(
    title="社区参与户数差分隐私查询服务",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

_STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
with open(os.path.join(_STATIC, "index.html"), encoding="utf-8") as _f:
    INDEX_HTML = _f.read()


class QueryIn(BaseModel):
    community: str


@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML


@app.get("/api/status")
def status():
    try:
        return dbcore.get_status(DB_PATH)
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
