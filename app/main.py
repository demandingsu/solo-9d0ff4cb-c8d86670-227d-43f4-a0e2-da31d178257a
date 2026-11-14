"""FastAPI 入口：REST API + 静态网页 + 采集后台任务生命周期。

所有浏览器页面都轮询同一个进程内的 :class:`WeighingStateMachine` 单例，
因此“多个页面共用服务端采集状态”。
"""
from __future__ import annotations

import contextlib
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import config
from .balance_service import BalanceService
from .database import Database
from .state_machine import SampleLockedError, WeighingStateMachine

WEB_DIR = os.path.join(os.path.dirname(__file__), "web")

# ---------------------------------------------------------------------- #
# 进程内单例
# ---------------------------------------------------------------------- #
db = Database()
sm = WeighingStateMachine()
balance = BalanceService(sm, db)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init()
    balance.start()
    try:
        yield
    finally:
        await balance.stop()
        await db.close()


app = FastAPI(title="实验室天平采集系统", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------- #
# 请求模型
# ---------------------------------------------------------------------- #
class SampleSelectBody(BaseModel):
    sample_code: str = Field(min_length=1, max_length=32)


class ControlBody(BaseModel):
    action: str = Field(min_length=1, max_length=32)
    weight: float | None = None


class FailInjectionBody(BaseModel):
    count: int = Field(ge=0, le=100)


# ---------------------------------------------------------------------- #
# 状态与样品
# ---------------------------------------------------------------------- #
@app.get("/api/status")
async def get_status() -> dict[str, Any]:
    snap = sm.snapshot()
    snap["simulator_fail_remaining"] = db.fail_remaining()
    return snap


@app.get("/api/samples")
async def get_samples() -> list[dict[str, Any]]:
    return await db.list_samples()


@app.post("/api/samples/select")
async def select_sample(body: SampleSelectBody) -> dict[str, Any]:
    if not await db.sample_exists(body.sample_code):
        raise HTTPException(status_code=404, detail=f"样品 {body.sample_code} 不存在")
    try:
        sm.select_sample(body.sample_code)
    except SampleLockedError as exc:
        # 稳定回零前拒绝换样
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, "selected_sample": sm.selected_sample}


# ---------------------------------------------------------------------- #
# 已存称量查询
# ---------------------------------------------------------------------- #
@app.get("/api/weighings")
async def get_weighings(limit: int = 100, sample_code: str | None = None) -> dict[str, Any]:
    if sample_code is not None and not await db.sample_exists(sample_code):
        raise HTTPException(status_code=404, detail=f"样品 {sample_code} 不存在")
    rows = await db.list_weighings(limit=limit, sample_code=sample_code)
    return {"count": len(rows), "items": rows}


# ---------------------------------------------------------------------- #
# 故障注入：演示“入库失败仍保留样品、后续稳定读数重试”
# ---------------------------------------------------------------------- #
@app.post("/api/dev/fail-next-writes")
async def fail_next_writes(body: FailInjectionBody) -> dict[str, Any]:
    db.set_fail_next_writes(body.count)
    return {"ok": True, "fail_remaining": db.fail_remaining()}


# ---------------------------------------------------------------------- #
# 模拟器控制代理（浏览器不直连模拟器，避免跨域/端口暴露）
# ---------------------------------------------------------------------- #
@app.post("/api/simulator/control")
async def simulator_control(body: ControlBody) -> dict[str, Any]:
    payload: dict[str, Any] = {"action": body.action}
    if body.weight is not None:
        payload["weight"] = body.weight
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"{config.SIMULATOR_CONTROL_URL}/control", json=payload
            )
        data = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(status_code=502, detail=f"模拟器控制失败：{exc}") from exc
    if not data.get("ok"):
        raise HTTPException(status_code=400, detail=data.get("error", "模拟器拒绝动作"))
    return data


@app.get("/api/simulator/state")
async def simulator_state() -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{config.SIMULATOR_CONTROL_URL}/state")
        return resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(status_code=502, detail=f"模拟器不可达：{exc}") from exc


# ---------------------------------------------------------------------- #
# 静态网页（多页面，共用同一套后端 API/状态）
# ---------------------------------------------------------------------- #
@app.get("/")
async def index() -> FileResponse:
    return FileResponse(os.path.join(WEB_DIR, "index.html"))


app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")
