"""归档 HTTP API：POST/GET /api/archives、合并查询、故障注入（不经过 lifespan）。"""
import time

import httpx
import pytest

from app import main as main_mod
from app.balance_service import BalanceService
from app.database import Database
from app.state_machine import WeighingStateMachine


@pytest.fixture
async def api_client(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "api.db"), archive_dir=str(tmp_path / "archives"))
    await db.init()
    sm = WeighingStateMachine()
    monkeypatch.setattr(main_mod, "db", db)
    monkeypatch.setattr(main_mod, "sm", sm)
    monkeypatch.setattr(main_mod, "balance", BalanceService(sm, db))
    transport = httpx.ASGITransport(app=main_mod.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, db
    await db.close()


async def _seed(db: Database, n: int) -> list[int]:
    return [
        await db.insert_weighing("S-001", 5.0 + i, True, 1700000000.0 + i)
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_archive_endpoints_and_merged_query(api_client):
    client, db = api_client
    ids = await _seed(db, 5)

    # 执行归档：截止第 3 条
    r = await client.post("/api/archives", json={"cutoff_id": ids[2]})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "archived"
    assert body["range"] == {"min_id": ids[0], "max_id": ids[2]}
    assert body["row_count"] == 3
    assert body["online_remaining"] == 2

    # 归档状态：范围、批次、计数
    r = await client.get("/api/archives")
    assert r.status_code == 200
    status = r.json()
    assert status["online_count"] == 2
    assert status["archived_count"] == 3
    assert len(status["batches"]) == 1
    assert status["batches"][0]["min_id"] == ids[0]
    assert status["last_run"]["status"] == "archived"

    # 原查询入口覆盖在线 + 归档：样品筛选、ID 倒序、条数限制
    r = await client.get("/api/weighings", params={"limit": 4})
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 4
    assert [i["id"] for i in data["items"]] == sorted(ids, reverse=True)[:4]
    sources = {i["id"]: i["source"] for i in data["items"]}
    assert sources[ids[4]] == "online" and sources[ids[1]] == "archive"

    r = await client.get("/api/weighings", params={"limit": 500, "sample_code": "S-001"})
    assert sorted(i["id"] for i in r.json()["items"]) == ids

    # 重复归档：幂等
    r = await client.post("/api/archives", json={"cutoff_id": ids[2]})
    assert r.json()["status"] == "noop"

    # 非法截止 ID
    r = await client.post("/api/archives", json={"cutoff_id": 0})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_archive_register_failure_via_api_keeps_online_rows(api_client):
    client, db = api_client
    ids = await _seed(db, 3)

    r = await client.post("/api/dev/fail-next-archive-register", json={"count": 1})
    assert r.json()["fail_remaining"] == 1

    r = await client.post("/api/archives", json={"cutoff_id": ids[1]})
    assert r.status_code == 500
    assert "归档失败" in r.json()["detail"]

    # 在线行保留，未发布半成品；状态里能看到错误
    r = await client.get("/api/weighings", params={"limit": 500})
    assert r.json()["count"] == 3
    status = (await client.get("/api/archives")).json()
    assert status["batches"] == []
    assert status["last_run"]["status"] == "error"
    assert status["last_run"]["error"]

    # 重试成功
    r = await client.post("/api/archives", json={"cutoff_id": ids[1]})
    assert r.json()["status"] == "archived"
    r = await client.get("/api/weighings", params={"limit": 500})
    assert r.json()["count"] == 3  # 归档后历史仍完整
