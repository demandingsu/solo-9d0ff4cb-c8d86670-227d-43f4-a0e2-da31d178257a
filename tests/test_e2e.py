"""端到端：真实 TCP 模拟器 + BalanceService + SQLite，覆盖拆包、断线、入库重试。"""
import asyncio
import json
import socket
import time

import pytest

from app.balance_service import BalanceService
from app.database import Database
from app.state_machine import Phase, WeighingStateMachine
from simulator.control_server import ControlHttpServer
from simulator.state import SimulatorState
from simulator.tcp_server import TcpBalanceServer


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _control(http_port: int, action: str, **extra) -> dict:
    reader, writer = await asyncio.open_connection("127.0.0.1", http_port)
    body = json.dumps({"action": action, **extra}).encode()
    writer.write(
        (
            f"POST /control HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
        ).encode()
        + body
    )
    await writer.drain()
    raw = await reader.read()
    writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        pass
    payload = raw.split(b"\r\n\r\n", 1)[1]
    return json.loads(payload)


async def _wait_phase(sm: WeighingStateMachine, phase: Phase, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if sm.phase is phase:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"阶段未在 {timeout}s 内变为 {phase}，当前 {sm.phase}")


async def _row_count(db: Database) -> int:
    return len(await db.list_weighings(limit=500))


async def _wait(predicate, timeout: float = 5.0, interval: float = 0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await asyncio.sleep(interval)
    raise AssertionError("等待条件超时")


@pytest.fixture
async def harness(tmp_path):
    tcp_port = _free_port()
    http_port = _free_port()
    sim_state = SimulatorState()
    tcp_server = TcpBalanceServer(sim_state, "127.0.0.1", tcp_port)
    ctrl = ControlHttpServer(sim_state, "127.0.0.1", http_port)
    await tcp_server.start()
    await ctrl.start()

    db = Database(str(tmp_path / "e2e.db"))
    await db.init()
    sm = WeighingStateMachine()
    service = BalanceService(sm, db)

    # 让服务连到本测试随机端口
    from app import config as cfg
    cfg.BALANCE_HOST = "127.0.0.1"
    cfg.BALANCE_PORT = tcp_port
    cfg.BALANCE_RECONNECT_SECONDS = 0.2

    service.start()
    try:
        yield {
            "sm": sm,
            "db": db,
            "state": sim_state,
            "control": lambda action, **kw: _control(http_port, action, **kw),
            "tcp_port": tcp_port,
        }
    finally:
        await service.stop()
        await tcp_server.stop()
        await ctrl.stop()
        await db.close()


@pytest.mark.asyncio
async def test_full_collection_cycle_with_fragmentation(harness):
    sm, db, control = harness["sm"], harness["db"], harness["control"]

    # 1) 模拟器初始稳定零位 -> 自动就绪
    await _wait_phase(sm, Phase.READY)
    assert sm.connected is True

    # 2) 开启拆包：一行 JSON 被切成多段发送，后端必须能重组
    r = await control("fragment_on")
    assert r["ok"] and harness["state"].fragment is True

    # 3) 选择样品（服务端状态机单例）
    sm.select_sample("S-001")
    # 4) 放样：离开零位锁定，首个正稳定读数入库
    await control("place", weight=12.5)
    await _wait(lambda: sm.recorded is True)
    assert sm.phase is Phase.HOLDING
    assert sm.locked_sample == "S-001"
    rows = await db.list_weighings()
    assert len(rows) == 1
    assert rows[0]["weight_g"] == pytest.approx(12.5, abs=1e-4)
    assert rows[0]["stable"] is True
    first_id = rows[0]["id"]

    # 5) 重复稳定读数不产生第二条（防重复记数）
    await asyncio.sleep(0.5)
    rows = await db.list_weighings()
    assert len(rows) == 1 and rows[0]["id"] == first_id

    # 6) 锁定期间换样被拒
    with pytest.raises(Exception):
        sm.select_sample("S-002")

    # 7) 抖动 -> 不稳定读数，依然不新增记录
    await control("jitter_on")
    await _wait(lambda: sm.stable is False)
    assert len(await db.list_weighings()) == 1
    await control("jitter_off")
    await _wait(lambda: sm.stable is True)

    # 8) 稳定回零 -> 解锁，才能换下一个样品
    await control("fragment_off")
    await control("zero")
    await _wait_phase(sm, Phase.READY)
    assert sm.locked_sample is None
    sm.select_sample("S-002")
    await control("place", weight=3.25)
    await _wait(lambda: sm.recorded is True)

    async def _at_least_two_rows():
        return await _row_count(db) >= 2

    await _wait(_at_least_two_rows)
    rows = await db.list_weighings()
    assert len(rows) == 2
    assert {r["sample_code"] for r in rows} == {"S-001", "S-002"}


@pytest.mark.asyncio
async def test_db_failure_retry_then_success(harness):
    sm, db, control = harness["sm"], harness["db"], harness["control"]
    await _wait_phase(sm, Phase.READY)
    sm.select_sample("S-004")

    db.set_fail_next_writes(3)
    await control("place", weight=6.6)
    # 失败重试期间：样品保留、未标记已记、库里没记录
    await _wait(lambda: sm.record_attempts >= 1)
    # 至少存在一个“尝试过但仍未成功”的瞬间
    await _wait(lambda: sm.recorded is False and sm.record_attempts >= 1
                and sm.locked_sample == "S-004" and sm.phase is Phase.HOLDING)
    assert await db.list_weighings() == []
    # 故障次数耗尽后，后续稳定读数自动重试成功（不提前视为已记）
    await _wait(lambda: sm.recorded is True, timeout=8)
    rows = await db.list_weighings()
    assert len(rows) == 1
    assert rows[0]["sample_code"] == "S-004"
    await control("zero")
    await _wait_phase(sm, Phase.READY)


@pytest.mark.asyncio
async def test_disconnect_requires_stable_zero_and_keeps_records(harness):
    sm, db, control = harness["sm"], harness["db"], harness["control"]
    await _wait_phase(sm, Phase.READY)
    sm.select_sample("S-005")
    await control("place", weight=8.8)
    await _wait(lambda: sm.recorded is True)
    assert len(await db.list_weighings()) == 1

    # 断线：阶段回到 WAIT_ZERO，锁定清除
    await control("disconnect")
    await _wait(lambda: sm.connected is False)
    assert sm.phase is Phase.WAIT_ZERO
    assert sm.locked_sample is None

    # 恢复连接，模拟器初始为稳定零位；必须重新观察零位才就绪
    generation_before = sm.connection_generation
    await control("reconnect")
    # 连接代数 +1：确定性地证明发生了重连（重连瞬间阶段被强制复位为 WAIT_ZERO）
    await _wait(lambda: sm.connection_generation > generation_before, timeout=8)
    assert sm.connected is True
    # 随后收到稳定零位读数才推进到 READY
    await _wait_phase(sm, Phase.READY, timeout=8)

    # 历史记录保留
    rows = await db.list_weighings()
    assert len(rows) == 1 and rows[0]["weight_g"] == pytest.approx(8.8, abs=1e-4)

    # 重新观察零位后可以开始下一次采集
    sm.select_sample("S-006")
    await control("place", weight=1.0)
    await _wait(lambda: sm.recorded is True)
    assert len(await db.list_weighings()) == 2
