"""Database：预置样品、写入/查询、故障注入、持久化（重启后记录保留）。"""
import time

import pytest

from app.database import Database


@pytest.mark.asyncio
async def test_seed_samples_and_weighing_crud(tmp_path):
    db = Database(str(tmp_path / "t.db"), archive_dir=str(tmp_path / "archives"))
    await db.init()
    samples = await db.list_samples()
    codes = {s["code"] for s in samples}
    assert {"S-001", "S-002", "S-006"} <= codes
    assert await db.sample_exists("S-001")
    assert not await db.sample_exists("NOPE")

    wid = await db.insert_weighing("S-001", 12.5, True, time.time())
    assert wid > 0
    rows = await db.list_weighings()
    assert len(rows) == 1
    assert rows[0]["sample_code"] == "S-001"
    assert rows[0]["weight_g"] == 12.5
    assert rows[0]["stable"] is True
    assert rows[0]["sample_name"]  # JOIN 出样品名

    filtered = await db.list_weighings(sample_code="S-002")
    assert filtered == []
    await db.close()


@pytest.mark.asyncio
async def test_fail_injection_then_retry_succeeds(tmp_path):
    db = Database(str(tmp_path / "f.db"), archive_dir=str(tmp_path / "archives"))
    await db.init()
    db.set_fail_next_writes(2)
    with pytest.raises(RuntimeError):
        await db.insert_weighing("S-001", 1.0, True, time.time())
    with pytest.raises(RuntimeError):
        await db.insert_weighing("S-001", 1.0, True, time.time())
    assert db.fail_remaining() == 0
    # 失败的两次都没有落库
    assert (await db.list_weighings()) == []
    # 故障耗尽后重试成功
    wid = await db.insert_weighing("S-001", 1.0, True, time.time())
    assert wid > 0
    assert len(await db.list_weighings()) == 1
    await db.close()


@pytest.mark.asyncio
async def test_persistence_across_reopen(tmp_path):
    path = str(tmp_path / "p.db")
    db1 = Database(path)
    await db1.init()
    await db1.insert_weighing("S-003", 7.77, True, time.time())
    await db1.close()

    # 模拟应用重启：重新打开同一数据库文件，记录仍在，样品幂等预置
    db2 = Database(path)
    await db2.init()
    rows = await db2.list_weighings()
    assert len(rows) == 1 and rows[0]["weight_g"] == 7.77
    assert await db2.sample_exists("S-003")
    await db2.close()
