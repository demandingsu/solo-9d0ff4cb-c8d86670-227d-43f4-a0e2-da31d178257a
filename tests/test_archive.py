"""归档：在线行搬入独立 SQLite 档案文件，合并查询、并发、失败与重启恢复。"""
import asyncio
import os
import sqlite3
import time

import pytest

from app.database import Database


def _make_db(tmp_path, name="t.db") -> Database:
    return Database(str(tmp_path / name), archive_dir=str(tmp_path / "archives"))


async def _seed(db: Database, n: int) -> list[int]:
    """插入 n 条称量（样品轮换），返回自增 id 列表。"""
    ids = []
    samples = ["S-001", "S-002", "S-003"]
    for i in range(n):
        ids.append(
            await db.insert_weighing(samples[i % 3], 10.0 + i, True, 1700000000.0 + i)
        )
    return ids


# ---------------------------------------------------------------------- #
# 基本归档行为
# ---------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_archive_moves_rows_preserving_ids_and_values(tmp_path):
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 5)
    before = await db.list_weighings(limit=500)

    result = await db.archive_weighings(ids[2])  # 截止第 3 条
    assert result["status"] == "archived"
    assert result["range"] == {"min_id": ids[0], "max_id": ids[2]}
    assert result["row_count"] == 3
    assert result["file_name"]
    assert result["online_remaining"] == 2

    # 在线只剩后两条；合并查询仍能看到全部 5 条，且字段值原样保留
    merged = await db.list_weighings(limit=500)
    assert [r["id"] for r in merged] == sorted(ids, reverse=True)
    by_id = {r["id"]: r for r in merged}
    for old in before:
        new = by_id[old["id"]]
        for key in ("sample_code", "weight_g", "stable", "recorded_at", "sample_name"):
            assert new[key] == old[key], f"id={old['id']} 字段 {key} 被改变"
    # 来源标记：前 3 条来自归档，后 2 条在线
    assert [by_id[i]["source"] for i in ids[:3]] == ["archive"] * 3
    assert [by_id[i]["source"] for i in ids[3:]] == ["online"] * 2
    await db.close()


@pytest.mark.asyncio
async def test_archive_file_content_and_meta(tmp_path):
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 4)
    result = await db.archive_weighings(ids[3])
    await db.close()

    path = os.path.join(str(tmp_path / "archives"), result["file_name"])
    assert os.path.exists(path)
    # 归档文件是独立 SQLite：原 ID、原字段值、内嵌元信息
    conn = sqlite3.connect(path)
    rows = conn.execute(
        "SELECT id, sample_code, weight_g, stable, recorded_at FROM weighings ORDER BY id"
    ).fetchall()
    assert [r[0] for r in rows] == ids
    assert rows[0][2] == pytest.approx(10.0)
    meta = dict(conn.execute("SELECT key, value FROM archive_meta").fetchall())
    assert meta["min_id"] == str(ids[0])
    assert meta["max_id"] == str(ids[3])
    assert meta["row_count"] == "4"
    conn.close()
    # 单文件归档：无 WAL 副件残留
    assert not os.path.exists(path + "-wal")
    assert not os.path.exists(path + "-shm")


@pytest.mark.asyncio
async def test_merged_query_order_limit_and_sample_filter(tmp_path):
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 10)  # 样品按 S-001/S-002/S-003 轮换
    await db.archive_weighings(ids[5])  # 归档前 6 条，在线留 4 条

    # ID 倒序 + 条数限制：跨在线/归并未裁剪
    rows = await db.list_weighings(limit=5)
    assert [r["id"] for r in rows] == sorted(ids, reverse=True)[:5]
    sources = [r["source"] for r in rows]
    assert sources == ["online"] * 4 + ["archive"]  # 第 5 条来自归档

    rows = await db.list_weighings(limit=3)
    assert [r["id"] for r in rows] == sorted(ids, reverse=True)[:3]

    # 样品筛选跨来源：S-001 落在 id 1,4,7,10（前两个已归档）
    rows = await db.list_weighings(limit=500, sample_code="S-001")
    assert [r["id"] for r in rows] == [ids[9], ids[6], ids[3], ids[0]]
    assert [r["source"] for r in rows] == ["online", "online", "archive", "archive"]
    assert all(r["sample_name"] for r in rows)

    # 全量：10 条不重不漏
    rows = await db.list_weighings(limit=500)
    assert sorted(r["id"] for r in rows) == ids
    await db.close()


@pytest.mark.asyncio
async def test_archive_noop_when_range_empty(tmp_path):
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 3)
    first = await db.archive_weighings(ids[2])
    assert first["status"] == "archived"

    # 重复归档同一范围：幂等 no-op，不产生新批次、不报错
    second = await db.archive_weighings(ids[2])
    assert second["status"] == "noop"
    assert second["row_count"] == 0
    status = await db.archive_status()
    assert len(status["batches"]) == 1
    assert status["archived_count"] == 3
    assert status["online_count"] == 0

    with pytest.raises(ValueError):
        await db.archive_weighings(0)
    await db.close()


# ---------------------------------------------------------------------- #
# 并发：并发归档不能重复收录同一条；归档期间采集照常入库
# ---------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_concurrent_archives_never_double_record(tmp_path):
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 20)

    # 多个重叠范围的归档请求并发提交
    results = await asyncio.gather(
        db.archive_weighings(ids[7]),
        db.archive_weighings(ids[19]),
        db.archive_weighings(ids[7]),
        db.archive_weighings(ids[11]),
    )
    assert all(r["status"] in ("archived", "noop") for r in results)

    status = await db.archive_status()
    assert status["online_count"] == 0
    assert status["archived_count"] == 20
    # 批次范围互不重叠且并集恰好覆盖 1..20
    ranges = sorted((b["min_id"], b["max_id"]) for b in status["batches"])
    covered: list[int] = []
    for lo, hi in ranges:
        covered.extend(range(lo, hi + 1))
    assert covered == ids

    merged = await db.list_weighings(limit=500)
    assert sorted(r["id"] for r in merged) == ids  # 无重复、无遗漏
    assert all(r["source"] == "archive" for r in merged)
    await db.close()


@pytest.mark.asyncio
async def test_acquisition_continues_during_archive(tmp_path, monkeypatch):
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 5)

    real_write = db._write_archive_file
    inserted_during: list[int] = []

    async def write_and_keep_acquiring(path, rows):
        # 归档写文件期间，采集任务照常写入新称量（id 大于截止 id）
        new_id = await db.insert_weighing("S-002", 99.9, True, time.time())
        inserted_during.append(new_id)
        await real_write(path, rows)

    monkeypatch.setattr(db, "_write_archive_file", write_and_keep_acquiring)
    result = await db.archive_weighings(ids[2])
    assert result["status"] == "archived"

    # 归档期间新入库的记录保持在线、未被归档吞掉
    new_id = inserted_during[0]
    assert new_id > ids[2]
    merged = await db.list_weighings(limit=500)
    by_id = {r["id"]: r for r in merged}
    assert by_id[new_id]["source"] == "online"
    assert by_id[new_id]["weight_g"] == pytest.approx(99.9)
    assert sorted(by_id) == ids + [new_id]  # 全部历史不重不漏
    await db.close()


@pytest.mark.asyncio
async def test_query_during_archive_switch_is_consistent(tmp_path, monkeypatch):
    """归档发布中途发起的查询：要么看到归档前（全在线），要么看到归档后，绝不漏/重。"""
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 6)

    real_register = db._register_and_delete
    mid_archive_snapshots: list[list[int]] = []

    async def register_and_observe(*args):
        # 档案文件已写好、尚未登记：此刻查询应看到全部在线（未登记的文件不参与查询）
        rows, _ = await db.query_weighings(limit=500)
        mid_archive_snapshots.append(sorted(r["id"] for r in rows))
        assert all(r["source"] == "online" for r in rows)
        await real_register(*args)

    monkeypatch.setattr(db, "_register_and_delete", register_and_observe)
    await db.archive_weighings(ids[3])

    assert mid_archive_snapshots == [ids]  # 归档切换中查询不漏不重
    after, _ = await db.query_weighings(limit=500)
    assert sorted(r["id"] for r in after) == ids
    await db.close()


# ---------------------------------------------------------------------- #
# 失败与恢复：保留在线行、不发布半成品、重启后历史完整、可重试
# ---------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_register_failure_keeps_online_rows_and_retry_succeeds(tmp_path):
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 4)

    db.set_fail_next_archive_register(1)
    with pytest.raises(RuntimeError, match="登记"):
        await db.archive_weighings(ids[1])

    # 在线行全部保留；登记表为空；档案文件已写出但未登记 -> 不参与查询
    assert len(await db.list_weighings(limit=500)) == 4
    status = await db.archive_status()
    assert status["batches"] == [] and status["archived_count"] == 0
    assert status["last_run"]["status"] == "error"
    assert status["last_run"]["error"]
    orphan = os.path.join(str(tmp_path / "archives"), f"weighings_{ids[0]:08d}_{ids[1]:08d}.db")
    assert os.path.exists(orphan)
    rows, warnings = await db.query_weighings(limit=500)
    assert [r["source"] for r in rows] == ["online"] * 4

    # 重试：同名孤儿文件被覆盖，归档成功，历史完整
    result = await db.archive_weighings(ids[1])
    assert result["status"] == "archived"
    merged = await db.list_weighings(limit=500)
    assert sorted(r["id"] for r in merged) == ids
    assert len(os.listdir(str(tmp_path / "archives"))) == 1  # 没有重复文件
    await db.close()


@pytest.mark.asyncio
async def test_write_failure_keeps_online_rows(tmp_path, monkeypatch):
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 3)

    async def boom(path, rows):
        raise OSError("磁盘已满（模拟）")

    monkeypatch.setattr(db, "_write_archive_file", boom)
    with pytest.raises(OSError):
        await db.archive_weighings(ids[1])

    # 写文件失败：在线行保留、无登记、无正式档案文件
    assert len(await db.list_weighings(limit=500)) == 3
    status = await db.archive_status()
    assert status["batches"] == []
    assert status["last_run"]["status"] == "error"
    archive_dir = str(tmp_path / "archives")
    leftovers = os.listdir(archive_dir) if os.path.isdir(archive_dir) else []
    assert [f for f in leftovers if f.endswith(".db")] == []
    await db.close()


@pytest.mark.asyncio
async def test_restart_keeps_full_history_and_sweeps_orphans(tmp_path):
    # 第一次运行：归档前 3 条，并伪造“归档中进程退出”留下的未登记残留
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 5)
    result = await db.archive_weighings(ids[2])
    archive_dir = str(tmp_path / "archives")
    orphan_db = os.path.join(archive_dir, "weighings_00000004_00000004.db")
    orphan_tmp = os.path.join(archive_dir, "weighings_00000004_00000005.db.tmp")
    for p in (orphan_db, orphan_tmp):
        conn = sqlite3.connect(p)
        conn.execute("CREATE TABLE weighings(id INTEGER PRIMARY KEY)")
        conn.close()
    await db.close()

    # 模拟重启：重新打开同一数据库，历史（在线 2 + 归档 3）完整可查
    db2 = _make_db(tmp_path)
    await db2.init()
    merged = await db2.list_weighings(limit=500)
    assert sorted(r["id"] for r in merged) == ids
    assert [r["source"] for r in merged].count("archive") == 3
    status = await db2.archive_status()
    assert len(status["batches"]) == 1
    assert status["batches"][0]["file_name"] == result["file_name"]
    # 未登记残留被清理，不参与查询、不妨碍重试
    assert not os.path.exists(orphan_db) and not os.path.exists(orphan_tmp)
    assert any("残留" in w for w in status["warnings"])

    # 重启后继续归档剩余在线行：正常工作
    more = await db2.archive_weighings(ids[4])
    assert more["status"] == "archived"
    assert (await db2.archive_status())["online_count"] == 0
    merged = await db2.list_weighings(limit=500)
    assert sorted(r["id"] for r in merged) == ids
    await db2.close()


@pytest.mark.asyncio
async def test_archive_status_counts(tmp_path):
    db = _make_db(tmp_path)
    await db.init()
    ids = await _seed(db, 7)
    await db.archive_weighings(ids[2])
    await db.archive_weighings(ids[4])

    status = await db.archive_status()
    assert status["online_count"] == 2
    assert status["archived_count"] == 5
    assert status["max_online_id"] == ids[6]
    assert [(b["min_id"], b["max_id"], b["row_count"]) for b in status["batches"]] == [
        (ids[0], ids[2], 3),
        (ids[3], ids[4], 2),
    ]
    assert status["last_run"]["status"] == "archived"
    assert status["archive_dir"] == str(tmp_path / "archives")
    await db.close()
