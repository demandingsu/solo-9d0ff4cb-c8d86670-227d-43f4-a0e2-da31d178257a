"""SQLite 异步访问层（aiosqlite）。

* WAL 模式：网页查询（读）与采集写入互不阻塞。
* 样品主数据预置；称量记录只增不改，断线/重启后记录保留。
* ``set_fail_next_writes`` 支持演示“入库失败后重试”，不影响正常流程。
"""
from __future__ import annotations

import os
from typing import Any

import aiosqlite

from . import config

SEED_SAMPLES = [
    ("S-001", "氯化钠标准样", 1),
    ("S-002", "葡萄糖试样", 1),
    ("S-003", "碳酸钙粉末", 1),
    ("S-004", "缓冲液 A", 1),
    ("S-005", "缓冲液 B", 1),
    ("S-006", "空白对照", 1),
]


class Database:
    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or config.DB_PATH
        self._conn: aiosqlite.Connection | None = None
        self._fail_remaining = 0  # 故障注入：剩余强制失败的写次数

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("数据库尚未初始化")
        return self._conn

    # ------------------------------------------------------------------ #
    async def init(self) -> None:
        directory = os.path.dirname(self.db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS samples (
                code      TEXT PRIMARY KEY,
                name      TEXT NOT NULL,
                active    INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS weighings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sample_code TEXT NOT NULL,
                weight_g    REAL NOT NULL,
                stable      INTEGER NOT NULL,
                recorded_at REAL NOT NULL,
                FOREIGN KEY (sample_code) REFERENCES samples(code)
            );
            CREATE INDEX IF NOT EXISTS idx_weighings_sample
                ON weighings(sample_code);
            """
        )
        # 幂等预置样品（已存在的不覆盖）
        for code, name, active in SEED_SAMPLES:
            await self._conn.execute(
                "INSERT OR IGNORE INTO samples(code, name, active) VALUES (?, ?, ?)",
                (code, name, active),
            )
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------ #
    # 故障注入（仅演示用）：让接下来 n 次写入抛出异常
    # ------------------------------------------------------------------ #
    def set_fail_next_writes(self, n: int) -> None:
        self._fail_remaining = max(0, int(n))

    def fail_remaining(self) -> int:
        return self._fail_remaining

    # ------------------------------------------------------------------ #
    async def list_samples(self) -> list[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT code, name, active FROM samples WHERE active = 1 ORDER BY code"
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def sample_exists(self, code: str) -> bool:
        async with self.conn.execute(
            "SELECT 1 FROM samples WHERE code = ? AND active = 1", (code,)
        ) as cur:
            row = await cur.fetchone()
        return row is not None

    async def insert_weighing(
        self, sample_code: str, weight_g: float, stable: bool, recorded_at: float
    ) -> int:
        """写入一条称量记录，返回自增 id。失败时抛异常，由调用方重试。"""
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            raise RuntimeError(
                f"模拟数据库写入故障（剩余 {self._fail_remaining} 次）"
            )
        cur = await self.conn.execute(
            "INSERT INTO weighings(sample_code, weight_g, stable, recorded_at)"
            " VALUES (?, ?, ?, ?)",
            (sample_code, float(weight_g), 1 if stable else 0, recorded_at),
        )
        await self.conn.commit()
        row_id = cur.lastrowid
        await cur.close()
        if row_id is None:  # pragma: no cover - 防御
            raise RuntimeError("数据库未返回记录 id")
        return row_id

    async def list_weighings(
        self, limit: int = 100, sample_code: str | None = None
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        if sample_code:
            sql = (
                "SELECT w.id, w.sample_code, s.name AS sample_name, w.weight_g,"
                " w.stable, w.recorded_at FROM weighings w"
                " JOIN samples s ON s.code = w.sample_code"
                " WHERE w.sample_code = ?"
                " ORDER BY w.id DESC LIMIT ?"
            )
            params: tuple[Any, ...] = (sample_code, limit)
        else:
            sql = (
                "SELECT w.id, w.sample_code, s.name AS sample_name, w.weight_g,"
                " w.stable, w.recorded_at FROM weighings w"
                " JOIN samples s ON s.code = w.sample_code"
                " ORDER BY w.id DESC LIMIT ?"
            )
            params = (limit,)
        async with self.conn.execute(sql, params) as cur:
            rows = await cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["stable"] = bool(d["stable"])
            result.append(d)
        return result
