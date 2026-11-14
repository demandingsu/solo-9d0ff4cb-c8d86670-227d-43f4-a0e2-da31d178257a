"""SQLite 异步访问层（aiosqlite）。

* WAL 模式：网页查询（读）与采集写入互不阻塞。
* 样品主数据预置；称量记录只增不改，断线/重启后记录保留。
* ``set_fail_next_writes`` 支持演示“入库失败后重试”，不影响正常流程。

归档设计（在线表 → 独立 SQLite 档案文件）
----------------------------------------
* 归档 = 把 ``id <= cutoff`` 的在线称量按**原 ID、原字段值**写入
  ``archive_dir`` 下的独立 SQLite 文件，再把对应在线行删除。
* 发布顺序保证“不发布半成品、失败保留在线行”：
    1. 完整写入 ``*.tmp`` 临时文件并关闭（此阶段在线行原封不动）；
    2. ``os.replace`` 原子改名为正式文件名；
    3. 在**独立连接**的单个事务里同时 ``INSERT`` 登记表 + ``DELETE`` 在线行。
  第 1/2 步失败：在线行保留、登记表不变；第 3 步失败：事务回滚，
  在线行保留，遗留的未登记文件不参与任何查询，重试时同名覆盖。
* 查询一致性：登记表与在线表在**同一个读事务快照**中读取；
  归档文件一经登记即不可变。单次查询对“归档切换”既不漏行也不重复。
* 并发归档由进程内 asyncio 锁串行化；已归档的 ID 不再在线，
  后到的归档请求自然收录不到同一行（幂等 no-op）。
* 归档期间采集照常：新称量的自增 ID 恒大于截止 ID，落在归档范围之外。
"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import os
import time
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

# 归档文件内嵌的元信息键（便于实验复算时审计来源）
ARCHIVE_META_SOURCE = "weighings-online"


class Database:
    def __init__(self, db_path: str | None = None, archive_dir: str | None = None) -> None:
        self.db_path = db_path or config.DB_PATH
        self.archive_dir = archive_dir or config.ARCHIVE_DIR
        self._conn: aiosqlite.Connection | None = None      # 写连接：采集入库、样品、裸 SELECT
        self._rconn: aiosqlite.Connection | None = None     # 读连接：合并查询的一致性快照
        self._archive_conns: dict[str, aiosqlite.Connection] = {}  # 已登记归档文件的只读连接缓存
        self._read_lock = asyncio.Lock()     # 串行化读连接上的显式读事务
        self._archive_lock = asyncio.Lock()  # 串行化并发归档请求
        self._fail_remaining = 0             # 故障注入：剩余强制失败的写次数
        self._fail_archive_register = 0      # 故障注入：剩余强制失败的归档登记次数
        self.last_archive_run: dict[str, Any] | None = None  # 最近一次归档尝试的状态（内存态）
        self._startup_warnings: list[str] = []

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("数据库尚未初始化")
        return self._conn

    @property
    def rconn(self) -> aiosqlite.Connection:
        if self._rconn is None:
            raise RuntimeError("数据库尚未初始化")
        return self._rconn

    # ------------------------------------------------------------------ #
    async def init(self) -> None:
        directory = os.path.dirname(self.db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.execute("PRAGMA busy_timeout=5000")
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
            -- 归档登记表：只有登记在册的归档文件才参与查询；
            -- 登记与在线行删除在同一事务提交，二者要么同时可见要么同时不可见。
            CREATE TABLE IF NOT EXISTS archives (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name  TEXT NOT NULL UNIQUE,
                min_id     INTEGER NOT NULL,
                max_id     INTEGER NOT NULL,
                row_count  INTEGER NOT NULL,
                created_at REAL NOT NULL
            );
            """
        )
        # 幂等预置样品（已存在的不覆盖）
        for code, name, active in SEED_SAMPLES:
            await self._conn.execute(
                "INSERT OR IGNORE INTO samples(code, name, active) VALUES (?, ?, ?)",
                (code, name, active),
            )
        await self._conn.commit()

        # 专用读连接：合并查询在此开启显式读事务，保证“登记表+在线表”同一快照
        self._rconn = await aiosqlite.connect(self.db_path)
        self._rconn.row_factory = aiosqlite.Row
        await self._rconn.execute("PRAGMA journal_mode=WAL")
        await self._rconn.execute("PRAGMA busy_timeout=5000")

        await self._sweep_orphan_archives()

    async def close(self) -> None:
        for aconn in self._archive_conns.values():
            await aconn.close()
        self._archive_conns.clear()
        if self._rconn is not None:
            await self._rconn.close()
            self._rconn = None
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------ #
    # 故障注入（仅演示用）
    # ------------------------------------------------------------------ #
    def set_fail_next_writes(self, n: int) -> None:
        """让接下来 n 次称量入库抛出异常。"""
        self._fail_remaining = max(0, int(n))

    def fail_remaining(self) -> int:
        return self._fail_remaining

    def set_fail_next_archive_register(self, n: int) -> None:
        """让接下来 n 次归档“登记”失败（档案文件已写好，演示不发布半成品）。"""
        self._fail_archive_register = max(0, int(n))

    def fail_archive_register_remaining(self) -> int:
        return self._fail_archive_register

    # ------------------------------------------------------------------ #
    # 样品与采集入库（原有行为不变）
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

    # ------------------------------------------------------------------ #
    # 称量查询：在线 + 已登记归档合并（样品筛选、ID 倒序、条数限制保持不变）
    # ------------------------------------------------------------------ #
    async def list_weighings(
        self, limit: int = 100, sample_code: str | None = None
    ) -> list[dict[str, Any]]:
        rows, _warnings = await self.query_weighings(limit=limit, sample_code=sample_code)
        return rows

    async def query_weighings(
        self, limit: int = 100, sample_code: str | None = None
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """合并查询在线表与所有已登记归档文件。

        返回 ``(rows, warnings)``。每行带 ``source``（``online``/``archive``）。
        一致性：登记表与在线表取自同一读事务快照；归档文件登记后不可变，
        因此归档切换不会导致单次查询漏行或重复。
        """
        limit = max(1, min(int(limit), 500))
        warnings: list[str] = []
        async with self._read_lock:
            batches, online_rows = await self._read_registry_and_online(limit, sample_code)

        sources: list[list[dict[str, Any]]] = [online_rows]
        for batch in batches:
            try:
                aconn = await self._archive_conn(batch["file_name"])
                sources.append(
                    await self._fetch_weighings(
                        aconn, limit, sample_code, source="archive"
                    )
                )
            except Exception as exc:  # 文件被外部删除/损坏：跳过并显式告警
                warnings.append(f"归档文件 {batch['file_name']} 读取失败：{exc}")
        warnings.extend(self._startup_warnings)

        # 各源均按 id DESC 有序，k 路归并取全局前 limit 条
        merged = heapq.merge(*sources, key=lambda r: r["id"], reverse=True)
        rows = list(itertools.islice(merged, limit))

        names = await self._sample_names()
        for r in rows:
            r["sample_name"] = names.get(r["sample_code"], "")
        return rows, warnings

    async def _read_registry_and_online(
        self, limit: int, sample_code: str | None
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """在读连接的一个显式读事务里读取归档登记表 + 在线 top-N。"""
        await self.rconn.execute("BEGIN")
        try:
            async with self.rconn.execute(
                "SELECT id, file_name, min_id, max_id, row_count, created_at"
                " FROM archives ORDER BY min_id"
            ) as cur:
                batches = [dict(r) for r in await cur.fetchall()]
            online = await self._fetch_weighings(
                self.rconn, limit, sample_code, source="online"
            )
            await self.rconn.execute("COMMIT")
        except Exception:
            await self.rconn.execute("ROLLBACK")
            raise
        return batches, online

    @staticmethod
    async def _fetch_weighings(
        conn: aiosqlite.Connection,
        limit: int,
        sample_code: str | None,
        source: str,
    ) -> list[dict[str, Any]]:
        sql = "SELECT id, sample_code, weight_g, stable, recorded_at FROM weighings"
        params: list[Any] = []
        if sample_code:
            sql += " WHERE sample_code = ?"
            params.append(sample_code)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        async with conn.execute(sql, params) as cur:
            rows = await cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["stable"] = bool(d["stable"])
            d["source"] = source
            result.append(d)
        return result

    async def _sample_names(self) -> dict[str, str]:
        async with self.conn.execute("SELECT code, name FROM samples") as cur:
            rows = await cur.fetchall()
        return {r["code"]: r["name"] for r in rows}

    async def _archive_conn(self, file_name: str) -> aiosqlite.Connection:
        """已登记归档文件的只读连接（按文件名缓存；文件登记后不可变）。"""
        if os.path.basename(file_name) != file_name:  # 防御：登记表只应含裸文件名
            raise RuntimeError(f"非法归档文件名：{file_name!r}")
        cached = self._archive_conns.get(file_name)
        if cached is not None:
            return cached
        path = os.path.join(self.archive_dir, file_name)
        if not os.path.exists(path):
            raise FileNotFoundError(f"归档文件不存在：{path}")
        aconn = await aiosqlite.connect(path)
        aconn.row_factory = aiosqlite.Row
        await aconn.execute("PRAGMA query_only=ON")  # 归档只读，绝不回写
        self._archive_conns[file_name] = aconn
        return aconn

    # ------------------------------------------------------------------ #
    # 归档：在线行 → 独立 SQLite 档案文件 → 登记并删除在线行
    # ------------------------------------------------------------------ #
    async def archive_weighings(self, cutoff_id: int) -> dict[str, Any]:
        """把 ``id <= cutoff_id`` 的在线称量归档到独立 SQLite 文件。

        并发安全：进程内锁串行化；已归档的行不在线，重复请求是幂等 no-op。
        失败安全：写文件/登记任一失败都保留在线行，不发布半成品。
        """
        cutoff_id = int(cutoff_id)
        if cutoff_id < 1:
            raise ValueError("截止记录 ID 必须是正整数")

        async with self._archive_lock:
            run: dict[str, Any] = {
                "status": "running",
                "cutoff_id": cutoff_id,
                "min_id": None,
                "max_id": None,
                "row_count": 0,
                "file_name": None,
                "error": None,
                "started_at": time.time(),
                "finished_at": None,
            }
            self.last_archive_run = run
            try:
                rows = await self._select_online_up_to(cutoff_id)
                if not rows:
                    run["status"] = "noop"
                    run["finished_at"] = time.time()
                    return await self._archive_result(run)

                min_id, max_id = rows[0]["id"], rows[-1]["id"]
                file_name = f"weighings_{min_id:08d}_{max_id:08d}.db"
                run.update(
                    min_id=min_id, max_id=max_id,
                    row_count=len(rows), file_name=file_name,
                )
                os.makedirs(self.archive_dir, exist_ok=True)
                path = os.path.join(self.archive_dir, file_name)

                # 1) 完整写出档案文件（失败则在线行原样保留，登记表不变）
                await self._write_archive_file(path, rows)
                # 2) 登记 + 删除在线行，单事务提交（失败则回滚，在线行保留）
                await self._register_and_delete(
                    file_name, min_id, max_id, len(rows), cutoff_id
                )
                run["status"] = "archived"
                run["finished_at"] = time.time()
            except Exception as exc:
                run["status"] = "error"
                run["error"] = str(exc)
                run["finished_at"] = time.time()
                raise
            return await self._archive_result(run)

    async def _archive_result(self, run: dict[str, Any]) -> dict[str, Any]:
        online_remaining = await self._online_count()
        result = {
            "ok": run["status"] != "error",
            "status": run["status"],
            "cutoff_id": run["cutoff_id"],
            "range": (
                {"min_id": run["min_id"], "max_id": run["max_id"]}
                if run["min_id"] is not None
                else None
            ),
            "row_count": run["row_count"],
            "file_name": run["file_name"],
            "online_remaining": online_remaining,
            "error": run["error"],
        }
        if run["status"] == "archived":
            result["message"] = (
                f"已归档 ID {run['min_id']}–{run['max_id']} 共 {run['row_count']} 条"
                f" → {run['file_name']}；在线剩余 {online_remaining} 条"
            )
        elif run["status"] == "noop":
            result["message"] = (
                f"截止 ID {run['cutoff_id']} 范围内没有仍在线的称量，无需归档"
            )
        else:
            result["message"] = f"归档失败：{run['error']}（在线行保留，未发布半成品）"
        return result

    async def _select_online_up_to(self, cutoff_id: int) -> list[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT id, sample_code, weight_g, stable, recorded_at"
            " FROM weighings WHERE id <= ? ORDER BY id",
            (cutoff_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def _online_count(self) -> int:
        async with self.conn.execute("SELECT COUNT(*) AS c FROM weighings") as cur:
            row = await cur.fetchone()
        return int(row["c"])

    async def _write_archive_file(
        self, path: str, rows: list[dict[str, Any]]
    ) -> None:
        """把选中行按原 ID、原字段值写入独立 SQLite 文件（先写临时文件再原子改名）。

        同名正式文件只可能是上次崩溃遗留的未登记孤儿（已登记的范围不可能再次
        出现在在线表中），直接覆盖；查询只认登记表，覆盖过程对查询不可见。
        """
        tmp_path = f"{path}.tmp"
        for stale in (
            tmp_path, path, f"{path}-wal", f"{path}-shm", f"{path}-journal"
        ):
            if os.path.exists(stale):
                os.remove(stale)

        min_id, max_id = rows[0]["id"], rows[-1]["id"]
        aconn = await aiosqlite.connect(tmp_path)
        try:
            await aconn.execute("PRAGMA journal_mode=DELETE")  # 归档为单文件，无 WAL 副件
            await aconn.executescript(
                """
                CREATE TABLE weighings (
                    id          INTEGER PRIMARY KEY,  -- 保留原在线表 ID
                    sample_code TEXT NOT NULL,
                    weight_g    REAL NOT NULL,
                    stable      INTEGER NOT NULL,
                    recorded_at REAL NOT NULL
                );
                CREATE INDEX idx_weighings_sample ON weighings(sample_code);
                CREATE TABLE archive_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                """
            )
            await aconn.executemany(
                "INSERT INTO weighings(id, sample_code, weight_g, stable, recorded_at)"
                " VALUES (?, ?, ?, ?, ?)",
                [
                    (r["id"], r["sample_code"], r["weight_g"], r["stable"], r["recorded_at"])
                    for r in rows
                ],
            )
            await aconn.executemany(
                "INSERT INTO archive_meta(key, value) VALUES (?, ?)",
                [
                    ("source", ARCHIVE_META_SOURCE),
                    ("min_id", str(min_id)),
                    ("max_id", str(max_id)),
                    ("row_count", str(len(rows))),
                    ("archived_at", f"{time.time():.6f}"),
                ],
            )
            await aconn.commit()
        finally:
            await aconn.close()
        os.replace(tmp_path, path)  # 原子发布：正式文件名只会指向完整文件

    async def _register_and_delete(
        self, file_name: str, min_id: int, max_id: int, row_count: int, cutoff_id: int
    ) -> None:
        """登记归档文件并删除对应在线行——同一事务，要么都发生要么都不发生。

        使用独立短连接，避免与采集写连接上的隐式事务互相嵌套。
        """
        if self._fail_archive_register > 0:
            self._fail_archive_register -= 1
            raise RuntimeError(
                f"模拟归档登记故障（剩余 {self._fail_archive_register} 次）"
            )
        conn = await aiosqlite.connect(self.db_path)
        try:
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA busy_timeout=5000")
            await conn.execute("BEGIN IMMEDIATE")
            try:
                await conn.execute(
                    "INSERT INTO archives(file_name, min_id, max_id, row_count, created_at)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (file_name, min_id, max_id, row_count, time.time()),
                )
                cur = await conn.execute(
                    "DELETE FROM weighings WHERE id <= ?", (cutoff_id,)
                )
                deleted = cur.rowcount
                await cur.close()
                if deleted != row_count:
                    raise RuntimeError(
                        f"在线行删除数（{deleted}）与归档行数（{row_count}）不一致，已回滚"
                    )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
        finally:
            await conn.close()

    # ------------------------------------------------------------------ #
    # 归档状态（页面展示：范围、状态、错误）
    # ------------------------------------------------------------------ #
    async def list_archives(self) -> list[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT id, file_name, min_id, max_id, row_count, created_at"
            " FROM archives ORDER BY min_id"
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def archive_status(self) -> dict[str, Any]:
        batches = await self.list_archives()
        async with self.conn.execute(
            "SELECT COUNT(*) AS c, COALESCE(MAX(id), 0) AS m FROM weighings"
        ) as cur:
            row = await cur.fetchone()
        return {
            "online_count": int(row["c"]),
            "max_online_id": int(row["m"]),
            "archived_count": sum(b["row_count"] for b in batches),
            "batches": batches,
            "last_run": self.last_archive_run,
            "archive_dir": self.archive_dir,
            "warnings": list(self._startup_warnings),
        }

    async def _sweep_orphan_archives(self) -> None:
        """启动时清理未登记的归档残留；登记在册但缺失的文件给出告警。

        崩溃可能留下：``*.tmp`` 半成品、已改名但未登记的 ``*.db``。
        它们从未参与查询；这里清掉以免妨碍重试或占用空间。
        """
        if not os.path.isdir(self.archive_dir):
            return
        batches = await self.list_archives()
        registered = {b["file_name"] for b in batches}
        for name in sorted(os.listdir(self.archive_dir)):
            path = os.path.join(self.archive_dir, name)
            if name.endswith(".tmp") or (
                name.endswith(".db") and name not in registered
            ):
                for p in (path, f"{path}-wal", f"{path}-shm", f"{path}-journal"):
                    if os.path.exists(p):
                        os.remove(p)
                self._startup_warnings.append(f"已清理未登记的归档残留：{name}")
        for b in batches:
            if not os.path.exists(os.path.join(self.archive_dir, b["file_name"])):
                self._startup_warnings.append(
                    f"登记表中的归档文件缺失：{b['file_name']}（查询将跳过并告警）"
                )
