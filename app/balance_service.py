"""天平 TCP 采集客户端（单例后台任务）。

* 全应用只有一个采集循环，因此所有浏览器页面看到的是同一份服务端采集状态。
* 自动重连；每次（重新）连接都重置半包缓冲并要求重新观察稳定零位。
* 用 :class:`~app.line_buffer.LineDecoder` 处理 JSON 报文拆分/粘包。
* 状态机判定需要入库时执行 DB 写入；写入失败不改变状态机“未记录”事实，
  后续稳定读数会再次触发写入重试。
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from . import config
from .line_buffer import LineDecoder
from .state_machine import WeighingStateMachine


class BalanceService:
    def __init__(self, sm: WeighingStateMachine, db: Any) -> None:
        self.sm = sm
        self.db = db
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run(), name="balance-client")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    # ------------------------------------------------------------------ #
    async def _run(self) -> None:
        decoder = LineDecoder()
        while not self._stop_event.is_set():
            try:
                reader, _writer = await asyncio.wait_for(
                    asyncio.open_connection(config.BALANCE_HOST, config.BALANCE_PORT),
                    timeout=5,
                )
            except (asyncio.TimeoutError, OSError) as exc:
                self.sm.handle_disconnect(f"无法连接 {config.BALANCE_HOST}:{config.BALANCE_PORT}（{exc}）")
                decoder.reset()
                await self._sleep_reconnect()
                continue

            decoder.reset()
            self.sm.handle_connect()
            try:
                while not reader.at_eof():
                    chunk = await reader.read(config.SOCKET_RECV_BYTES)
                    if not chunk:
                        break  # 对端关闭连接
                    for line in decoder.feed(chunk):
                        await self._handle_line(line)
            except (ConnectionError, OSError) as exc:
                self.sm.handle_disconnect(f"读取失败：{exc}")
            else:
                self.sm.handle_disconnect("对端关闭了连接")
            finally:
                decoder.reset()
            await self._sleep_reconnect()

    async def _sleep_reconnect(self) -> None:
        try:
            await asyncio.wait_for(
                self._stop_event.wait(), timeout=config.BALANCE_RECONNECT_SECONDS
            )
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------------ #
    async def _handle_line(self, line: str) -> None:
        line = line.strip()
        if not line:
            return
        try:
            payload = json.loads(line)
            weight = float(payload["weight_g"])
            stable = bool(payload["stable"])
        except (ValueError, KeyError, TypeError) as exc:
            # 单条畸形报文不能杀死采集循环
            self.sm.add_event("warn", f"忽略无法解析的报文：{line[:80]}（{exc}）")
            return

        result = self.sm.feed_reading(weight, stable)
        if result.event is not None:
            pass  # 事件已在状态机内入环
        if result.want_record and result.sample_code is not None:
            try:
                weighing_id = await self.db.insert_weighing(
                    sample_code=result.sample_code,
                    weight_g=result.weight if result.weight is not None else weight,
                    stable=True,
                    recorded_at=time.time(),
                )
                self.sm.notify_recorded(
                    weighing_id, result.weight if result.weight is not None else weight
                )
            except Exception as exc:  # 入库失败：样品保留，等待后续稳定读数重试
                self.sm.notify_record_failed(str(exc))
