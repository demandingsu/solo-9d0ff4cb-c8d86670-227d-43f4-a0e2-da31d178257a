"""模拟器 TCP 服务：像真实天平一样按行连续发送 JSON 读数。

行为开关（由 HTTP 控制接口修改共享状态）：

* 拆包模式：把一行 JSON 切成多个字节段，段间 sleep，验证后端半包重组。
* 断线：关闭监听并踢掉当前连接；恢复后重新监听（连接后先发稳定零位）。

背压策略：广播循环直接向每个连接写入并 ``drain``，发送变慢（例如拆包延迟）
时整体发送节奏自然放缓，不使用无界队列堆积报文。
"""
from __future__ import annotations

import asyncio
import json
import random

from .state import FRAGMENT_DELAY, SEND_INTERVAL, SimulatorState


class TcpBalanceServer:
    def __init__(self, state: SimulatorState, host: str, port: int) -> None:
        self.state = state
        self.host = host
        self.port = port
        self._server: asyncio.AbstractServer | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        self._stop_event.clear()
        asyncio.create_task(self._supervise_listener(), name="sim-listener")
        asyncio.create_task(self._broadcast_loop(), name="sim-broadcast")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._server is not None:
            self._server.close()
        for writer in list(self._writers):
            writer.close()

    # ------------------------------------------------------------------ #
    async def _supervise_listener(self) -> None:
        """断线演示：accepting=False 时关闭监听；恢复后重新监听。"""
        while not self._stop_event.is_set():
            if self.state.accepting and self._server is None:
                try:
                    self._server = await asyncio.start_server(
                        self._on_client, self.host, self.port
                    )
                except OSError:
                    await asyncio.sleep(0.5)
                    continue
            elif not self.state.accepting and self._server is not None:
                # 踢掉所有现有客户端并停止监听
                for writer in list(self._writers):
                    writer.close()
                self._server.close()
                await self._server.wait_closed()
                self._server = None
            await asyncio.sleep(0.2)

    async def _on_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writers.add(writer)
        self.state.connected_clients = len(self._writers)
        try:
            # 读端只用于感知对端关闭；天平协议是单向推送
            while not reader.at_eof():
                chunk = await reader.read(1024)
                if not chunk:
                    break
        except (ConnectionError, OSError):
            pass
        finally:
            self._writers.discard(writer)
            self.state.connected_clients = len(self._writers)
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass

    async def _write_fragmented(
        self, writer: asyncio.StreamWriter, payload: bytes
    ) -> None:
        """把一行报文随机切成 2~4 段，段间延迟，制造真实拆包。"""
        if len(payload) <= 2:
            writer.write(payload)
            await writer.drain()
            return
        parts = min(len(payload), random.randint(2, 4))
        cut_points = sorted(random.sample(range(1, len(payload)), parts - 1))
        start = 0
        for cut in cut_points + [len(payload)]:
            writer.write(payload[start:cut])
            await writer.drain()
            start = cut
            await asyncio.sleep(FRAGMENT_DELAY)

    # ------------------------------------------------------------------ #
    async def _broadcast_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._writers:
                line = (
                    json.dumps(self.state.reading(), ensure_ascii=False) + "\n"
                ).encode("utf-8")
                for writer in list(self._writers):
                    try:
                        if self.state.fragment:
                            await self._write_fragmented(writer, line)
                        else:
                            writer.write(line)
                            await writer.drain()
                    except (ConnectionError, OSError, RuntimeError):
                        # 对端关闭/重置：交给连接协程清理
                        self._writers.discard(writer)
            await asyncio.sleep(SEND_INTERVAL)
