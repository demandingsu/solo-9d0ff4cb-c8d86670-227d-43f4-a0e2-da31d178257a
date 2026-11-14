"""天平模拟器入口：同时启动 TCP 读数服务与 HTTP 控制服务。"""
from __future__ import annotations

import asyncio
import os
import signal

from .control_server import ControlHttpServer
from .state import SimulatorState
from .tcp_server import TcpBalanceServer

TCP_HOST = os.environ.get("SIM_TCP_HOST", "0.0.0.0")
TCP_PORT = int(os.environ.get("SIM_TCP_PORT", "9000"))
HTTP_HOST = os.environ.get("SIM_HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("SIM_HTTP_PORT", "9001"))


async def amain() -> None:
    state = SimulatorState()  # 初始即稳定零位
    tcp_server = TcpBalanceServer(state, TCP_HOST, TCP_PORT)
    http_server = ControlHttpServer(state, HTTP_HOST, HTTP_PORT)
    await tcp_server.start()
    await http_server.start()
    print(
        f"[simulator] TCP balance on {TCP_HOST}:{TCP_PORT}, "
        f"control HTTP on {HTTP_HOST}:{HTTP_PORT}",
        flush=True,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows
            pass
    try:
        await stop.wait()
    finally:
        await tcp_server.stop()
        await http_server.stop()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
