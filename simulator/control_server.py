"""模拟器 HTTP 控制接口（仅依赖标准库，轻量 HTTP/1.1）。

POST /control  body: {"action": "...", "weight": 12.3}
GET  /state    返回当前模拟器状态

动作：place / jitter_on / jitter_off / zero / fragment_on /
      fragment_off / disconnect / reconnect
"""
from __future__ import annotations

import asyncio
import json

from .state import SimulatorState


class ControlHttpServer:
    def __init__(self, state: SimulatorState, host: str, port: int) -> None:
        self.state = state
        self.host = host
        self.port = port
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._on_http, self.host, self.port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    # ------------------------------------------------------------------ #
    def _json_response(
        self, status: int, body: dict
    ) -> tuple[bytes, str]:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed"}.get(
            status, "OK"
        )
        header = (
            f"HTTP/1.1 {status} {reason}\r\n"
            "Content-Type: application/json; charset=utf-8\r\n"
            f"Content-Length: {len(payload)}\r\n"
            "Connection: close\r\n\r\n"
        )
        return header.encode("ascii") + payload, reason

    async def _on_http(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            parts = request_line.decode("ascii", errors="replace").split()
            if len(parts) < 2:
                writer.write(self._json_response(400, {"error": "bad request"})[0])
                return
            method, path = parts[0], parts[1]
            headers: dict[str, str] = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                if b":" in line:
                    k, v = line.decode("ascii", errors="replace").split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            if method == "GET" and path == "/state":
                writer.write(
                    self._json_response(200, {"ok": True, "state": self.state.snapshot()})[0]
                )
                return
            if method != "POST":
                writer.write(self._json_response(405, {"error": "method not allowed"})[0])
                return
            if path != "/control":
                writer.write(self._json_response(404, {"error": "not found"})[0])
                return

            length = int(headers.get("content-length", "0") or "0")
            raw = await reader.read(length) if length else b"{}"
            try:
                body = json.loads(raw.decode("utf-8"))
                if not isinstance(body, dict):
                    raise ValueError("body must be JSON object")
            except (ValueError, UnicodeDecodeError) as exc:
                writer.write(self._json_response(400, {"error": f"invalid JSON: {exc}"})[0])
                return

            result = self._apply_action(body)
            writer.write(self._json_response(200, result)[0])
        except (ConnectionError, OSError):
            pass
        finally:
            try:
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass

    # ------------------------------------------------------------------ #
    def _apply_action(self, body: dict) -> dict:
        action = str(body.get("action", "")).strip()
        st = self.state

        if action == "place":
            try:
                weight = float(body.get("weight", 10.0))
            except (TypeError, ValueError):
                return {"ok": False, "error": "weight 必须是数字"}
            if weight <= 0:
                return {"ok": False, "error": "放样重量必须为正数；回零请用 zero"}
            st.target_weight = weight
            st.jitter = False
            st.touch()
            return {"ok": True, "message": f"已放样 {weight} g（稳定）", "state": st.snapshot()}

        if action == "jitter_on":
            st.jitter = True
            st.touch()
            return {"ok": True, "message": "抖动已开启（读数不稳定）", "state": st.snapshot()}

        if action == "jitter_off":
            st.jitter = False
            st.touch()
            return {"ok": True, "message": "抖动已关闭（读数恢复稳定）", "state": st.snapshot()}

        if action == "zero":
            st.target_weight = 0.0
            st.jitter = False
            st.touch()
            return {"ok": True, "message": "已回零（稳定 0 g）", "state": st.snapshot()}

        if action == "fragment_on":
            st.fragment = True
            st.touch()
            return {"ok": True, "message": "拆包模式已开启（JSON 行分段发送）", "state": st.snapshot()}

        if action == "fragment_off":
            st.fragment = False
            st.touch()
            return {"ok": True, "message": "拆包模式已关闭", "state": st.snapshot()}

        if action == "disconnect":
            st.accepting = False
            st.touch()
            return {"ok": True, "message": "天平已断线（监听关闭、连接被踢）", "state": st.snapshot()}

        if action == "reconnect":
            st.accepting = True
            st.target_weight = 0.0
            st.jitter = False
            st.touch()
            return {
                "ok": True,
                "message": "天平已恢复连接，初始为稳定零位",
                "state": st.snapshot(),
            }

        return {"ok": False, "error": f"未知动作: {action}"}
