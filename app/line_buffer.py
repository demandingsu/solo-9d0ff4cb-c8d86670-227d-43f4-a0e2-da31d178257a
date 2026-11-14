"""TCP 报文拆分处理。

天平按行（``\\n`` 分隔）连续发送 JSON。TCP 是字节流，一次 recv 可能：

* 只收到一行的一部分；
* 一次收到多行；
* 一行被拆成若干次 recv 才到齐（对应模拟器的“拆包”演示）。

本模块只负责“喂字节 -> 吐出完整行”，与具体业务解耦，便于单测。
"""
from __future__ import annotations


class LineDecoder:
    """增量式换行解码器，兼容 ``\\n`` 与 ``\\r\\n``。"""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> list[str]:
        """喂入一段 TCP 字节，返回本次凑齐的所有完整行（不含换行符）。"""
        self._buffer.extend(chunk)
        lines: list[str] = []
        while True:
            nl = self._buffer.find(b"\n")
            if nl == -1:
                break
            raw = self._buffer[:nl]
            del self._buffer[: nl + 1]
            if raw.endswith(b"\r"):
                raw = raw[:-1]
            # 天平只发 ASCII JSON；忽略畸形编码字节而不是中断采集
            lines.append(raw.decode("utf-8", errors="replace"))
        return lines

    def pending(self) -> int:
        """残留在缓冲区、尚未凑成一行的字节数。"""
        return len(self._buffer)

    def reset(self) -> None:
        """连接断开时清空半包，避免旧连接残留污染新连接。"""
        self._buffer.clear()
