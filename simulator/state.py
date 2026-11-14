"""天平模拟器的共享状态（纯数据 + 同步方法，被 TCP/HTTP 两个服务共用）。"""
from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass, field

# 发送频率与模拟器内部时间常量
SEND_INTERVAL = 0.1          # 10 Hz
JITTER_AMPLITUDE = 0.15      # 抖动模式的波动幅度（克）
FRAGMENT_DELAY = 0.08        # 拆包时字节段之间的延迟（秒）


@dataclass
class SimulatorState:
    target_weight: float = 0.0          # 放样目标重量
    jitter: bool = False                # 抖动模式
    fragment: bool = False              # 拆包模式
    accepting: bool = True              # 是否接受 TCP 连接（断线演示）
    connected_clients: int = 0
    last_change_at: float = field(default_factory=time.time)

    def touch(self) -> None:
        self.last_change_at = time.time()

    # ------------------------------------------------------------------ #
    def reading(self) -> dict:
        """生成一条天平读数（JSON 报文内容）。"""
        if self.jitter:
            # 抖动：在目标重量附近正弦漂移 + 随机噪声，且稳定标志为 False
            noise = (
                math.sin(time.time() * 6.0) * JITTER_AMPLITUDE
                + random.uniform(-JITTER_AMPLITUDE, JITTER_AMPLITUDE) * 0.5
            )
            weight = self.target_weight + noise
            stable = False
        else:
            weight = float(self.target_weight)
            stable = True
        return {
            "weight_g": round(weight, 4),
            "stable": stable,
            "unit": "g",
            "ts": round(time.time(), 3),
        }

    def snapshot(self) -> dict:
        return {
            "target_weight": self.target_weight,
            "jitter": self.jitter,
            "fragment": self.fragment,
            "accepting": self.accepting,
            "connected_clients": self.connected_clients,
            "last_change_at": self.last_change_at,
        }
