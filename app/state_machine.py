"""采集状态机（纯 Python 逻辑，不碰 IO，便于单测）。

阶段（phase）
-------------
``WAIT_ZERO``
    断线后 / 应用刚启动 / 重连后，尚未重新观察到**稳定零位**。
    此阶段不允许采集；浏览器选择样品会被保留，但不会锁定。
``READY``
    已观察到稳定零位，等待放样。
``HOLDING``
    重量已离开零位，当前样品被锁定，等待“首次正重量稳定读数”入库；
    稳定回零之前拒绝换样，且同一样品只记一条。

规则要点
--------
* 离开零位（|w| > eps，无论稳定与否）即锁定当前选中的样品。
* 只有进入 HOLDING 后出现的**首次正重量稳定读数**才触发一次入库。
* 入库失败不改变“尚未记录”的事实：后续每次正重量稳定读数都会重试，
  在成功之前绝不提前标记为已记。
* 稳定回零后解除锁定、回到 READY；未记录成功会给出告警事件。
* 断线 -> 回 WAIT_ZERO，清除锁定；已入库记录保留。重连后须再次看到稳定零位。
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from . import config


class Phase(str, Enum):
    WAIT_ZERO = "WAIT_ZERO"
    READY = "READY"
    HOLDING = "HOLDING"


class SampleLockedError(RuntimeError):
    """HOLDING 阶段（稳定回零前）尝试更换样品。"""


@dataclass
class Event:
    ts: float
    level: str  # info | warn | error
    message: str


@dataclass
class FeedResult:
    """一次读数驱动后的结果；``want_record`` 为 True 时由服务层执行入库。"""

    want_record: bool = False
    sample_code: str | None = None
    weight: float | None = None
    event: Event | None = None


@dataclass
class WeighingStateMachine:
    zero_epsilon: float = config.ZERO_EPSILON
    event_ring_size: int = config.EVENT_RING_SIZE

    phase: Phase = Phase.WAIT_ZERO
    connected: bool = False
    connection_generation: int = 0
    selected_sample: str | None = None
    locked_sample: str | None = None
    current_weight: float | None = None
    stable: bool = False
    recorded: bool = False
    record_attempts: int = 0
    no_sample_warning: bool = False
    last_error: str | None = None
    last_reading_at: float | None = None
    events: deque[Event] = field(init=False)

    def __post_init__(self) -> None:
        self.events = deque(maxlen=self.event_ring_size)
        self.add_event("info", "状态机已初始化，等待连接天平并观察稳定零位")

    # ------------------------------------------------------------------ #
    # 事件环（多个网页通过 GET /api/status 共享同一份服务端状态）
    # ------------------------------------------------------------------ #
    def add_event(self, level: str, message: str) -> Event:
        evt = Event(ts=time.time(), level=level, message=message)
        self.events.append(evt)
        return evt

    def _is_zero(self, weight: float) -> bool:
        return abs(weight) <= self.zero_epsilon

    # ------------------------------------------------------------------ #
    # 连接事件
    # ------------------------------------------------------------------ #
    def handle_connect(self) -> None:
        self.connected = True
        # 连接代数单调递增，供外部确定性地感知“发生了一次重连”
        self.connection_generation += 1
        # 新连接必须重新观察稳定零位（即使本进程之前已就绪）
        self.phase = Phase.WAIT_ZERO
        self.locked_sample = None
        self.recorded = False
        self.record_attempts = 0
        self.last_error = None
        self.add_event("info", "TCP 已连接，等待稳定零位…")

    def handle_disconnect(self, reason: str) -> None:
        self.connected = False
        self.phase = Phase.WAIT_ZERO
        self.stable = False
        self.current_weight = None
        self.locked_sample = None
        self.recorded = False
        self.record_attempts = 0
        self.no_sample_warning = False
        self.add_event("error", f"连接断开：{reason}；须重新观察稳定零位后才能采集（历史记录保留）")

    # ------------------------------------------------------------------ #
    # 样品选择
    # ------------------------------------------------------------------ #
    def select_sample(self, code: str) -> None:
        if self.phase is Phase.HOLDING:
            raise SampleLockedError(
                f"样品 {self.locked_sample} 已锁定，稳定回零前不能更换样品"
            )
        self.selected_sample = code
        self.no_sample_warning = False
        self.add_event("info", f"已选择样品 {code}")

    # ------------------------------------------------------------------ #
    # 读数驱动：返回服务层需要执行的动作
    # ------------------------------------------------------------------ #
    def feed_reading(self, weight: float, stable: bool, ts: float | None = None) -> FeedResult:
        ts = ts if ts is not None else time.time()
        self.last_reading_at = ts
        self.current_weight = weight
        self.stable = stable

        if self.phase is Phase.WAIT_ZERO:
            if stable and self._is_zero(weight):
                self.phase = Phase.READY
                return FeedResult(
                    event=self.add_event("info", "观察到稳定零位（0 g），可以开始采集")
                )
            return FeedResult()

        if self.phase is Phase.READY:
            if self._is_zero(weight):
                # 零位附近（含不稳定抖动），继续待命
                return FeedResult()
            # 离开零位 -> 尝试锁定当前样品
            if self.selected_sample is None:
                if not self.no_sample_warning:
                    self.no_sample_warning = True
                    return FeedResult(
                        event=self.add_event(
                            "warn", "重量已离开零位，但尚未选择样品；请先选择样品再放样"
                        )
                    )
                return FeedResult()
            self.phase = Phase.HOLDING
            self.locked_sample = self.selected_sample
            self.recorded = False
            self.record_attempts = 0
            self.last_error = None
            return FeedResult(
                event=self.add_event(
                    "info",
                    f"重量离开零位，样品 {self.locked_sample} 已锁定；等待首次正重量稳定读数",
                )
            )

        # HOLDING
        if stable and weight > self.zero_epsilon:
            if self.recorded:
                # 已记过：后续稳定读数一律忽略，杜绝重复记数
                return FeedResult()
            self.record_attempts += 1
            return FeedResult(
                want_record=True,
                sample_code=self.locked_sample,
                weight=weight,
            )

        if stable and self._is_zero(weight):
            # 稳定回零：解除锁定
            if not self.recorded:
                self.add_event(
                    "warn",
                    f"样品 {self.locked_sample} 稳定回零前未成功记录，本次不产生称量记录",
                )
            else:
                self.add_event("info", f"样品 {self.locked_sample} 已稳定回零，锁定解除")
            self.phase = Phase.READY
            self.locked_sample = None
            self.recorded = False
            self.record_attempts = 0
            self.last_error = None
            self.no_sample_warning = False
            return FeedResult()

        # 不稳定 / 负值稳定 / 非零：继续保持锁定
        return FeedResult()

    # ------------------------------------------------------------------ #
    # 入库结果回调（由服务层在 DB 操作后调用）
    # ------------------------------------------------------------------ #
    def notify_recorded(self, weighing_id: int, weight: float) -> None:
        self.recorded = True
        self.last_error = None
        self.add_event(
            "info",
            f"样品 {self.locked_sample} 称量 {weight:.4f} g 已入库（id={weighing_id}），锁定中不可重复记录",
        )

    def notify_record_failed(self, err: str) -> None:
        # 关键：不置 recorded，后续稳定读数仍会重试
        self.last_error = err
        self.add_event("error", f"入库失败（样品保留，稍后稳定读数自动重试）：{err}")

    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "connection_generation": self.connection_generation,
            "phase": self.phase.value,
            "selected_sample": self.selected_sample,
            "locked_sample": self.locked_sample,
            "current_weight": self.current_weight,
            "stable": self.stable,
            "recorded": self.recorded,
            "record_attempts": self.record_attempts,
            "no_sample_warning": self.no_sample_warning,
            "last_error": self.last_error,
            "last_reading_at": self.last_reading_at,
            "server_time": time.time(),
            "zero_epsilon": self.zero_epsilon,
            "events": [
                {"ts": e.ts, "level": e.level, "message": e.message}
                for e in self.events
            ],
        }
