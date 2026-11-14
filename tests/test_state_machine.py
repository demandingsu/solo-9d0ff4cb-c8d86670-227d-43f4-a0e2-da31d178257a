"""状态机规则测试：锁定、首次稳定正读数、拒绝重复记、回零解锁、断线复位。"""
import pytest

from app.state_machine import Phase, SampleLockedError, WeighingStateMachine


def make_sm():
    sm = WeighingStateMachine()
    sm.handle_connect()
    return sm


def arm_ready(sm):
    """连接后观察稳定零位 -> READY。"""
    r = sm.feed_reading(0.0, stable=True)
    assert sm.phase is Phase.READY
    assert not r.want_record
    sm.select_sample("S-001")
    return sm


def test_must_observe_stable_zero_before_arming():
    sm = WeighingStateMachine()
    sm.handle_connect()
    # 非零读数不能解除 WAIT_ZERO
    assert sm.feed_reading(5.0, stable=True).want_record is False
    assert sm.phase is Phase.WAIT_ZERO
    # 不稳定零位也不算
    assert sm.feed_reading(0.0, stable=False).want_record is False
    assert sm.phase is Phase.WAIT_ZERO
    # 稳定零位才就绪
    sm.feed_reading(0.0, stable=True)
    assert sm.phase is Phase.READY


def test_leaving_zero_locks_selected_sample():
    sm = arm_ready(make_sm())
    # 不稳定的非零读数同样触发锁定（离开零位即锁）
    r = sm.feed_reading(0.3, stable=False)
    assert sm.phase is Phase.HOLDING
    assert sm.locked_sample == "S-001"
    assert not r.want_record  # 不稳定，还不能记


def test_first_positive_stable_reading_requests_one_record():
    sm = arm_ready(make_sm())
    sm.feed_reading(2.0, stable=False)  # 离开零位，锁定
    r1 = sm.feed_reading(12.5, stable=True)
    assert r1.want_record and r1.sample_code == "S-001" and r1.weight == 12.5
    # 模拟入库成功
    sm.notify_recorded(1, 12.5)
    assert sm.recorded is True
    # 后续稳定读数不得再产生记录（防重复记数）
    assert sm.feed_reading(12.5, stable=True).want_record is False
    assert sm.feed_reading(12.6, stable=True).want_record is False
    assert sm.record_attempts == 1


def test_unstable_and_non_positive_readings_never_record():
    sm = arm_ready(make_sm())
    sm.feed_reading(1.0, stable=False)
    assert sm.feed_reading(3.0, stable=False).want_record is False
    # 负重量稳定读数也不允许记
    assert sm.feed_reading(-2.0, stable=True).want_record is False
    assert sm.record_attempts == 0
    # 首个正稳定读数仍可正常记录
    r = sm.feed_reading(4.0, stable=True)
    assert r.want_record and r.weight == 4.0


def test_db_failure_keeps_sample_and_retries_until_success():
    sm = arm_ready(make_sm())
    sm.feed_reading(1.0, stable=True)  # 锁定
    first = sm.feed_reading(9.0, stable=True)
    assert first.want_record
    # 入库失败：绝不能提前标记为已记
    sm.notify_record_failed("disk full")
    assert sm.recorded is False
    assert sm.last_error == "disk full"
    # 下一个稳定读数再次要求入库（重试）
    retry = sm.feed_reading(9.1, stable=True)
    assert retry.want_record and retry.sample_code == "S-001"
    sm.notify_record_failed("still failing")
    assert sm.recorded is False
    # 再来一次，终于成功
    ok = sm.feed_reading(9.2, stable=True)
    assert ok.want_record
    sm.notify_recorded(7, 9.2)
    assert sm.recorded is True
    # 成功后不再重复
    assert sm.feed_reading(9.2, stable=True).want_record is False


def test_sample_change_rejected_while_locked_until_stable_zero():
    sm = arm_ready(make_sm())
    sm.feed_reading(1.0, stable=True)  # 锁定 S-001
    with pytest.raises(SampleLockedError):
        sm.select_sample("S-002")
    # 非稳定回零不能解锁
    sm.feed_reading(0.0, stable=False)
    assert sm.phase is Phase.HOLDING
    with pytest.raises(SampleLockedError):
        sm.select_sample("S-002")
    # 稳定回零解锁，之后才能换样
    sm.feed_reading(0.0, stable=True)
    assert sm.phase is Phase.READY
    assert sm.locked_sample is None
    sm.select_sample("S-002")
    assert sm.selected_sample == "S-002"


def test_returning_zero_without_record_warns_but_no_record():
    sm = arm_ready(make_sm())
    sm.feed_reading(1.0, stable=False)  # 锁定但始终不稳定，没记成
    sm.feed_reading(0.5, stable=False)
    r = sm.feed_reading(0.0, stable=True)  # 稳定回零
    assert not r.want_record
    assert sm.phase is Phase.READY
    warn_events = [e for e in sm.events if e.level == "warn"]
    assert any("未成功记录" in e.message for e in warn_events)


def test_no_sample_selected_when_leaving_zero_warns_and_stays_ready():
    sm = make_sm()
    sm.feed_reading(0.0, stable=True)  # READY，但不选样品
    r = sm.feed_reading(3.0, stable=True)
    assert not r.want_record
    assert sm.phase is Phase.READY  # 没东西可锁，不进 HOLDING
    # 回零后选样品，再放样可以正常采集
    sm.feed_reading(0.0, stable=True)
    sm.select_sample("S-003")
    sm.feed_reading(2.0, stable=True)
    assert sm.phase is Phase.HOLDING
    assert sm.locked_sample == "S-003"


def test_disconnect_resets_and_requires_stable_zero_again():
    sm = arm_ready(make_sm())
    sm.feed_reading(5.0, stable=True)
    assert sm.phase is Phase.HOLDING
    sm.handle_disconnect("模拟断线")
    assert sm.phase is Phase.WAIT_ZERO
    assert sm.connected is False
    assert sm.locked_sample is None
    assert sm.recorded is False
    # 重连后即使直接来稳定正读数，也不能记，必须先看到稳定零位
    sm.handle_connect()
    assert sm.feed_reading(5.0, stable=True).want_record is False
    assert sm.phase is Phase.WAIT_ZERO
    sm.feed_reading(0.0, stable=True)
    assert sm.phase is Phase.READY


def test_initial_state_waits_for_zero():
    sm = WeighingStateMachine()  # 未连接
    assert sm.phase is Phase.WAIT_ZERO
    assert sm.connected is False
    # 重启后的新实例：没有任何残留锁定
    assert sm.locked_sample is None and sm.selected_sample is None


def test_events_ring_bounded():
    sm = WeighingStateMachine(event_ring_size=5)
    for i in range(20):
        sm.add_event("info", f"e{i}")
    assert len(sm.events) == 5
    assert sm.events[-1].message == "e19"
