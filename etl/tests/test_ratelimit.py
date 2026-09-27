import pytest
from weather_etl.openmeteo import QuotaExceeded
from weather_etl.ratelimit import CallBudget

SMALL = (("minute", 60, 10.0), ("hour", 3600, 25.0), ("day", 86400, 40.0))


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.slept = getattr(self, "slept", []) + [s]
        self.t += s


@pytest.fixture
def clock():
    return Clock()


def budget(tmp_path, clock, **kw):
    return CallBudget(tmp_path / "rl.sqlite", limits=SMALL, clock=clock, sleep=clock.sleep, **kw)


def test_acquire_under_limits_does_not_wait(tmp_path, clock):
    b = budget(tmp_path, clock)
    b.acquire(4)
    b.acquire(4)
    assert not hasattr(clock, "slept")


def test_minute_window_waits_for_oldest_call_to_expire(tmp_path, clock):
    b = budget(tmp_path, clock)
    b.acquire(6)
    clock.t += 10
    b.acquire(6)  # 12 > 10 within a minute -> wait until first call ages out
    assert clock.slept == [pytest.approx(50.0)]


def test_hour_window_raises_quota(tmp_path, clock):
    b = budget(tmp_path, clock)
    for _ in range(3):
        b.acquire(8)
        clock.t += 61
    with pytest.raises(QuotaExceeded) as exc:
        b.acquire(8)
    assert exc.value.scope == "hour"


def test_day_window_raises_quota(tmp_path, clock):
    b = budget(tmp_path, clock)
    for _ in range(5):
        b.acquire(8)
        clock.t += 3601
    with pytest.raises(QuotaExceeded) as exc:
        b.acquire(8)
    assert exc.value.scope == "day"


def test_old_calls_are_forgotten(tmp_path, clock):
    b = budget(tmp_path, clock)
    for _ in range(5):
        b.acquire(8)
        clock.t += 3601
    clock.t += 86400
    b.acquire(8)  # all previous calls are older than a day


def test_weight_larger_than_smallest_limit_is_rejected(tmp_path, clock):
    with pytest.raises(ValueError, match="exceeds"):
        budget(tmp_path, clock).acquire(11)


def test_state_is_shared_between_instances(tmp_path, clock):
    budget(tmp_path, clock).acquire(6)
    budget(tmp_path, clock).acquire(6)
    assert clock.slept == [pytest.approx(60.0)]


def test_excessive_minute_wait_becomes_quota_error(tmp_path, clock):
    b = budget(tmp_path, clock, max_wait_s=5)
    b.acquire(6)
    with pytest.raises(QuotaExceeded) as exc:
        b.acquire(6)
    assert exc.value.scope == "minute"
