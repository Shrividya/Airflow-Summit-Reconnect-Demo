from healing.breaker import CLOSED, HALF_OPEN, OPEN, CircuitBreaker, MemoryStore


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def make(threshold=3, cooldown=60.0):
    clock = Clock()
    return CircuitBreaker(MemoryStore(), threshold, cooldown, clock), clock


def test_starts_closed_and_allows():
    b, _ = make()
    assert b.state == CLOSED and b.allow()


def test_opens_after_threshold_failures():
    b, _ = make(threshold=3)
    assert b.record_failure() is None
    assert b.record_failure() is None
    assert b.record_failure() == "opened"
    assert b.state == OPEN and not b.allow()


def test_success_resets_the_failure_count():
    b, _ = make(threshold=3)
    b.record_failure()
    b.record_failure()
    b.record_success()
    assert b.record_failure() is None
    assert b.state == CLOSED


def test_open_stays_blocked_until_cooldown_then_one_probe():
    b, clock = make(threshold=1, cooldown=60)
    b.record_failure()
    clock.now += 59
    assert not b.allow()
    clock.now += 2
    assert b.allow()
    assert b.state == HALF_OPEN
    assert not b.allow()


def test_probe_success_closes():
    b, clock = make(threshold=1, cooldown=60)
    b.record_failure()
    clock.now += 61
    b.allow()
    assert b.record_success() == "closed"
    assert b.state == CLOSED and b.allow()


def test_probe_failure_reopens_for_another_cooldown():
    b, clock = make(threshold=1, cooldown=60)
    b.record_failure()
    clock.now += 61
    b.allow()
    assert b.record_failure() == "reopened"
    assert b.state == OPEN
    clock.now += 30
    assert not b.allow()


def test_lost_probe_is_retried_after_another_cooldown():
    b, clock = make(threshold=1, cooldown=60)
    b.record_failure()
    clock.now += 61
    assert b.allow()
    clock.now += 61
    assert b.allow()


def test_late_failure_while_open_is_ignored():
    b, _ = make(threshold=1)
    b.record_failure()
    assert b.record_failure() is None
