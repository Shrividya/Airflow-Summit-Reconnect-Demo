from types import SimpleNamespace

import pytest
from healing import callbacks


def ctx(try_number=1, max_tries=4, exception=None):
    ti = SimpleNamespace(dag_id="d", task_id="t", try_number=try_number, max_tries=max_tries, run_id="r")
    return {"ti": ti, "run_id": "r", "exception": exception}


@pytest.fixture
def sent(monkeypatch):
    events = []
    monkeypatch.setattr(callbacks, "notify", events.append)
    return events


def test_event_counts_attempts_from_max_tries():
    e = callbacks.event_from_context(ctx(try_number=2, max_tries=4), "retry", error="boom")
    assert (e.attempt, e.max_attempts, e.error) == (2, 5, "boom")


def test_event_falls_back_to_context_exception():
    e = callbacks.event_from_context(ctx(exception=ValueError("bad")), "retry")
    assert e.error == "bad"


def test_success_on_first_try_is_silent(sent):
    callbacks.on_success(ctx(try_number=1))
    assert sent == []


def test_success_after_retry_reports_recovery(sent):
    callbacks.on_success(ctx(try_number=3))
    assert [e.kind for e in sent] == ["recovered"]


def test_a_broken_notifier_never_breaks_the_task(monkeypatch):
    def boom(_):
        raise RuntimeError("slack down")

    monkeypatch.setattr(callbacks, "notify", boom)
    callbacks.on_failure(ctx())
    callbacks.on_retry(ctx())
