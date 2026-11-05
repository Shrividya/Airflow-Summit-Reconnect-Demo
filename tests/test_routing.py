from healing.routing import (
    Event,
    Severity,
    classify,
    dispatch,
    format_message,
    route,
)


def ev(kind, **kw):
    return Event(kind=kind, dag_id="d", task_id="t", run_id="r1", **kw)


def test_severity_by_kind():
    assert classify(ev("retry")) is Severity.INFO
    assert classify(ev("recovered")) is Severity.WARNING
    assert classify(ev("exhausted")) is Severity.CRITICAL
    assert classify(ev("something_new")) is Severity.WARNING


def test_routes():
    assert route(Severity.INFO) == ("log",)
    assert route(Severity.WARNING) == ("log", "slack")
    assert route(Severity.CRITICAL) == ("log", "slack")


def test_per_task_events_never_reach_slack_whatever_their_severity():
    for kind in ("retry", "recovered", "exhausted", "fallback", "quarantined", "agent_healed"):
        assert route(classify(ev(kind)), kind) == ("log",)
    for kind in ("breaker_opened", "deadline_missed"):
        assert route(classify(ev(kind)), kind) == ("log", "slack")


def test_warnings_and_criticals_reach_slack_but_not_info():
    sent = []
    transports = {c: (lambda e, s, c=c: sent.append(c)) for c in ("log", "slack")}
    for kind in ("retry", "breaker_opened", "deadline_missed"):
        dispatch(ev(kind), transports)
    assert sent.count("slack") == 2
    assert sent.count("log") == 3


def test_dispatch_never_raises_when_a_transport_fails():
    def boom(e, s):
        raise RuntimeError("slack is down")

    result = dispatch(ev("breaker_opened"), {"log": lambda e, s: None, "slack": boom})
    assert result == {"log": True, "slack": False}


def test_missing_transport_is_reported_not_raised():
    assert dispatch(ev("breaker_opened"), {"log": lambda e, s: None}) == {
        "log": True,
        "slack": False,
    }


def test_message_has_context_and_truncates_errors():
    msg = format_message(ev("retry", attempt=2, max_attempts=5, error="x" * 500))
    assert "d.t" in msg and "attempt 2/5" in msg and "INFO" in msg
    assert len(msg) < 300


def test_new_healing_kinds_are_warnings_and_have_text():
    for kind in ("agent_healed", "deadline_missed"):
        assert classify(ev(kind)) is Severity.WARNING
        assert kind not in format_message(ev(kind))


def test_approval_request_reaches_slack_immediately():
    assert classify(ev("approval_needed")) is Severity.WARNING
    assert route(Severity.WARNING, "approval_needed") == ("log", "slack")
    assert "waiting for human approval" in format_message(ev("approval_needed"))
