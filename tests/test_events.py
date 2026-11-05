import json

from healing.routing import Event, Severity


def test_event_serialises_to_one_json_line_with_severity():
    e = Event(kind="exhausted", dag_id="d", task_id="t", attempt=5, max_attempts=5, error="x")
    out = json.loads(e.to_json())
    assert out["severity"] == "CRITICAL" and out["kind"] == "exhausted" and out["attempt"] == 5
    assert "\n" not in e.to_json()


def test_explicit_severity_overrides_classification():
    assert json.loads(Event(kind="retry", dag_id="d").to_json(Severity.WARNING))["severity"] == "WARNING"
