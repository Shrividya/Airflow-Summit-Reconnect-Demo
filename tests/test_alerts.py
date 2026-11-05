import json
import logging
from types import SimpleNamespace

import pytest
from healing import alerts
from healing.routing import Event, classify


def test_slack_transport_posts_prefixed_message(monkeypatch):
    seen = {}

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"ok"

    def fake_open(req, timeout):
        seen["url"], seen["body"] = req.full_url, json.loads(req.data)
        return Resp()

    monkeypatch.setattr(alerts, "get_connection", lambda _: SimpleNamespace(password="https://hooks/x"))
    monkeypatch.setattr(alerts.urllib.request, "urlopen", fake_open)
    e = Event(kind="exhausted", dag_id="d", task_id="t")
    alerts.slack_transport(e, classify(e))
    assert seen["url"] == "https://hooks/x"
    assert seen["body"]["text"].startswith(":rotating_light: [CRITICAL] d.t")


def test_missing_webhook_raises_so_dispatch_can_report_it(monkeypatch):
    monkeypatch.setattr(alerts, "get_connection", lambda _: SimpleNamespace(password=""))
    e = Event(kind="exhausted", dag_id="d")
    with pytest.raises(RuntimeError):
        alerts.slack_transport(e, classify(e))


def test_log_transport_emits_structured_line(caplog):
    e = Event(kind="recovered", dag_id="d", task_id="t")
    with caplog.at_level(logging.INFO):
        alerts.log_transport(e, classify(e))
    assert any("healing_event {" in r.getMessage() for r in caplog.records)


def test_non_https_webhook_is_rejected(monkeypatch):
    monkeypatch.setattr(alerts, "get_connection", lambda _: SimpleNamespace(password="file:///etc/passwd"))
    e = Event(kind="exhausted", dag_id="d")
    with pytest.raises(ValueError):
        alerts.slack_transport(e, classify(e))


def test_slack_renders_digest_links_as_mrkdwn(monkeypatch):
    from healing.digest import build_digest

    seen = {}

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"ok"

    def fake_open(req, timeout):
        seen["text"] = json.loads(req.data)["text"]
        return Resp()

    monkeypatch.setattr(alerts, "get_connection", lambda _: SimpleNamespace(password="https://hooks/x"))
    monkeypatch.setattr(alerts.urllib.request, "urlopen", fake_open)
    e = build_digest(
        dag_id="d",
        run_id="r",
        states={"load": "failed"},
        attempts={},
        absorbed={},
        url_for=lambda *a, **k: "http://af/dags/d/runs/r/tasks/load",
    )
    alerts.slack_transport(e, classify(e))
    assert seen["text"].startswith(":rotating_light:")
    assert "<http://af/dags/d/runs/r/tasks/load|open task>" in seen["text"]
