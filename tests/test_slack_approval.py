import hashlib
import hmac
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest
from healing import alerts
from healing import slack_approval as sa
from healing.routing import Event, classify

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from approver import server  # noqa: E402

SECRET = "s3cret"


def sign(body: bytes, ts: int | None = None, secret: str = SECRET):
    ts = str(int(time.time()) if ts is None else ts)
    sig = "v0=" + hmac.new(secret.encode(), f"v0:{ts}:".encode() + body, hashlib.sha256).hexdigest()
    return {"X-Slack-Request-Timestamp": ts, "X-Slack-Signature": sig}


def click(decision="approve", dag="llm_healing", task="heal_settlements_schema", user="U1"):
    value = json.loads(sa.build_blocks("t", dag, "run1", task)[-1]["elements"][0 if decision == "approve" else 1]["value"])
    payload = {
        "type": "block_actions",
        "user": {"id": user},
        "response_url": "https://hooks.slack.com/actions/x",
        "message": {"blocks": [{"type": "section", "text": {"text": "orig"}}]},
        "actions": [
            {
                "action_id": sa.ACTION_APPROVE if decision == "approve" else sa.ACTION_DENY,
                "value": json.dumps(value),
            }
        ],
    }
    return payload


def body_of(payload) -> bytes:
    return urlencode({"payload": json.dumps(payload)}).encode()


@pytest.fixture
def calls(monkeypatch):
    posted = []
    monkeypatch.setattr(server, "_post_back", lambda url, payload: posted.append(payload))
    return posted


def run(payload, allowed=frozenset({"U1"}), headers=None, responder=None):
    body = body_of(payload)
    got = []
    resp = responder or (lambda *a: got.append(a))
    status, _ = server.handle(body, headers or sign(body), SECRET, allowed, respond=resp)
    return status, got


def test_signature_accepts_valid_rejects_tampered_stale_and_missing():
    body = b"payload=x"
    h = sign(body)
    assert sa.verify_signature(SECRET, h["X-Slack-Request-Timestamp"], body, h["X-Slack-Signature"])
    assert not sa.verify_signature(SECRET, h["X-Slack-Request-Timestamp"], body + b"!", h["X-Slack-Signature"])
    old = sign(body, ts=int(time.time()) - 1000)
    assert not sa.verify_signature(SECRET, old["X-Slack-Request-Timestamp"], body, old["X-Slack-Signature"])
    assert not sa.verify_signature("", h["X-Slack-Request-Timestamp"], body, h["X-Slack-Signature"])
    assert not sa.verify_signature(SECRET, "abc", body, h["X-Slack-Signature"])


def test_approve_calls_airflow_once(calls):
    status, got = run(click("approve"))
    assert status == 200 and got == [("llm_healing", "run1", "heal_settlements_schema", True)]
    assert "approved" in json.dumps(calls[-1]) and "actions" not in json.dumps(calls[-1])


def test_deny_calls_airflow_with_false(calls):
    _, got = run(click("deny"))
    assert got == [("llm_healing", "run1", "heal_settlements_schema", False)]


def test_unauthorized_user_cannot_approve(calls):
    status, got = run(click(user="U9"))
    assert status == 200 and got == []
    assert calls[-1]["response_type"] == "ephemeral"


def test_empty_allowlist_fails_closed(calls):
    assert not sa.is_authorized("U1", frozenset())
    _, got = run(click(), allowed=frozenset())
    assert got == []


def test_bad_signature_is_401_and_does_nothing(calls):
    body = body_of(click())
    got = []
    status, _ = server.handle(body, sign(body, secret="wrong"), SECRET, frozenset({"U1"}), respond=lambda *a: got.append(a))
    assert status == 401 and got == []


def test_signed_but_unlisted_target_is_rejected(calls):
    status, got = run(click(dag="other_dag"))
    assert status == 400 and got == []
    status, got = run(click(task="load_warehouse"))
    assert status == 400 and got == []


def test_action_value_mismatch_is_rejected(calls):
    p = click("approve")
    p["actions"][0]["action_id"] = sa.ACTION_DENY
    status, got = run(p)
    assert status == 400 and got == []


def test_airflow_failure_is_reported_not_raised(calls):
    def boom(*a):
        raise RuntimeError("409 already answered")

    status, _ = run(click(), responder=boom)
    assert status == 200 and "did not accept" in json.dumps(calls[-1])


def test_response_url_must_be_slack():
    assert sa.safe_response_url("https://hooks.slack.com/actions/x")
    assert not sa.safe_response_url("https://evil.example/hooks.slack.com")
    assert not sa.safe_response_url("http://hooks.slack.com/x")


def test_transport_adds_buttons_only_when_enabled(monkeypatch):
    seen = {}

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"ok"

    monkeypatch.setattr(alerts, "get_connection", lambda _: SimpleNamespace(password="https://hooks/x"))
    monkeypatch.setattr(alerts.urllib.request, "urlopen", lambda req, timeout: seen.update(b=json.loads(req.data)) or Resp())
    e = Event(kind="approval_needed", dag_id="llm_healing", task_id="heal_settlements_schema", run_id="r", url="http://x/t")
    monkeypatch.delenv("SLACK_APPROVAL_BUTTONS", raising=False)
    alerts.slack_transport(e, classify(e))
    assert "blocks" not in seen["b"]
    monkeypatch.setenv("SLACK_APPROVAL_BUTTONS", "1")
    alerts.slack_transport(e, classify(e))
    assert [x["action_id"] for x in seen["b"]["blocks"][-1]["elements"]] == [sa.ACTION_APPROVE, sa.ACTION_DENY]
