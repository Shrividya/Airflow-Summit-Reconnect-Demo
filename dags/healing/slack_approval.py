from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from urllib.parse import urlparse

ACTION_APPROVE = "schema_approve"
ACTION_DENY = "schema_deny"
MAX_SKEW_SECONDS = 300

ALLOWED_TARGETS = frozenset(
    {
        ("llm_healing", "heal_settlements_schema"),
        ("hybrid_healing", "heal_settlements_schema"),
    }
)


def buttons_enabled() -> bool:
    return os.getenv("SLACK_APPROVAL_BUTTONS", "").lower() in {"1", "true", "yes"}


def _value(decision: str, dag_id: str, run_id: str, task_id: str) -> str:
    return json.dumps({"d": decision, "dag": dag_id, "run": run_id, "task": task_id}, separators=(",", ":"))


def build_blocks(text: str, dag_id: str, run_id: str, task_id: str, url: str = "") -> list[dict]:
    blocks: list[dict] = [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
    if url:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"<{url}|open task>"}]})
    blocks.append(
        {
            "type": "actions",
            "block_id": "schema_approval",
            "elements": [
                {
                    "type": "button",
                    "action_id": ACTION_APPROVE,
                    "style": "primary",
                    "text": {"type": "plain_text", "text": "Approve DDL"},
                    "value": _value("approve", dag_id, run_id, task_id),
                    "confirm": {
                        "title": {"type": "plain_text", "text": "Run the proposed DDL?"},
                        "text": {
                            "type": "mrkdwn",
                            "text": "This adds a column to the settlements table. It cannot be undone from Slack.",
                        },
                        "confirm": {"type": "plain_text", "text": "Approve"},
                        "deny": {"type": "plain_text", "text": "Cancel"},
                    },
                },
                {
                    "type": "button",
                    "action_id": ACTION_DENY,
                    "style": "danger",
                    "text": {"type": "plain_text", "text": "Deny"},
                    "value": _value("deny", dag_id, run_id, task_id),
                },
            ],
        }
    )
    return blocks


def verify_signature(secret: str, timestamp: str, body: bytes, signature: str, now: float | None = None) -> bool:
    if not secret or not timestamp or not signature:
        return False
    try:
        age = abs((now if now is not None else time.time()) - int(timestamp))
    except ValueError:
        return False
    if age > MAX_SKEW_SECONDS:
        return False
    base = b"v0:" + timestamp.encode() + b":" + body
    expected = "v0=" + hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def approver_ids() -> frozenset[str]:
    return frozenset(i.strip() for i in os.getenv("SLACK_APPROVER_IDS", "").split(",") if i.strip())


def is_authorized(user_id: str, allowed: frozenset[str] | None = None) -> bool:
    allowed = approver_ids() if allowed is None else allowed
    return bool(user_id) and user_id in allowed


class BadInteraction(ValueError):
    pass


def parse_interaction(payload: dict) -> dict:
    if payload.get("type") != "block_actions":
        raise BadInteraction("not a block_actions payload")
    actions = payload.get("actions") or []
    if len(actions) != 1 or actions[0].get("action_id") not in {ACTION_APPROVE, ACTION_DENY}:
        raise BadInteraction("unexpected action")
    try:
        v = json.loads(actions[0]["value"])
        decision, dag_id, run_id, task_id = v["d"], v["dag"], v["run"], v["task"]
    except (KeyError, TypeError, ValueError) as exc:
        raise BadInteraction("malformed button value") from exc
    if decision not in {"approve", "deny"} or (dag_id, task_id) not in ALLOWED_TARGETS or not run_id:
        raise BadInteraction("target not allowed")
    if (actions[0]["action_id"] == ACTION_APPROVE) != (decision == "approve"):
        raise BadInteraction("action/value mismatch")
    return {
        "decision": decision,
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "user_id": (payload.get("user") or {}).get("id", ""),
        "response_url": payload.get("response_url", ""),
    }


def safe_response_url(url: str) -> bool:
    p = urlparse(url)
    return p.scheme == "https" and p.hostname == "hooks.slack.com"


def result_blocks(original_text: str, decision: str, user_id: str, outcome: str) -> list[dict]:
    verb = "approved" if decision == "approve" else "denied"
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": original_text}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"<@{user_id}> {verb} this. {outcome}"}]},
    ]
