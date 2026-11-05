from __future__ import annotations

import json
import logging
import urllib.request

from .conn import get_connection
from . import slack_approval
from .routing import Event, Severity, dispatch, format_digest, format_message

log = logging.getLogger(__name__)

_LOG_LEVEL = {
    Severity.INFO: logging.INFO,
    Severity.WARNING: logging.WARNING,
    Severity.CRITICAL: logging.ERROR,
}

_PREFIX = {
    Severity.INFO: "",
    Severity.WARNING: ":warning: ",
    Severity.CRITICAL: ":rotating_light: ",
}


def log_transport(event: Event, severity: Severity) -> None:
    log.log(_LOG_LEVEL[severity], format_message(event, severity))
    log.log(_LOG_LEVEL[severity], "healing_event %s", event.to_json(severity))


def slack_transport(event: Event, severity: Severity) -> None:
    webhook = get_connection("alert_slack").password
    if not webhook:
        raise RuntimeError("alert_slack has no webhook URL; set SLACK_WEBHOOK_URL")
    if not webhook.startswith("https://"):
        raise ValueError("alert_slack webhook must be an https:// URL")
    if event.kind == "digest":
        text = format_digest(event, severity, link=lambda label, url: f"<{url}|{label}>")
    else:
        text = format_message(event, severity)
        if event.url:
            text = text.replace(f" {event.url}", f" <{event.url}|open task>")
    payload: dict = {"text": _PREFIX[severity] + text}
    if event.kind == "approval_needed" and slack_approval.buttons_enabled():
        payload["blocks"] = slack_approval.build_blocks(
            payload["text"], event.dag_id, event.run_id, event.task_id
        )
    body = json.dumps(payload).encode()
    req = urllib.request.Request(  # noqa: S310
        webhook, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=5.0) as resp:  # noqa: S310
        resp.read()


TRANSPORTS = {
    "log": log_transport,
    "slack": slack_transport,
}


def notify(event: Event) -> dict:
    return dispatch(event, TRANSPORTS)
