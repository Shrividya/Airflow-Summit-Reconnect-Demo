from __future__ import annotations

import json
import logging
import os
import sys
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from approver import airflow_api  # noqa: E402
from healing import slack_approval as sa  # noqa: E402

log = logging.getLogger("approver")
MAX_BODY = 64 * 1024


def _post_back(response_url: str, payload: dict) -> None:
    if not sa.safe_response_url(response_url):
        log.warning("refusing to post to response_url %r", response_url)
        return
    req = urllib.request.Request(  # noqa: S310
        response_url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=5).read()  # noqa: S310
    except Exception:  # noqa: BLE001
        log.exception("could not update the Slack message")


def handle(body: bytes, headers, secret: str, allowed: frozenset[str], respond=airflow_api.respond):
    if not sa.verify_signature(
        secret, headers.get("X-Slack-Request-Timestamp", ""), body, headers.get("X-Slack-Signature", "")
    ):
        return 401, "bad signature"
    try:
        payload = json.loads(parse_qs(body.decode())["payload"][0])
        action = sa.parse_interaction(payload)
    except (KeyError, IndexError, ValueError):
        return 400, "bad request"

    original = "\n".join(
        b["text"]["text"] for b in (payload.get("message") or {}).get("blocks", []) if b.get("type") == "section"
    ) or "Schema change approval"
    user = action["user_id"]
    if not sa.is_authorized(user, allowed):
        log.warning("unauthorized approval attempt by %s", user)
        _post_back(
            action["response_url"],
            {"response_type": "ephemeral", "replace_original": False, "text": "You are not on the approver list."},
        )
        return 200, ""
    try:
        respond(action["dag_id"], action["run_id"], action["task_id"], action["decision"] == "approve")
        outcome = "Airflow will resume the task."
        log.info("%s %s by %s", action["decision"], action["run_id"], user)
    except Exception as exc:  # noqa: BLE001
        log.exception("airflow rejected the response")
        outcome = f"Airflow did not accept it ({exc}). It may have already timed out or been answered."
    _post_back(
        action["response_url"],
        {"replace_original": True, "blocks": sa.result_blocks(original, action["decision"], user, outcome)},
    )
    return 200, ""


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        if self.path != "/slack/interactions":
            return self._reply(404, "not found")
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length > MAX_BODY:
            return self._reply(413, "too large")
        status, text = handle(
            self.rfile.read(length),
            self.headers,
            os.environ.get("SLACK_SIGNING_SECRET", ""),
            sa.approver_ids(),
        )
        self._reply(status, text)

    def do_GET(self):  # noqa: N802
        self._reply(200, "ok") if self.path == "/healthz" else self._reply(404, "not found")

    def _reply(self, status: int, text: str):
        data = text.encode()
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def main():
    logging.basicConfig(level=logging.INFO)
    if not os.getenv("SLACK_SIGNING_SECRET"):
        sys.exit("SLACK_SIGNING_SECRET is required")
    if not sa.approver_ids():
        log.warning("SLACK_APPROVER_IDS is empty: every click will be refused")
    ThreadingHTTPServer(("0.0.0.0", int(os.getenv("PORT", "8081"))), Handler).serve_forever()  # noqa: S104


if __name__ == "__main__":
    main()
