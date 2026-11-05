from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from urllib.parse import quote


class AirflowError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(f"airflow API {status}: {message}")
        self.status = status


def _base() -> str:
    return os.getenv("AIRFLOW_API_URL", "http://airflow:8080").rstrip("/")


def _call(method: str, path: str, body: dict | None = None, token: str | None = None):
    req = urllib.request.Request(  # noqa: S310
        _base() + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", **({"Authorization": f"Bearer {token}"} if token else {})},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raise AirflowError(exc.code, exc.read().decode(errors="replace")[:300]) from exc
    except urllib.error.URLError as exc:
        raise AirflowError(0, str(exc.reason)) from exc


def get_token() -> str:
    out = _call(
        "POST",
        "/auth/token",
        {"username": os.environ["AIRFLOW_API_USER"], "password": os.environ["AIRFLOW_API_PASSWORD"]},
    )
    return out["access_token"]


def respond(dag_id: str, run_id: str, task_id: str, approve: bool) -> None:
    options = (
        [os.getenv("HITL_APPROVE_OPTION", "Approve")] if approve else [os.getenv("HITL_DENY_OPTION", "Reject")]
    )
    path = os.getenv(
        "HITL_PATCH_PATH", "/api/v2/dags/{dag}/dagRuns/{run}/taskInstances/{task}/-1/hitlDetails"
    ).format(dag=quote(dag_id, safe=""), run=quote(run_id, safe=""), task=quote(task_id, safe=""))
    _call("PATCH", path, {"chosen_options": options}, token=get_token())
