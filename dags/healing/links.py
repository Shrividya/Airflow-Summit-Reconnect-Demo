from __future__ import annotations

import os
from urllib.parse import quote

DEFAULT_BASE_URL = "http://localhost:8080"


def _base_url() -> str:
    env = os.getenv("AIRFLOW__API__BASE_URL")
    if env:
        return env
    try:
        from airflow.sdk.configuration import conf

        return conf.get("api", "base_url", fallback=DEFAULT_BASE_URL)
    except Exception:  # noqa: BLE001
        return DEFAULT_BASE_URL


def task_url(
    dag_id: str, run_id: str, task_id: str, try_number: int | None = None, base_url: str | None = None
) -> str:
    base = (base_url or _base_url()).rstrip("/")
    url = f"{base}/dags/{quote(dag_id)}/runs/{quote(run_id)}/tasks/{quote(task_id)}"
    return f"{url}?try_number={try_number}" if try_number else url
