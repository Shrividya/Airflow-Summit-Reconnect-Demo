from __future__ import annotations

from datetime import timedelta

from .callbacks import on_failure, on_retry, on_success


def default_args(fast: bool = False) -> dict:
    return {
        "retries": 4,
        "retry_delay": timedelta(seconds=5) if fast else timedelta(minutes=1),
        "retry_exponential_backoff": 2.0,
        "max_retry_delay": timedelta(seconds=60) if fast else timedelta(minutes=15),
        "on_retry_callback": on_retry,
        "on_failure_callback": on_failure,
        "on_success_callback": on_success,
    }
