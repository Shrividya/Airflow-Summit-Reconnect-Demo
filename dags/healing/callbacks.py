from __future__ import annotations

import functools
import logging

from .alerts import notify
from .links import task_url
from .routing import Event

ATTEMPT_KEY = "healing_attempt"

log = logging.getLogger(__name__)


def event_from_context(context: dict, kind: str, error=None) -> Event:
    ti = context["ti"]
    attempt = int(getattr(ti, "try_number", 1) or 1)
    max_tries = getattr(ti, "max_tries", None)
    total = max_tries + 1 if isinstance(max_tries, int) and max_tries >= 0 else attempt
    if error is None:
        error = context.get("exception")
    return Event(
        kind=kind,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=context.get("run_id") or getattr(ti, "run_id", "") or "",
        attempt=attempt,
        max_attempts=max(total, attempt),
        error=str(error) if error else "",
        url=task_url(
            ti.dag_id, context.get("run_id") or getattr(ti, "run_id", "") or "", ti.task_id, attempt
        ),
    )


def _never_fail(fn):
    @functools.wraps(fn)
    def wrapper(context):
        try:
            fn(context)
        except Exception:  # noqa: BLE001
            log.exception("callback %s failed", fn.__name__)

    return wrapper


@_never_fail
def on_retry(context: dict) -> None:
    notify(event_from_context(context, "retry"))


@_never_fail
def on_failure(context: dict) -> None:
    notify(event_from_context(context, "exhausted"))


@_never_fail
def on_success(context: dict) -> None:
    event = event_from_context(context, "recovered")
    if event.attempt > 1:
        notify(event)
        context["ti"].xcom_push(key=ATTEMPT_KEY, value={"attempt": event.attempt, "max": event.max_attempts})
