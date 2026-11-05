from __future__ import annotations

import logging

from .alerts import notify
from .callbacks import ATTEMPT_KEY
from .links import task_url
from .routing import DigestItem, Event, Severity

log = logging.getLogger(__name__)

_FAILED = {"failed", "upstream_failed"}


def build_digest(
    *,
    dag_id: str,
    run_id: str,
    states: dict[str, str],
    attempts: dict[str, dict],
    absorbed: dict[str, tuple[str, str]],
    notes: dict[str, str] | None = None,
    summary: str = "",
    url_for=task_url,
) -> Event:
    notes = notes or {}
    items: list[DigestItem] = []
    for task_id, state in sorted(states.items()):
        if state not in _FAILED:
            continue
        resolver, default_note = absorbed.get(task_id, ("", ""))
        url = url_for(dag_id, run_id, task_id)
        if state == "failed" and resolver and states.get(resolver) == "success":
            items.append(DigestItem("handled", task_id, f"failed, {notes.get(task_id, default_note)}", url))
        elif state == "failed":
            items.append(DigestItem("attention", task_id, "failed, retries exhausted", url))
        else:
            items.append(DigestItem("attention", task_id, "did not run, an upstream task failed", url))
    items.sort(key=lambda i: (i.section != "attention", i.task_id))
    for task_id, info in sorted(attempts.items()):
        if states.get(task_id) == "success":
            text = f"succeeded on attempt {info['attempt']}/{info['max']}"
            items.append(
                DigestItem("recovered", task_id, text, url_for(dag_id, run_id, task_id, info["attempt"]))
            )
    if any(i.section == "attention" for i in items):
        level = Severity.CRITICAL
    elif items:
        level = Severity.WARNING
    else:
        level = Severity.INFO
    return Event(kind="digest", dag_id=dag_id, run_id=run_id, error=summary, items=tuple(items), level=level)


def _states_for_run(ti, dag_id: str, run_id: str) -> dict[str, str]:
    raw = ti.get_task_states(dag_id=dag_id, run_ids=[run_id]) or {}
    scoped = raw.get(run_id, raw)
    return {str(t): str(getattr(s, "value", s)) for t, s in scoped.items()}


def post_run_digest(
    context: dict, absorbed: dict[str, tuple[str, str]], notes=None, summary: str = ""
) -> Event | None:
    try:
        ti = context["ti"]
        run_id = context["run_id"]
        states = _states_for_run(ti, ti.dag_id, run_id)
        states.pop(ti.task_id, None)
        candidates = [t for t, s in states.items() if s == "success"]
        pulled = ti.xcom_pull(task_ids=candidates, key=ATTEMPT_KEY) if candidates else []
        attempts = {t: v for t, v in zip(candidates, pulled or [], strict=False) if isinstance(v, dict)}
        event = build_digest(
            dag_id=ti.dag_id,
            run_id=run_id,
            states=states,
            attempts=attempts,
            absorbed=absorbed,
            notes=notes,
            summary=summary,
        )
        notify(event)
        return event
    except Exception:  # noqa: BLE001
        log.exception("post_run_digest failed")
        return None
