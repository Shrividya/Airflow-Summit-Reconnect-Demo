from __future__ import annotations

import logging

from .alerts import notify
from .breaker import CircuitBreaker
from .callbacks import event_from_context, on_failure, on_success
from .routing import Event
from .stores import VariableStore

log = logging.getLogger(__name__)


def warehouse_breaker() -> CircuitBreaker:
    return CircuitBreaker(VariableStore("breaker_warehouse"), failure_threshold=3, cooldown_seconds=60)


def _failed(context):
    try:
        if warehouse_breaker().record_failure() == "opened":
            notify(event_from_context(context, "breaker_opened"))
    except Exception:  # noqa: BLE001
        pass


def _succeeded(context):
    try:
        if warehouse_breaker().record_success() == "closed":
            notify(event_from_context(context, "breaker_closed"))
    except Exception:  # noqa: BLE001
        pass


BREAKER_FAILURE_CALLBACKS = [on_failure, _failed]
BREAKER_SUCCESS_CALLBACKS = [on_success, _succeeded]


def resume_warehouse(context=None, **_kwargs):
    try:
        dag_run = (context or {}).get("dag_run")
        dag_id = getattr(dag_run, "dag_id", None) or (dag_run or {}).get("dag_id", "unknown")
        run_id = getattr(dag_run, "run_id", None) or (dag_run or {}).get("run_id", "")
        log.warning("ALTER WAREHOUSE WH_LOAD RESUME IF SUSPENDED  -- requested by deadline callback")
        notify(
            Event(
                kind="deadline_missed",
                dag_id=str(dag_id),
                task_id="load_warehouse",
                run_id=str(run_id),
                error="WH_LOAD resume requested",
            )
        )
    except Exception:  # noqa: BLE001
        log.exception("resume_warehouse failed")
