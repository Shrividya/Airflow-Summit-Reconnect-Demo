from __future__ import annotations

from datetime import timedelta

from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import TriggerRule
from healing.alerts import notify
from healing.links import task_url
from healing.routing import Event

DRIFT_MESSAGE = (
    "settlements.csv drifted (expected 'amount', found 'amount_cents'). Approve or deny the proposed DDL "
    "within 5 minutes; on timeout it is denied and rows are quarantined."
)


def _notify_slack(context) -> None:
    ti = context["ti"]
    notify(
        Event(
            kind="approval_needed",
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=context["run_id"],
            error=DRIFT_MESSAGE,
            url=task_url(ti.dag_id, context["run_id"], ti.task_id),
        )
    )


def request_schema_approval() -> ApprovalOperator:
    return ApprovalOperator(
        task_id="request_schema_approval",
        subject="Approve schema DDL for settlements?",
        body=DRIFT_MESSAGE,
        execution_timeout=timedelta(minutes=5),
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
        on_execute_callback=_notify_slack,
    )
