from __future__ import annotations

from datetime import timedelta


def warehouse_stall_deadline(minutes: int = 4):
    from airflow.sdk import DeadlineAlert, DeadlineReference
    from airflow.sdk.definitions.callback import SyncCallback

    return DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=minutes),
        callback=SyncCallback("healing.gates.resume_warehouse"),
        name="warehouse_stall",
    )
