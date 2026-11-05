from datetime import datetime, timedelta

from airflow.sdk import Param, Variable, dag, get_current_context, task, teardown
from healing import scenario
from healing.alerts import notify
from healing.compat import AirflowFailException, task_checkpoint
from healing.deadlines import warehouse_stall_deadline
from healing.defaults import default_args
from healing.digest import post_run_digest
from healing.errors import SchemaDrift
from healing.gates import (
    BREAKER_FAILURE_CALLBACKS,
    BREAKER_SUCCESS_CALLBACKS,
    warehouse_breaker,
)
from healing.routing import Event
from healing.smart_retry import rules_policy

ABSORBED = {
    "refresh_fx_rates": ("resolve_fx_rates", "cached FX rates used"),
    "parse_settlements": ("triage_settlements", "settlements quarantined"),
}


@dag(
    dag_id="traditional_healing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    deadline=warehouse_stall_deadline(minutes=4),
    default_args=default_args(fast=True),
    params={
        "inject_schema_drift": Param(True, type="boolean"),
        "inject_warehouse_suspended": Param(True, type="boolean"),
    },
    tags=["self-healing", "traditional"],
)
def traditional_healing():
    @task.sensor(poke_interval=5, timeout=60, mode="reschedule")
    def wait_for_partner_files() -> bool:
        return True

    @task.short_circuit
    def breaker_allows() -> bool:
        return warehouse_breaker().allow()

    @task(retry_policy=rules_policy(), execution_timeout=timedelta(seconds=30))
    def fetch_payments() -> int:
        ctx = get_current_context()
        checkpoint = task_checkpoint(ctx)
        offset = int(checkpoint.get("last_batch_offset", 0))
        if offset:
            print(f"resuming from checkpoint: last_batch_offset={offset}")
        rows = scenario.fetch_payments(
            ctx["ti"].try_number,
            offset=offset,
            on_batch=lambda next_offset: checkpoint.set("last_batch_offset", next_offset),
        )
        checkpoint.set("last_batch_offset", 0)
        return rows

    @task(retry_policy=rules_policy(), execution_timeout=timedelta(seconds=30))
    def refresh_fx_rates() -> dict:
        return scenario.fetch_fx_rates()

    @task(trigger_rule="all_done")
    def resolve_fx_rates() -> dict:
        ctx = get_current_context()
        rates = ctx["ti"].xcom_pull(task_ids="refresh_fx_rates")
        if rates:
            return rates
        notify(
            Event(
                kind="fallback",
                dag_id=ctx["ti"].dag_id,
                task_id="refresh_fx_rates",
                run_id=ctx["run_id"],
                error="using cached FX rates",
            )
        )
        return scenario.CACHED_FX_RATES

    @task(execution_timeout=timedelta(seconds=30))
    def parse_settlements() -> int:
        drift = get_current_context()["params"]["inject_schema_drift"]
        try:
            return scenario.check_settlements(drift)
        except SchemaDrift as exc:
            raise AirflowFailException(str(exc)) from exc

    @task(trigger_rule="all_done")
    def triage_settlements() -> str:
        ctx = get_current_context()
        if ctx["ti"].xcom_pull(task_ids="parse_settlements") is not None:
            return "settlements ok"
        notify(
            Event(
                kind="quarantined",
                dag_id=ctx["ti"].dag_id,
                task_id="parse_settlements",
                run_id=ctx["run_id"],
                error="settlements.csv moved to quarantine, loading the rest",
            )
        )
        return "settlements quarantined"

    @task(
        execution_timeout=timedelta(minutes=2),
        on_failure_callback=BREAKER_FAILURE_CALLBACKS,
        on_success_callback=BREAKER_SUCCESS_CALLBACKS,
    )
    def load_warehouse(rows: int, rates: dict) -> None:
        ctx = get_current_context()
        outage = Variable.get("warehouse_outage", default="false") == "true"
        scenario.load_to_warehouse(ctx["ti"].try_number, ctx["params"]["inject_warehouse_suspended"], outage)
        print(f"upserted {rows} payments using {len(rates)} FX rates (overwrite by partition)")

    @teardown
    def cleanup_staging_and_locks() -> list[str]:
        ctx = get_current_context()
        released = scenario.cleanup_warehouse_artifacts(ctx["run_id"])
        print(f"released {released}")
        return released

    @task(task_id="post_run_digest", trigger_rule="all_done", retries=0)
    def post_run_digest_task(triage: str) -> None:
        post_run_digest(get_current_context(), absorbed=ABSORBED, notes={"parse_settlements": triage})

    gate = breaker_allows()
    wait_for_partner_files() >> gate
    payments = fetch_payments()
    fx = refresh_fx_rates()
    parsed = parse_settlements()
    rates = resolve_fx_rates()
    gate >> [payments, fx, parsed]
    fx >> rates
    triage = triage_settlements()
    parsed >> triage
    loaded = load_warehouse(payments, rates)
    cleanup = cleanup_staging_and_locks()
    loaded >> cleanup
    [loaded, cleanup] >> post_run_digest_task(triage)


traditional_healing()
