from datetime import datetime, timedelta

from airflow.sdk import Param, TriggerRule, Variable, dag, get_current_context, task, teardown
from healing import scenario
from healing.approval import request_schema_approval
from healing.alerts import notify
from healing.arrival import partner_drop_asset
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
from healing.smart_retry import rules_policy, rules_then_llm
from healing.tools import (
    APPLIED_DDL_KEY,
    HEAL_SYSTEM_PROMPT,
    QUARANTINED_KEY,
    SchemaHealResult,
    build_toolset,
)

partner_file = partner_drop_asset()

ABSORBED = {
    "refresh_fx_rates": ("resolve_fx_rates", "cached FX rates used"),
    "parse_settlements": ("triage_settlements", "settlements quarantined"),
}


@dag(
    dag_id="hybrid_healing",
    schedule=[partner_file],
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
    tags=["self-healing", "hybrid"],
)
def hybrid_healing():
    @task.sensor(poke_interval=5, timeout=60, mode="reschedule")
    def wait_for_partner_files() -> bool:
        if get_current_context().get("triggering_asset_events"):
            print("triggered by the partner_file asset event: no polling needed")
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

    @task(
        retry_policy=rules_then_llm(),
        execution_timeout=timedelta(seconds=30),
    )
    def parse_settlements() -> int:
        drift = get_current_context()["params"]["inject_schema_drift"]
        try:
            return scenario.check_settlements(drift)
        except SchemaDrift as exc:
            raise AirflowFailException(str(exc)) from exc

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=HEAL_SYSTEM_PROMPT,
        toolsets=[build_toolset()],
        output_type=SchemaHealResult,
        serialize_output=True,
        tool_approval_timeout=timedelta(minutes=5),
        on_tool_approval_timeout="deny",
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
        execution_timeout=timedelta(minutes=10),
    )
    def heal_settlements_schema() -> str:
        return scenario.heal_prompt()

    @task(trigger_rule="all_done")
    def reparse_settlements() -> int | None:
        ctx = get_current_context()
        ti = ctx["ti"]
        applied_ddl = ti.xcom_pull(task_ids="heal_settlements_schema", key=APPLIED_DDL_KEY)
        if not applied_ddl:
            return None
        return scenario.reparse_settlements(
            ctx["params"]["inject_schema_drift"],
            applied_ddl,
            ti.xcom_pull(task_ids="heal_settlements_schema", key=QUARANTINED_KEY),
        )

    @task(trigger_rule="all_done")
    def resolve_settlements() -> dict:
        ti = get_current_context()["ti"]
        return scenario.merge_settlements(
            ti.xcom_pull(task_ids="parse_settlements"),
            ti.xcom_pull(task_ids="heal_settlements_schema"),
            applied_ddl=ti.xcom_pull(task_ids="heal_settlements_schema", key=APPLIED_DDL_KEY),
            quarantined=ti.xcom_pull(task_ids="heal_settlements_schema", key=QUARANTINED_KEY),
            reparsed=ti.xcom_pull(task_ids="reparse_settlements"),
        )

    @task(trigger_rule="all_done")
    def triage_settlements(settlements: dict) -> str:
        ctx = get_current_context()
        source = settlements["source"]
        if source == "parsed":
            return "settlements ok"
        notify(
            Event(
                kind="agent_healed" if source == "healed" else "quarantined",
                dag_id=ctx["ti"].dag_id,
                task_id="parse_settlements",
                run_id=ctx["run_id"],
                error=settlements["action"],
            )
        )
        return f"settlements {source}"

    @task(
        retry_policy=rules_then_llm(),
        execution_timeout=timedelta(minutes=2),
        on_failure_callback=BREAKER_FAILURE_CALLBACKS,
        on_success_callback=BREAKER_SUCCESS_CALLBACKS,
    )
    def load_warehouse(rows: int, rates: dict, settlements: dict) -> str:
        ctx = get_current_context()
        outage = Variable.get("warehouse_outage", default="false") == "true"
        scenario.load_to_warehouse(ctx["ti"].try_number, ctx["params"]["inject_warehouse_suspended"], outage)
        return (
            f"upserted {rows} payments using {len(rates)} FX rates "
            f"and {settlements['rows']} settlements ({settlements['source']})"
        )

    @teardown
    def cleanup_staging_and_locks() -> list[str]:
        ctx = get_current_context()
        released = scenario.cleanup_warehouse_artifacts(ctx["run_id"])
        print(f"released {released}")
        return released

    @task.llm(
        llm_conn_id="pydanticai_default",
        system_prompt="You write short, calm incident summaries for engineers.",
        trigger_rule="all_done",
        retries=0,
    )
    def summarize_run() -> str:
        ti = get_current_context()["ti"]
        results = scenario.outcomes(
            ti,
            [
                "fetch_payments",
                "refresh_fx_rates",
                "parse_settlements",
                "resolve_settlements",
                "load_warehouse",
            ],
        )
        return scenario.summary_prompt(results)

    @task(task_id="post_run_digest", trigger_rule="all_done", retries=0)
    def post_run_digest_task(text: str | None, triage: str | None) -> None:
        post_run_digest(
            get_current_context(),
            absorbed=ABSORBED,
            notes={"parse_settlements": triage or ABSORBED["parse_settlements"][1]},
            summary=text or "",
        )

    gate = breaker_allows()
    wait_for_partner_files() >> gate
    payments = fetch_payments()
    fx = refresh_fx_rates()
    parsed = parse_settlements()
    rates = resolve_fx_rates()
    gate >> [payments, fx, parsed]
    fx >> rates
    healed = heal_settlements_schema()
    reparsed = reparse_settlements()
    settled = resolve_settlements()
    parsed >> request_schema_approval()
    parsed >> healed >> reparsed >> settled
    triage = triage_settlements(settled)
    loaded = load_warehouse(payments, rates, settled)
    cleanup = cleanup_staging_and_locks()
    loaded >> cleanup
    summary = summarize_run()
    [loaded, triage] >> summary
    [summary, triage, cleanup] >> post_run_digest_task(summary, triage)


hybrid_healing()
