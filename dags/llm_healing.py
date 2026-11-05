from datetime import datetime, timedelta

from airflow.sdk import Param, TriggerRule, Variable, dag, get_current_context, task
from healing import scenario
from healing.approval import request_schema_approval
from healing.alerts import notify
from healing.defaults import default_args
from healing.digest import post_run_digest
from healing.routing import Event
from healing.smart_retry import rules_then_llm
from healing.tools import (
    APPLIED_DDL_KEY,
    HEAL_SYSTEM_PROMPT,
    QUARANTINED_KEY,
    SchemaHealResult,
    build_toolset,
)

ABSORBED = {
    "refresh_fx_rates": ("resolve_fx_rates", "last-known cached FX rates used"),
    "parse_settlements": ("resolve_settlements", "settlements quarantined or healed"),
}


@dag(
    dag_id="llm_healing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(fast=True),
    params={
        "inject_schema_drift": Param(True, type="boolean"),
        "inject_warehouse_suspended": Param(True, type="boolean"),
    },
    tags=["self-healing", "llm"],
)
def llm_healing():
    @task(retry_policy=rules_then_llm())
    def fetch_payments() -> int:
        ti = get_current_context()["ti"]
        return scenario.fetch_payments(ti.try_number)

    @task(retry_policy=rules_then_llm())
    def refresh_fx_rates() -> dict:
        return scenario.validate_fx_rates(scenario.fetch_fx_rates())

    @task(trigger_rule="all_done")
    def resolve_fx_rates() -> dict:
        ctx = get_current_context()
        ti = ctx["ti"]
        rates = ti.xcom_pull(task_ids="refresh_fx_rates")
        if rates:
            return rates
        notify(
            Event(
                kind="fallback",
                dag_id=ti.dag_id,
                task_id="refresh_fx_rates",
                run_id=ctx["run_id"],
                error=f"using last-known cached FX rates: {scenario.CACHED_FX_RATES}",
            )
        )
        return scenario.CACHED_FX_RATES

    @task(retry_policy=rules_then_llm())
    def parse_settlements() -> int:
        drift = get_current_context()["params"]["inject_schema_drift"]
        return scenario.check_settlements(drift)

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
        ctx = get_current_context()
        ti = ctx["ti"]
        settlements = scenario.merge_settlements(
            ti.xcom_pull(task_ids="parse_settlements"),
            ti.xcom_pull(task_ids="heal_settlements_schema"),
            applied_ddl=ti.xcom_pull(task_ids="heal_settlements_schema", key=APPLIED_DDL_KEY),
            quarantined=ti.xcom_pull(task_ids="heal_settlements_schema", key=QUARANTINED_KEY),
            reparsed=ti.xcom_pull(task_ids="reparse_settlements"),
        )
        if settlements["source"] != "parsed":
            notify(
                Event(
                    kind="agent_healed" if settlements["source"] == "healed" else "quarantined",
                    dag_id=ti.dag_id,
                    task_id="parse_settlements",
                    run_id=ctx["run_id"],
                    error=settlements["action"],
                )
            )
        return settlements

    @task(retry_policy=rules_then_llm())
    def load_warehouse(rows: int, rates: dict, settlements: dict) -> str:
        ctx = get_current_context()
        outage = Variable.get("warehouse_outage", default="false") == "true"
        scenario.load_to_warehouse(ctx["ti"].try_number, ctx["params"]["inject_warehouse_suspended"], outage)
        return (
            f"upserted {rows} payments using {len(rates)} FX rates "
            f"and {settlements['rows']} settlements ({settlements['source']})"
        )

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
    def post_run_digest_task(text: str | None, settlements: dict | None) -> None:
        source = (settlements or {}).get("source", "quarantined")
        post_run_digest(
            get_current_context(),
            absorbed=ABSORBED,
            notes={"parse_settlements": f"settlements {source}"},
            summary=text or "",
        )

    payments = fetch_payments()
    fx = refresh_fx_rates()
    rates = resolve_fx_rates()
    fx >> rates
    parsed = parse_settlements()
    healed = heal_settlements_schema()
    parsed >> healed
    parsed >> request_schema_approval()
    reparsed = reparse_settlements()
    settled = resolve_settlements()
    healed >> reparsed >> settled
    loaded = load_warehouse(payments, rates, settled)
    summary = summarize_run()
    loaded >> summary
    post_run_digest_task(summary, settled)


llm_healing()
