from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def bag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS))


def task(bag, dag_id, task_id):
    return bag.dags[dag_id].get_task(task_id)


def test_dags_import_without_errors(bag):
    assert bag.import_errors == {}
    assert {"traditional_healing", "llm_healing", "hybrid_healing"} <= set(bag.dags)


@pytest.mark.parametrize("dag_id", ["traditional_healing", "hybrid_healing"])
def test_cleanup_is_a_teardown_after_load(bag, dag_id):
    cleanup = task(bag, dag_id, "cleanup_staging_and_locks")
    assert cleanup.is_teardown
    assert cleanup.upstream_task_ids == {"load_warehouse"}


@pytest.mark.parametrize("dag_id", ["traditional_healing", "hybrid_healing"])
def test_dag_has_a_warehouse_deadline(bag, dag_id):
    assert [d.name for d in bag.dags[dag_id].deadline] == ["warehouse_stall"]


@pytest.mark.parametrize(
    "dag_id,task_id",
    [
        ("hybrid_healing", "heal_settlements_schema"),
        ("llm_healing", "heal_settlements_schema"),
    ],
)
def test_healing_tasks_only_run_after_a_failure(bag, dag_id, task_id):
    from airflow.sdk import TriggerRule

    assert task(bag, dag_id, task_id).trigger_rule == TriggerRule.ONE_FAILED


@pytest.mark.parametrize("dag_id", ["hybrid_healing", "llm_healing"])
def test_load_takes_the_resolved_settlements(bag, dag_id):
    assert "resolve_settlements" in task(bag, dag_id, "load_warehouse").upstream_task_ids


@pytest.mark.parametrize("dag_id", ["hybrid_healing", "llm_healing"])
def test_reparse_sits_between_the_heal_and_the_resolver(bag, dag_id):
    assert task(bag, dag_id, "reparse_settlements").upstream_task_ids == {"heal_settlements_schema"}
    assert "reparse_settlements" in task(bag, dag_id, "resolve_settlements").upstream_task_ids


def test_dangerous_tool_requires_approval():
    pytest.importorskip("airflow.providers.common.ai")
    from healing.tools import build_toolset

    toolset = build_toolset()
    gated = {name for name, t in toolset.tools.items() if t.requires_approval}
    assert gated == {"apply_schema_evolution_ddl"}


def test_hybrid_is_asset_scheduled(bag):
    assert type(bag.dags["hybrid_healing"].timetable).__name__ == "AssetTriggeredTimetable"


@pytest.mark.parametrize("dag_id", ["traditional_healing", "llm_healing", "hybrid_healing"])
def test_every_dag_ends_with_one_digest_that_runs_even_after_failures(bag, dag_id):
    from airflow.sdk import TriggerRule

    digest = task(bag, dag_id, "post_run_digest")
    assert digest.trigger_rule == TriggerRule.ALL_DONE and not digest.downstream_task_ids
    assert "post_run_digest" in task(bag, dag_id, "load_warehouse").get_flat_relative_ids(upstream=False)
