import math

import pytest
from healing import scenario
from healing.errors import MalformedPayload, RateLimited, SchemaDrift


def test_pagination_checkpoints_every_page_and_fails_mid_way_on_first_try():
    saved = []
    with pytest.raises(RateLimited):
        scenario.fetch_payments(1, on_batch=saved.append)
    assert saved == [400, 800]


def test_retry_resumes_from_checkpoint_and_reads_only_the_rest():
    saved = []
    assert scenario.fetch_payments(2, offset=800, on_batch=saved.append) == scenario.PAYMENT_ROWS
    assert saved == [1200, 1204]


def test_without_a_checkpoint_a_retry_still_completes():
    assert scenario.fetch_payments(2) == scenario.PAYMENT_ROWS


def test_fx_validation_accepts_clean_rates_as_floats():
    assert scenario.validate_fx_rates({"EUR": 1, "GBP": 1.3}) == {"EUR": 1.0, "GBP": 1.3}


@pytest.mark.parametrize(
    "bad",
    [
        None,
        {},
        [],
        {"EUR": "1.1"},
        {"EUR": True},
        {"EUR": -1.0},
        {"EUR": 0},
        {"EUR": math.nan},
        {"EUR": math.inf},
    ],
)
def test_fx_validation_rejects_malformed_payloads(bad):
    with pytest.raises(MalformedPayload):
        scenario.validate_fx_rates(bad)


def test_model_rates_far_from_last_known_are_rejected():
    ref = scenario.CACHED_FX_RATES
    assert scenario.validate_fx_rates({"EUR": 1.10, "GBP": 1.25}, reference=ref)
    with pytest.raises(MalformedPayload):
        scenario.validate_fx_rates({"EUR": 2.5, "GBP": 1.25}, reference=ref)


def test_merge_prefers_parsed_then_healed_then_quarantine():
    assert scenario.merge_settlements(318, None)["source"] == "parsed"
    healed = scenario.merge_settlements(
        None, None, applied_ddl="ALTER TABLE settlements ADD COLUMN a INTEGER", quarantined=2, reparsed=316
    )
    assert (healed["source"], healed["rows"]) == ("healed", 316)
    assert (
        "ALTER TABLE settlements ADD COLUMN a INTEGER" in healed["action"]
        and "quarantined 2" in healed["action"]
    )
    assert scenario.merge_settlements(None, None)["source"] == "quarantined"


def test_healed_comes_from_the_tool_record_not_the_agents_answer():
    claim = {"healed_rows": 316, "action_taken": "added column", "ddl_applied": "ALTER TABLE ..."}
    result = scenario.merge_settlements(None, claim)
    assert result["source"] == "quarantined" and result["rows"] == 0
    assert "not trusted" in result["action"]


def test_the_agents_row_count_is_ignored_when_a_ddl_really_ran():
    claim = {"healed_rows": 99999, "action_taken": "x", "ddl_applied": "y"}
    result = scenario.merge_settlements(
        None, claim, applied_ddl="ALTER TABLE settlements ADD COLUMN a INTEGER", reparsed=scenario.SETTLEMENT_ROWS
    )
    assert result["rows"] == scenario.SETTLEMENT_ROWS


@pytest.mark.parametrize(
    "claim",
    [
        {"healed_rows": 0, "action_taken": "quarantined rows", "ddl_applied": None},
        {"healed_rows": 316, "action_taken": "says it healed", "ddl_applied": None},
    ],
)
def test_an_agent_that_ran_no_ddl_never_counts_as_healed(claim):
    result = scenario.merge_settlements(None, claim, quarantined=2)
    assert result["source"] == "quarantined" and result["rows"] == 0


def test_a_ddl_without_a_confirming_reparse_is_not_healed():
    result = scenario.merge_settlements(
        None, None, applied_ddl="ALTER TABLE settlements ADD COLUMN a INTEGER", quarantined=2
    )
    assert result["source"] == "quarantined" and result["rows"] == 0
    assert "reparse did not confirm" in result["action"]


def test_reparse_succeeds_once_the_drifted_column_exists():
    ddl = "ALTER TABLE settlements ADD COLUMN amount_cents BIGINT"
    assert scenario.reparse_settlements(True, ddl) == scenario.SETTLEMENT_ROWS
    assert scenario.reparse_settlements(True, ddl, quarantined=2) == scenario.SETTLEMENT_ROWS - 2


def test_reparse_still_fails_if_the_ddl_did_not_cover_the_drift():
    with pytest.raises(SchemaDrift):
        scenario.reparse_settlements(True, "ALTER TABLE settlements ADD COLUMN something_else BIGINT")
    with pytest.raises(SchemaDrift):
        scenario.reparse_settlements(True, None)


def test_cleanup_is_idempotent_and_names_are_sanitised():
    first = scenario.cleanup_warehouse_artifacts("manual__2026-10-31T18:03:36+00:00")
    assert first == scenario.cleanup_warehouse_artifacts("manual__2026-10-31T18:03:36+00:00")
    assert all(":" not in n or n.startswith("lock:") for n in first)
