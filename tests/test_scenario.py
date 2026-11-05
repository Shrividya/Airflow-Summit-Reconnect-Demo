import pytest
from healing import scenario
from healing.errors import RateLimited, SchemaDrift, Unauthorized, WarehouseSuspended


def test_fetch_is_rate_limited_once_then_heals():
    with pytest.raises(RateLimited):
        scenario.fetch_payments(1)
    assert scenario.fetch_payments(2) == 1204


def test_fx_key_is_always_revoked():
    with pytest.raises(Unauthorized):
        scenario.fetch_fx_rates()


def test_schema_drift_is_toggleable():
    with pytest.raises(SchemaDrift):
        scenario.check_settlements(True)
    assert scenario.check_settlements(False) == 318


def test_warehouse_suspended_only_on_first_try():
    with pytest.raises(WarehouseSuspended):
        scenario.load_to_warehouse(1, suspended=True, outage=False)
    scenario.load_to_warehouse(2, suspended=True, outage=False)


def test_outage_never_heals_by_retrying():
    for attempt in (1, 2, 5):
        with pytest.raises(ConnectionError):
            scenario.load_to_warehouse(attempt, suspended=False, outage=True)


def test_summary_prompt_lists_every_task():
    p = scenario.summary_prompt({"a": "ok", "b": "failed or skipped"})
    assert "- a: ok" in p and "- b: failed or skipped" in p
