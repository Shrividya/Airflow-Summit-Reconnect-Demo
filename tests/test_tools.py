import pytest
from healing import scenario, tools


@pytest.mark.parametrize(
    "ddl",
    [
        "ALTER TABLE settlements ADD COLUMN amount_cents BIGINT",
        "alter table settlements add column amount_cents bigint;",
        "ALTER TABLE settlements ADD COLUMN note VARCHAR(255)",
        "ALTER TABLE settlements ADD COLUMN fee NUMERIC(12,2)",
    ],
)
def test_additive_ddl_is_accepted(ddl):
    assert tools.validate_ddl(ddl).upper().startswith("ALTER TABLE SETTLEMENTS ADD COLUMN")


@pytest.mark.parametrize(
    "ddl",
    [
        "ALTER TABLE settlements RENAME COLUMN amount_cents TO amount",
        "ALTER TABLE settlements DROP COLUMN amount",
        "DROP TABLE settlements",
        "ALTER TABLE payments ADD COLUMN x BIGINT",
        "ALTER TABLE settlements ADD COLUMN x BIGINT; DROP TABLE settlements",
        "ALTER TABLE settlements ADD COLUMN x BIGINT -- sneaky",
        "",
    ],
)
def test_anything_else_is_rejected(ddl):
    with pytest.raises(ValueError):
        tools.validate_ddl(ddl)


def test_ddl_tool_reports_rejection_instead_of_raising():
    out = tools.apply_schema_evolution_ddl("DROP TABLE settlements")
    assert out["applied"] is False and "rejected" in out["error"]
    assert tools.apply_schema_evolution_ddl("ALTER TABLE settlements ADD COLUMN a INTEGER")["applied"] is True


def test_inspect_reports_the_drift():
    out = tools.inspect_csv_header()
    assert out["missing"] == ["amount"] and out["unexpected"] == ["amount_cents"]


def test_quarantine_counts_add_up():
    out = tools.quarantine_bad_rows("x" * 500)
    assert out["quarantined"] + out["loadable"] == 318 and len(out["reason"]) == 200


def test_heal_result_rejects_negative_rows():
    with pytest.raises(ValueError):
        tools.SchemaHealResult(healed_rows=-1, action_taken="x")
    assert tools.SchemaHealResult(healed_rows=1, action_taken="x").ddl_applied is None


def test_tools_record_what_they_really_did(monkeypatch):
    recorded = {}
    monkeypatch.setattr(tools, "_record", lambda key, value: recorded.__setitem__(key, value))
    tools.apply_schema_evolution_ddl("DROP TABLE settlements")
    assert recorded == {}
    tools.apply_schema_evolution_ddl("ALTER TABLE settlements ADD COLUMN a INTEGER")
    tools.quarantine_bad_rows("bad")
    assert recorded == {
        tools.APPLIED_DDL_KEY: "ALTER TABLE settlements ADD COLUMN a INTEGER",
        tools.QUARANTINED_KEY: 2,
    }


def test_a_model_returning_nothing_is_rejected_not_defaulted():
    from healing.errors import MalformedPayload

    with pytest.raises(MalformedPayload):
        scenario.validate_fx_rates({}, reference=scenario.CACHED_FX_RATES)
