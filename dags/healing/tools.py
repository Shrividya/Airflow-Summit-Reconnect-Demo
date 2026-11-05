from __future__ import annotations

import logging
import re

from pydantic import BaseModel, Field

from . import scenario

log = logging.getLogger(__name__)

APPLIED_DDL_KEY = "applied_ddl"
QUARANTINED_KEY = "quarantined_rows"


def _record(key: str, value) -> None:
    try:
        from airflow.sdk import get_current_context

        get_current_context()["ti"].xcom_push(key=key, value=value)
    except Exception:  # noqa: BLE001
        log.debug("could not record %s", key, exc_info=True)


_DDL = re.compile(
    r"^ALTER TABLE settlements ADD COLUMN [a-z_][a-z0-9_]{0,62} "
    r"(BIGINT|INTEGER|NUMERIC\(\d{1,2},\d{1,2}\)|VARCHAR\(\d{1,4}\)|TIMESTAMP|DATE|BOOLEAN)$",
    re.IGNORECASE,
)


HEAL_SYSTEM_PROMPT = (
    "You are a careful data engineer. Make the smallest safe change and never a destructive one."
)


class SchemaHealResult(BaseModel):
    healed_rows: int = Field(ge=0, description="rows now loadable into the warehouse")
    action_taken: str
    ddl_applied: str | None = None


def validate_ddl(ddl: str) -> str:
    stmt = ddl.strip().rstrip(";").strip()
    if not _DDL.match(stmt):
        raise ValueError(
            "rejected: only a single `ALTER TABLE settlements ADD COLUMN <name> <type>` is allowed"
        )
    return stmt


def inspect_csv_header(path: str = "s3://partner-drop/settlements.csv") -> dict:
    columns = scenario.settlement_columns(drift=True)
    return {
        "path": path,
        "columns": columns,
        "expected_columns": scenario.EXPECTED_SETTLEMENT_COLUMNS,
        "missing": [c for c in scenario.EXPECTED_SETTLEMENT_COLUMNS if c not in columns],
        "unexpected": [c for c in columns if c not in scenario.EXPECTED_SETTLEMENT_COLUMNS],
        "data_rows": scenario.SETTLEMENT_ROWS,
        "malformed_rows": scenario.SETTLEMENT_MALFORMED_ROWS,
    }


def apply_schema_evolution_ddl(ddl: str) -> dict:
    try:
        stmt = validate_ddl(ddl)
    except ValueError as exc:
        return {"applied": False, "error": str(exc)}
    _record(APPLIED_DDL_KEY, stmt)
    return {"applied": True, "ddl": stmt}


def quarantine_bad_rows(reason: str) -> dict:
    _record(QUARANTINED_KEY, scenario.SETTLEMENT_MALFORMED_ROWS)
    return {
        "quarantined": scenario.SETTLEMENT_MALFORMED_ROWS,
        "loadable": scenario.SETTLEMENT_ROWS - scenario.SETTLEMENT_MALFORMED_ROWS,
        "reason": reason[:200],
    }


def build_toolset():
    from pydantic_ai import Tool
    from pydantic_ai.toolsets import FunctionToolset

    return FunctionToolset(
        [
            Tool(inspect_csv_header),
            Tool(apply_schema_evolution_ddl, requires_approval=True),
            Tool(quarantine_bad_rows),
        ],
        id="settlement_healing",
    )
