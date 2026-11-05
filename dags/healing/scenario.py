from __future__ import annotations

import math
import re
from collections.abc import Callable

from .errors import MalformedPayload, RateLimited, SchemaDrift, Unauthorized, WarehouseSuspended

CACHED_FX_RATES = {"EUR": 1.08, "GBP": 1.27}

PAYMENT_ROWS = 1204
PAYMENT_PAGE_SIZE = 400
RATE_LIMIT_AT_OFFSET = 800

SETTLEMENT_ROWS = 318
SETTLEMENT_MALFORMED_ROWS = 2
EXPECTED_SETTLEMENT_COLUMNS = ["settlement_id", "amount", "currency"]
DRIFTED_SETTLEMENT_COLUMNS = ["settlement_id", "amount_cents", "currency"]


def fetch_payments(try_number: int, offset: int = 0, on_batch: Callable[[int], None] | None = None) -> int:
    while offset < PAYMENT_ROWS:
        if try_number == 1 and offset >= RATE_LIMIT_AT_OFFSET:
            raise RateLimited(
                f"429 Too Many Requests after {offset} rows: partner API quota exceeded, slow down"
            )
        offset = min(offset + PAYMENT_PAGE_SIZE, PAYMENT_ROWS)
        if on_batch is not None:
            on_batch(offset)
    return PAYMENT_ROWS


def fetch_fx_rates() -> dict:
    raise Unauthorized("401 Unauthorized: API key for fx-rates was revoked")


def validate_fx_rates(rates, reference: dict | None = None, tolerance: float = 0.2) -> dict:
    if not isinstance(rates, dict) or not rates:
        raise MalformedPayload(f"FX payload must be a non-empty object, got {type(rates).__name__}")
    clean: dict[str, float] = {}
    for code, value in rates.items():
        if not isinstance(code, str) or isinstance(value, bool) or not isinstance(value, (int, float)):
            raise MalformedPayload(f"bad FX entry {code!r}: {value!r}")
        if not math.isfinite(value) or value <= 0:
            raise MalformedPayload(f"FX rate for {code} must be positive and finite, got {value!r}")
        clean[code] = float(value)
    for code, ref in (reference or {}).items():
        if code in clean and abs(clean[code] - ref) / ref > tolerance:
            raise MalformedPayload(
                f"FX rate for {code} ({clean[code]}) is more than {tolerance:.0%} away from {ref}"
            )
    return clean


def settlement_columns(drift: bool) -> list[str]:
    return DRIFTED_SETTLEMENT_COLUMNS if drift else EXPECTED_SETTLEMENT_COLUMNS


def check_settlements(drift: bool) -> int:
    if drift:
        raise SchemaDrift("Schema drift in settlements.csv: expected column 'amount', found 'amount_cents'")
    return SETTLEMENT_ROWS


def added_column(ddl: str | None) -> str | None:
    match = re.search(r"ADD COLUMN\s+([a-z_][a-z0-9_]*)", ddl or "", re.IGNORECASE)
    return match.group(1).lower() if match else None


def reparse_settlements(drift: bool, applied_ddl: str | None, quarantined: int | None = None) -> int:
    table = set(EXPECTED_SETTLEMENT_COLUMNS) | ({added_column(applied_ddl)} - {None})
    unknown = [c for c in settlement_columns(drift) if c not in table]
    if unknown:
        raise SchemaDrift(f"settlements.csv still has columns the table does not know: {unknown}")
    return SETTLEMENT_ROWS - int(quarantined or 0)


def merge_settlements(
    parsed: int | None,
    healed: dict | None,
    applied_ddl: str | None = None,
    quarantined: int | None = None,
    reparsed: int | None = None,
) -> dict:
    if parsed is not None:
        return {"rows": parsed, "source": "parsed", "action": ""}
    if applied_ddl and reparsed is not None:
        action = f"agent ran `{applied_ddl}`, reparsed settlements.csv" + (
            f" and quarantined {quarantined} rows" if quarantined else ""
        )
        return {"rows": reparsed, "source": "healed", "action": action}
    if applied_ddl:
        note = f"agent ran `{applied_ddl}` but the reparse did not confirm it; "
        return {"rows": 0, "source": "quarantined", "action": note + "settlements.csv moved to quarantine"}
    if healed and healed.get("ddl_applied"):
        note = "agent reported a schema change that no tool ran, so it was not trusted; "
        return {"rows": 0, "source": "quarantined", "action": note + "settlements.csv moved to quarantine"}
    return {
        "rows": 0,
        "source": "quarantined",
        "action": "settlements.csv moved to quarantine, loading the rest",
    }


def cleanup_warehouse_artifacts(run_id: str) -> list[str]:
    token = "".join(c if c.isalnum() else "_" for c in run_id)
    return [f"staging.payments_{token}", f"staging.settlements_{token}", "lock:WH_LOAD"]


def load_to_warehouse(try_number: int, suspended: bool, outage: bool) -> None:
    if outage:
        raise ConnectionError("warehouse returned 503 Service Unavailable")
    if suspended and try_number == 1:
        raise WarehouseSuspended("Warehouse WH_LOAD is suspended: resume it or wait for auto-resume")


def outcomes(ti, task_ids: list[str]) -> dict:
    pulled = ti.xcom_pull(task_ids=task_ids)
    return {
        t: ("ok" if v is not None else "failed or skipped") for t, v in zip(task_ids, pulled, strict=False)
    }


def heal_prompt() -> str:
    return (
        "parse_settlements failed on settlements.csv because of schema drift. Decide how to heal it.\n"
        f"The warehouse table `settlements` expects columns {EXPECTED_SETTLEMENT_COLUMNS}.\n"
        "1. Call inspect_csv_header to see what actually arrived.\n"
        "2. If the file only adds or renames a column, call apply_schema_evolution_ddl with ONE additive "
        "statement (ALTER TABLE settlements ADD COLUMN <name> <type>). Never rename or drop columns: "
        "a renamed column can change units, and that must be a human decision.\n"
        "3. Call quarantine_bad_rows for rows that still cannot be loaded.\n"
        "4. Report healed_rows (rows now loadable), action_taken (one sentence) and ddl_applied "
        "(the statement, or null if none ran or it was rejected)."
    )


def summary_prompt(results: dict) -> str:
    lines = "\n".join(f"- {task}: {state}" for task, state in results.items())
    return (
        "Summarize this payments pipeline run in two plain sentences for an on-call "
        "engineer reading Slack. Say what finished, what did not, and what to check first.\n\n" + lines
    )
