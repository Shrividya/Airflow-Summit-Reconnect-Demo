import json
from pathlib import Path

import pytest
from healing import evalkit
from healing.evalkit import DISPUTED, RETRY, STOP, Case, Outcome


def outcome(label, decisions, id="c"):
    return Outcome(Case(id, "builtins.ValueError", "m", label), list(decisions))


def test_wilson_interval_brackets_the_rate_and_widens_for_small_samples():
    lo, hi = evalkit.wilson(9, 135)
    assert lo < 9 / 135 < hi and 0.03 < lo < 0.05 and 0.10 < hi < 0.13
    small = evalkit.wilson(1, 3)
    assert small[1] - small[0] > hi - lo
    assert evalkit.wilson(0, 0) == (0.0, 1.0)


def test_wrong_stop_is_a_stop_on_a_recoverable_error():
    r = evalkit.score([outcome(RETRY, [STOP]), outcome(STOP, [STOP]), outcome(RETRY, [RETRY])])
    assert (r["stops"], r["wrong_stops"]) == (2, 1)
    assert r["wrong_stop_share_of_stops"] == 0.5
    assert r["blocked_share_of_recoverable"] == 0.5


def test_the_two_denominators_differ():
    outcomes = [outcome(RETRY, [STOP], "a")] + [outcome(RETRY, [RETRY], f"r{i}") for i in range(9)]
    outcomes += [outcome(STOP, [STOP], f"s{i}") for i in range(9)]
    r = evalkit.score(outcomes)
    assert r["wrong_stop_share_of_stops"] == pytest.approx(1 / 10)
    assert r["blocked_share_of_recoverable"] == pytest.approx(1 / 10)
    assert r["missed_share_of_permanent"] == 0


def test_a_missed_stop_is_counted_separately_and_is_not_a_wrong_stop():
    r = evalkit.score([outcome(STOP, [RETRY, RETRY])])
    assert r["wrong_stops"] == 0 and r["missed_stops"] == 2 and r["stops"] == 0
    assert r["wrong_stop_share_of_stops"] is None


def test_disputed_cases_are_ignored():
    r = evalkit.score([outcome(DISPUTED, [STOP, STOP]), outcome(RETRY, [RETRY])])
    assert r["calls"] == 1 and r["stops"] == 0


def test_repeatability_flags_cases_whose_decision_flips_between_calls():
    r = evalkit.score([outcome(RETRY, [STOP, RETRY, RETRY], "flaky"), outcome(RETRY, [RETRY] * 3, "steady")])
    assert r["unstable_cases"] == ["flaky"] and r["repeatable_share"] == 0.5


def test_majority_never_turns_a_tie_into_a_stop():
    assert evalkit.majority([STOP, RETRY]) == RETRY
    assert evalkit.majority([STOP, STOP, RETRY]) == STOP
    assert evalkit.wrong_stops_on([outcome(RETRY, [STOP, STOP, RETRY], "x")]) == ["x"]


def test_default_and_retry_actions_both_count_as_retrying():
    assert evalkit.to_decision("fail") == STOP
    assert evalkit.to_decision("retry") == RETRY and evalkit.to_decision("default") == RETRY


def test_table_renders_every_policy():
    table = evalkit.format_table({"rules": evalkit.score([outcome(RETRY, [RETRY])])})
    assert "rules" in table and "wrong/stops" in table


def test_shipped_cases_are_well_formed():
    raw = json.loads((Path(__file__).parents[1] / "dags/healing/evals/cases.json").read_text())
    cases = [Case(**r) for r in raw]
    assert len({c.id for c in cases}) == len(cases)
    assert {c.label for c in cases} <= {STOP, RETRY, DISPUTED}
    assert sum(c.label == RETRY for c in cases) >= 6 and sum(c.label == STOP for c in cases) >= 6
