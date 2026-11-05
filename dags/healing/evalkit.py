from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field

STOP = "stop"
RETRY = "retry"
DISPUTED = "disputed"


@dataclass(frozen=True)
class Case:
    id: str
    exception: str
    message: str
    label: str
    note: str = ""


@dataclass
class Outcome:
    case: Case
    decisions: list[str] = field(default_factory=list)


def to_decision(action: str) -> str:
    return STOP if action == "fail" else RETRY


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def majority(decisions: list[str]) -> str:
    counts = Counter(decisions)
    top = counts.most_common()
    if len(top) > 1 and top[0][1] == top[1][1]:
        return RETRY
    return top[0][0]


def score(outcomes: list[Outcome]) -> dict:
    scored = [o for o in outcomes if o.case.label in (STOP, RETRY) and o.decisions]
    calls = [(o.case.label, d) for o in scored for d in o.decisions]
    stops = [(label, d) for label, d in calls if d == STOP]
    wrong_stops = sum(1 for label, _ in stops if label == RETRY)
    recoverable = [(label, d) for label, d in calls if label == RETRY]
    blocked = sum(1 for _, d in recoverable if d == STOP)
    permanent = [(label, d) for label, d in calls if label == STOP]
    missed = sum(1 for _, d in permanent if d == RETRY)
    unstable = [o.case.id for o in scored if len(set(o.decisions)) > 1]
    return {
        "calls": len(calls),
        "stops": len(stops),
        "wrong_stops": wrong_stops,
        "wrong_stop_share_of_stops": (wrong_stops / len(stops)) if stops else None,
        "wrong_stop_interval": wilson(wrong_stops, len(stops)),
        "recoverable_calls": len(recoverable),
        "recoverable_blocked": blocked,
        "blocked_share_of_recoverable": (blocked / len(recoverable)) if recoverable else None,
        "blocked_interval": wilson(blocked, len(recoverable)),
        "permanent_calls": len(permanent),
        "missed_stops": missed,
        "missed_share_of_permanent": (missed / len(permanent)) if permanent else None,
        "unstable_cases": unstable,
        "repeatable_share": (1 - len(unstable) / len(scored)) if scored else None,
    }


def wrong_stops_on(outcomes: list[Outcome]) -> list[str]:
    return [
        o.case.id for o in outcomes if o.case.label == RETRY and o.decisions and majority(o.decisions) == STOP
    ]


def format_table(results: dict[str, dict]) -> str:
    def pct(x):
        return "n/a" if x is None else f"{x:.0%}"

    def span(i):
        return f"{i[0]:.0%}-{i[1]:.0%}"

    head = (
        f"{'policy':<22}{'stops':>6}{'wrong':>7}{'wrong/stops':>13}{'95% CI':>11}"
        f"{'blocked':>9}{'missed':>8}{'repeatable':>12}"
    )
    rows = [head, "-" * len(head)]
    for name, r in results.items():
        rows.append(
            f"{name:<22}{r['stops']:>6}{r['wrong_stops']:>7}{pct(r['wrong_stop_share_of_stops']):>13}"
            f"{span(r['wrong_stop_interval']):>11}{pct(r['blocked_share_of_recoverable']):>9}"
            f"{pct(r['missed_share_of_permanent']):>8}{pct(r['repeatable_share']):>12}"
        )
    return "\n".join(rows)
