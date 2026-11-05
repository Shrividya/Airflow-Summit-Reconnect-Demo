from __future__ import annotations

import enum
import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass

log = logging.getLogger(__name__)


class Severity(enum.IntEnum):
    INFO = 1
    WARNING = 2
    CRITICAL = 3


@dataclass(frozen=True)
class DigestItem:
    section: str
    task_id: str
    text: str
    url: str = ""


@dataclass(frozen=True)
class Event:
    kind: str
    dag_id: str
    task_id: str = ""
    run_id: str = ""
    attempt: int = 1
    max_attempts: int = 1
    error: str = ""
    url: str = ""
    items: tuple[DigestItem, ...] = ()
    level: Severity | None = None

    def to_json(self, severity: Severity | None = None) -> str:
        sev = severity if severity is not None else classify(self)
        return json.dumps({**asdict(self), "severity": sev.name}, sort_keys=True)


_KIND_SEVERITY = {
    "retry": Severity.INFO,
    "recovered": Severity.WARNING,
    "exhausted": Severity.CRITICAL,
    "fallback": Severity.WARNING,
    "quarantined": Severity.WARNING,
    "summary": Severity.INFO,
    "breaker_opened": Severity.WARNING,
    "breaker_closed": Severity.INFO,
    "agent_healed": Severity.WARNING,
    "approval_needed": Severity.WARNING,
    "deadline_missed": Severity.WARNING,
    "digest": Severity.INFO,
}

_KIND_TEXT = {
    "retry": "failed, retry scheduled",
    "recovered": "healed after retries",
    "exhausted": "failed, retries exhausted",
    "fallback": "took the fallback path",
    "quarantined": "bad input quarantined",
    "summary": "run summary",
    "breaker_opened": "circuit breaker OPEN, calls paused",
    "breaker_closed": "circuit breaker closed, calls resumed",
    "agent_healed": "agent repaired the schema",
    "approval_needed": "agent proposes a schema change and is waiting for human approval",
    "deadline_missed": "run deadline missed, warehouse auto-resume requested",
    "digest": "run digest",
}

_ROUTES = {
    Severity.INFO: ("log",),
    Severity.WARNING: ("log", "slack"),
    Severity.CRITICAL: ("log", "slack"),
}

QUIET_KINDS = frozenset(
    {
        "retry",
        "recovered",
        "exhausted",
        "fallback",
        "quarantined",
        "agent_healed",
        "summary",
        "breaker_closed",
    }
)

Transport = Callable[[Event, Severity], None]


def classify(event: Event) -> Severity:
    if event.level is not None:
        return event.level
    return _KIND_SEVERITY.get(event.kind, Severity.WARNING)


def route(severity: Severity, kind: str = "") -> tuple[str, ...]:
    if kind in QUIET_KINDS:
        return ("log",)
    return _ROUTES[severity]


def format_message(event: Event, severity: Severity | None = None) -> str:
    severity = severity if severity is not None else classify(event)
    if event.kind == "digest":
        return format_digest(event, severity)
    where = f"{event.dag_id}.{event.task_id}" if event.task_id else event.dag_id
    text = _KIND_TEXT.get(event.kind, event.kind)
    msg = f"[{severity.name}] {where} {text}"
    if event.max_attempts > 1 or event.attempt > 1:
        msg += f" (attempt {event.attempt}/{event.max_attempts})"
    if event.error:
        msg += f": {event.error[:200]}"
    if event.url:
        msg += f" {event.url}"
    return msg


_SECTIONS = (
    ("attention", "Needs attention"),
    ("handled", "Handled by a fallback"),
    ("recovered", "Healed on retry"),
)


def format_digest(event: Event, severity: Severity, link=lambda text, url: f"{text} {url}") -> str:
    lines = [f"[{severity.name}] {event.dag_id} run {event.run_id}: "]
    if not event.items:
        lines[0] += "clean, nothing needed healing"
    else:
        lines[0] += {
            Severity.CRITICAL: "needs attention",
            Severity.WARNING: "finished with self-healing",
        }.get(severity, "finished")
    for key, title in _SECTIONS:
        group = [i for i in event.items if i.section == key]
        if group:
            lines.append(f"{title}:")
            lines.extend(
                f"  - {i.task_id}: {i.text}" + (f" ({link('open task', i.url)})" if i.url else "")
                for i in group
            )
    if event.error:
        lines.append(f"Summary: {event.error[:600]}")
    return "\n".join(lines)


def dispatch(event: Event, transports: Mapping[str, Transport]) -> dict[str, bool]:
    severity = classify(event)
    results: dict[str, bool] = {}
    for channel in route(severity, event.kind):
        transport = transports.get(channel)
        if transport is None:
            results[channel] = False
            continue
        try:
            transport(event, severity)
            results[channel] = True
        except Exception:  # noqa: BLE001
            log.exception("alert transport %r failed", channel)
            results[channel] = False
    return results
