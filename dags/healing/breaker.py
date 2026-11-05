from __future__ import annotations

import time
from collections.abc import Callable
from typing import Protocol

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"


class Store(Protocol):
    def load(self) -> dict | None: ...
    def save(self, state: dict) -> None: ...


class MemoryStore:
    def __init__(self) -> None:
        self._data: dict | None = None

    def load(self) -> dict | None:
        return dict(self._data) if self._data else None

    def save(self, state: dict) -> None:
        self._data = dict(state)


class CircuitBreaker:
    def __init__(
        self,
        store: Store,
        failure_threshold: int = 3,
        cooldown_seconds: float = 90.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.store = store
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.clock = clock

    def _load(self) -> dict:
        return self.store.load() or {
            "state": CLOSED,
            "failures": 0,
            "opened_at": 0.0,
            "probe_at": 0.0,
        }

    @property
    def state(self) -> str:
        return self._load()["state"]

    def allow(self) -> bool:
        s = self._load()
        now = self.clock()
        if s["state"] == CLOSED:
            return True
        if s["state"] == OPEN:
            if now - s["opened_at"] >= self.cooldown_seconds:
                s.update(state=HALF_OPEN, probe_at=now)
                self.store.save(s)
                return True
            return False
        if now - s["probe_at"] >= self.cooldown_seconds:
            s["probe_at"] = now
            self.store.save(s)
            return True
        return False

    def record_success(self) -> str | None:
        s = self._load()
        was = s["state"]
        s.update(state=CLOSED, failures=0, opened_at=0.0, probe_at=0.0)
        self.store.save(s)
        return "closed" if was != CLOSED else None

    def record_failure(self) -> str | None:
        s = self._load()
        now = self.clock()
        if s["state"] == HALF_OPEN:
            s.update(state=OPEN, opened_at=now, probe_at=0.0)
            self.store.save(s)
            return "reopened"
        if s["state"] == OPEN:
            return None
        s["failures"] += 1
        transition = None
        if s["failures"] >= self.failure_threshold:
            s.update(state=OPEN, opened_at=now)
            transition = "opened"
        self.store.save(s)
        return transition
