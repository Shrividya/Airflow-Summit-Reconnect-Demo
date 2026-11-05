from __future__ import annotations


def _variable_cls():
    try:
        from airflow.sdk import Variable
    except ImportError:  # pragma: no cover
        from airflow.models import Variable
    return Variable


class VariableStore:
    def __init__(self, key: str) -> None:
        self.key = key

    def load(self) -> dict | None:
        Variable = _variable_cls()
        try:
            return Variable.get(self.key, default=None, deserialize_json=True)
        except TypeError:  # pragma: no cover
            return Variable.get(self.key, default_var=None, deserialize_json=True)

    def save(self, state: dict) -> None:
        _variable_cls().set(self.key, state, serialize_json=True)
