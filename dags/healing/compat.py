from __future__ import annotations

try:
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:
    try:
        from airflow.exceptions import AirflowFailException
    except ImportError:

        class AirflowFailException(Exception):  # type: ignore[no-redef]
            pass


__all__ = ["AirflowFailException"]


class _NoCheckpoint:

    def get(self, key, default=None):
        return default

    def set(self, key, value, **_):
        return None


def task_checkpoint(context):
    store = context.get("task_state_store") if hasattr(context, "get") else None
    return store if store is not None else _NoCheckpoint()


__all__ += ["task_checkpoint"]
