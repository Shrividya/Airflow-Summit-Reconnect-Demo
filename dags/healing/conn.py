from __future__ import annotations


def get_connection(conn_id: str):
    try:
        from airflow.sdk import BaseHook
    except ImportError:
        from airflow.hooks.base import BaseHook
    return BaseHook.get_connection(conn_id)
