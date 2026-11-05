from __future__ import annotations

import os

PARTNER_FILE_URI = "s3://partner-drop/settlements.csv"


def partner_drop_asset():
    from airflow.sdk import Asset, AssetWatcher

    queue = os.getenv("PARTNER_DROP_QUEUE", "").strip()
    if not queue:
        return Asset(PARTNER_FILE_URI)
    from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

    return Asset(
        PARTNER_FILE_URI,
        watchers=[AssetWatcher(name="partner_drop_queue", trigger=MessageQueueTrigger(queue=queue))],
    )
