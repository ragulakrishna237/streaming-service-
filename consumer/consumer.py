"""At-least-once Kafka consumer with an idempotent SQLite upsert sink.

Delivery mode: at-least-once (enable.auto.commit=false, commit after sink write)
plus sink idempotency on event_id. This is not Kafka transactions / EOS.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from kafka import ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata, TopicPartition


def _ignore_closed_selector_fds() -> None:
    """kafka-python may unregister a socket that Windows already closed (fileno -1)."""
    import selectors

    if getattr(selectors.SelectSelector.unregister, "_orders_patched", False):
        return

    original = selectors.SelectSelector.unregister

    def unregister(self, fileobj):  # noqa: ANN001
        try:
            return original(self, fileobj)
        except (ValueError, KeyError, OSError):
            return None

    unregister._orders_patched = True  # type: ignore[attr-defined]
    selectors.SelectSelector.unregister = unregister  # type: ignore[method-assign]


_ignore_closed_selector_fds()

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
GROUP_ID = os.environ.get("KAFKA_GROUP", "orders-sink")
SINK_DB = os.environ.get("SINK_DB", "sink.db")


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class LoggingRebalanceListener(ConsumerRebalanceListener):
    """Prints partition revoke/assign as JSON so a rebalance can be captured in logs."""

    def on_partitions_revoked(self, revoked: list[TopicPartition]) -> None:
        print(
            json.dumps(
                {
                    "event": "partitions_revoked",
                    "partitions": sorted(tp.partition for tp in revoked),
                }
            ),
            flush=True,
        )

    def on_partitions_assigned(self, assigned: list[TopicPartition]) -> None:
        print(
            json.dumps(
                {
                    "event": "partitions_assigned",
                    "partitions": sorted(tp.partition for tp in assigned),
                }
            ),
            flush=True,
        )


class OrderEventSink:
    """SQLite upsert keyed by unique event_id. Replay does not create duplicate rows."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS order_events (
                event_id TEXT PRIMARY KEY,
                order_id INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                order_total TEXT NOT NULL,
                event_ts TEXT NOT NULL,
                kafka_partition INTEGER,
                kafka_offset INTEGER,
                ingested_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def upsert(self, event: dict[str, Any], *, partition: int | None, offset: int | None) -> None:
        self._conn.execute(
            """
            INSERT INTO order_events (
                event_id, order_id, customer_id, status, order_total, event_ts,
                kafka_partition, kafka_offset, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                order_id = excluded.order_id,
                customer_id = excluded.customer_id,
                status = excluded.status,
                order_total = excluded.order_total,
                event_ts = excluded.event_ts,
                kafka_partition = excluded.kafka_partition,
                kafka_offset = excluded.kafka_offset,
                ingested_at = excluded.ingested_at
            """,
            (
                event["event_id"],
                int(event["order_id"]),
                int(event["customer_id"]),
                event["status"],
                str(event["order_total"]),
                event["event_ts"],
                partition,
                offset,
                utc_now_iso(),
            ),
        )
        self._conn.commit()

    def row_count(self) -> int:
        (count,) = self._conn.execute("SELECT COUNT(*) FROM order_events").fetchone()
        return int(count)

    def unique_event_ids(self) -> set[str]:
        rows = self._conn.execute("SELECT event_id FROM order_events").fetchall()
        return {row[0] for row in rows}

    def close(self) -> None:
        self._conn.close()


def _build_consumer(
    *,
    bootstrap: str,
    topic: str,
    group_id: str,
    listener: ConsumerRebalanceListener | None = None,
) -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    consumer.subscribe([topic], listener=listener or LoggingRebalanceListener())
    return consumer


def consume_available(
    *,
    bootstrap: str = BOOTSTRAP,
    topic: str = TOPIC,
    group_id: str = GROUP_ID,
    sink_path: str | Path = SINK_DB,
    idle_timeout_s: float = 8.0,
    max_records: int | None = None,
    run_forever: bool = False,
) -> int:
    """Poll, upsert, then commit. Returns how many records were processed (including replays)."""
    sink = OrderEventSink(sink_path)
    consumer = _build_consumer(bootstrap=bootstrap, topic=topic, group_id=group_id)
    processed = 0
    idle_deadline = time.time() + idle_timeout_s
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                if run_forever:
                    continue
                if time.time() >= idle_deadline:
                    break
                continue
            idle_deadline = time.time() + idle_timeout_s
            for _tp, records in batch.items():
                for record in records:
                    sink.upsert(record.value, partition=record.partition, offset=record.offset)
                    consumer.commit(
                        {
                            TopicPartition(record.topic, record.partition): OffsetAndMetadata(
                                record.offset + 1, ""
                            )
                        }
                    )
                    processed += 1
                    print(
                        json.dumps(
                            {
                                "event_id": record.value.get("event_id"),
                                "customer_id": record.value.get("customer_id"),
                                "partition": record.partition,
                                "offset": record.offset,
                                "sink_rows": sink.row_count(),
                            }
                        ),
                        flush=True,
                    )
                    if max_records is not None and processed >= max_records:
                        return processed
    finally:
        consumer.close()
        # Heartbeat thread can outlive close(); a short wait avoids a second
        # consumer in the same process hitting a closed selector (seen on Windows).
        time.sleep(0.5)
        sink.close()
    return processed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Consume order events (at-least-once + idempotent SQLite upsert)."
    )
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group", default=GROUP_ID)
    parser.add_argument("--sink", default=SINK_DB)
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--timeout-s", type=float, default=10.0, help="Idle exit timeout unless --forever.")
    parser.add_argument("--forever", action="store_true", help="Do not exit on idle.")
    args = parser.parse_args(argv)

    consume_available(
        bootstrap=args.bootstrap,
        topic=args.topic,
        group_id=args.group,
        sink_path=args.sink,
        idle_timeout_s=args.timeout_s,
        max_records=args.max_records,
        run_forever=args.forever,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
