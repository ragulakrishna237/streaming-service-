"""At-least-once FI consumer: Holding Master + Transaction Master SQLite sinks."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

from consumer.consumer import LoggingRebalanceListener, utc_now_iso
from fi_streaming.topics import BOOTSTRAP, TOPIC_CASHFLOWS, TOPIC_POSITIONS, TOPIC_PREPAY

GROUP_ID = os.environ.get("FI_KAFKA_GROUP", "fi-masters-sink")
SINK_DB = os.environ.get("FI_SINK_DB", "fi_sink.db")


class FiMastersSink:
    """Holding Master rows from position updates; Transaction Master from cashflow/prepay."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS holding_events (
                event_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                transaction_id TEXT,
                application_id TEXT,
                instrument_id TEXT NOT NULL,
                position_qty TEXT,
                market_value TEXT,
                as_of_ts TEXT,
                event_ts TEXT NOT NULL,
                kafka_partition INTEGER,
                kafka_offset INTEGER,
                ingested_at TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transaction_events (
                event_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                transaction_id TEXT NOT NULL,
                application_id TEXT,
                event_type TEXT NOT NULL,
                instrument_id TEXT,
                payload TEXT NOT NULL,
                event_ts TEXT NOT NULL,
                kafka_partition INTEGER,
                kafka_offset INTEGER,
                ingested_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def upsert(self, event: dict[str, Any], *, topic: str, partition: int | None, offset: int | None) -> None:
        if topic.endswith("position.updates") or event.get("event_type") == "position_update":
            self._conn.execute(
                """
                INSERT INTO holding_events (
                    event_id, account_id, transaction_id, application_id, instrument_id,
                    position_qty, market_value, as_of_ts, event_ts,
                    kafka_partition, kafka_offset, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    position_qty = excluded.position_qty,
                    market_value = excluded.market_value,
                    ingested_at = excluded.ingested_at
                """,
                (
                    event["event_id"],
                    event["account_id"],
                    event.get("transaction_id"),
                    event.get("application_id"),
                    event["instrument_id"],
                    str(event.get("position_qty")),
                    str(event.get("market_value")),
                    event.get("as_of_ts"),
                    event["event_ts"],
                    partition,
                    offset,
                    utc_now_iso(),
                ),
            )
        else:
            self._conn.execute(
                """
                INSERT INTO transaction_events (
                    event_id, account_id, transaction_id, application_id, event_type,
                    instrument_id, payload, event_ts, kafka_partition, kafka_offset, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    payload = excluded.payload,
                    ingested_at = excluded.ingested_at
                """,
                (
                    event["event_id"],
                    event["account_id"],
                    event["transaction_id"],
                    event.get("application_id"),
                    event["event_type"],
                    event.get("instrument_id"),
                    json.dumps(event),
                    event["event_ts"],
                    partition,
                    offset,
                    utc_now_iso(),
                ),
            )
        self._conn.commit()

    def holding_count(self) -> int:
        (count,) = self._conn.execute("SELECT COUNT(*) FROM holding_events").fetchone()
        return int(count)

    def transaction_count(self) -> int:
        (count,) = self._conn.execute("SELECT COUNT(*) FROM transaction_events").fetchone()
        return int(count)

    def close(self) -> None:
        self._conn.close()


def consume_fi(
    *,
    bootstrap: str = BOOTSTRAP,
    group_id: str = GROUP_ID,
    sink_path: str | Path = SINK_DB,
    idle_timeout_s: float = 8.0,
    max_records: int | None = None,
    topics: list[str] | None = None,
) -> int:
    sink = FiMastersSink(sink_path)
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    consumer.subscribe(
        topics or [TOPIC_POSITIONS, TOPIC_CASHFLOWS, TOPIC_PREPAY],
        listener=LoggingRebalanceListener(),
    )
    processed = 0
    idle_deadline = time.time() + idle_timeout_s
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                if time.time() >= idle_deadline:
                    break
                continue
            idle_deadline = time.time() + idle_timeout_s
            for _tp, records in batch.items():
                for record in records:
                    event = record.value
                    sink.upsert(event, topic=record.topic, partition=record.partition, offset=record.offset)
                    consumer.commit(
                        {
                            TopicPartition(record.topic, record.partition): OffsetAndMetadata(
                                record.offset + 1, ""
                            )
                        }
                    )
                    processed += 1
                    if max_records is not None and processed >= max_records:
                        return processed
    finally:
        consumer.close()
        time.sleep(0.5)
        sink.close()
    return processed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Consume FI events into Holding/Transaction Master sinks.")
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    parser.add_argument("--group", default=GROUP_ID)
    parser.add_argument("--sink", default=SINK_DB)
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    args = parser.parse_args(argv)
    n = consume_fi(
        bootstrap=args.bootstrap,
        group_id=args.group,
        sink_path=args.sink,
        idle_timeout_s=args.timeout_s,
        max_records=args.max_records,
    )
    print(json.dumps({"processed": n}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
