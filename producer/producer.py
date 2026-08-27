"""Produce synthetic e-commerce order events to Kafka.

Events use the same domain as airflow-dbt-warehouse (orders with
customer_id, status, order_total) plus an event_id for sink dedup.

Messages are keyed by customer_id so related events land on one partition.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NotLeaderForPartitionError, TopicAlreadyExistsError, UnknownTopicOrPartitionError
from kafka.producer.future import RecordMetadata

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
NUM_PARTITIONS = int(os.environ.get("KAFKA_NUM_PARTITIONS", "3"))
REPLICATION_FACTOR = 1
# Same status labels as airflow-dbt-warehouse/data_generator/generate_orders.py
STATUSES = ("placed", "shipped", "delivered", "cancelled")
CUSTOMER_ID_MAX = 100


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def make_order_event(
    *,
    event_id: str | None = None,
    order_id: int | None = None,
    customer_id: int | None = None,
    status: str | None = None,
    order_total: str | None = None,
    event_ts: str | None = None,
) -> dict[str, Any]:
    """Build one order-event payload. event_id is the idempotency key."""
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "order_id": order_id if order_id is not None else random.randint(202401150001, 202401159999),
        "customer_id": customer_id if customer_id is not None else random.randint(1, CUSTOMER_ID_MAX),
        "status": status or random.choice(STATUSES),
        "order_total": order_total or f"{random.uniform(8.0, 180.0):.2f}",
        "event_ts": event_ts or utc_now_iso(),
    }


def wait_for_topic(
    topic: str,
    *,
    bootstrap: str = BOOTSTRAP,
    num_partitions: int = NUM_PARTITIONS,
    timeout_s: float = 30.0,
) -> None:
    """Block until the topic exists and partition leaders are visible to clients."""
    deadline = time.time() + timeout_s
    last_count: int | None = None
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="orders-wait")
    try:
        while time.time() < deadline:
            try:
                infos = admin.describe_topics([topic])
            except UnknownTopicOrPartitionError:
                time.sleep(0.25)
                continue
            if infos:
                partitions = infos[0].get("partitions") or []
                last_count = len(partitions)
                leaders_ready = partitions and all(
                    p.get("leader") not in (None, -1) for p in partitions
                )
                if last_count == num_partitions and leaders_ready:
                    return
            time.sleep(0.25)
    finally:
        admin.close()
    raise RuntimeError(
        f"Topic {topic!r} not ready with {num_partitions} partitions "
        f"(last seen {last_count}) at {bootstrap}"
    )


def ensure_topic(
    topic: str = TOPIC,
    *,
    bootstrap: str = BOOTSTRAP,
    num_partitions: int = NUM_PARTITIONS,
    replication_factor: int = REPLICATION_FACTOR,
) -> None:
    """Create the topic if missing. Single-broker demo: replication factor 1."""
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="orders-admin")
    try:
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)],
            validate_only=False,
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
    wait_for_topic(topic, bootstrap=bootstrap, num_partitions=num_partitions)


def _producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


def produce_events(
    events: list[dict[str, Any]],
    *,
    bootstrap: str = BOOTSTRAP,
    topic: str = TOPIC,
) -> list[RecordMetadata]:
    """Send events keyed by customer_id. Returns per-record metadata (includes partition)."""
    last_err: Exception | None = None
    for attempt in range(5):
        producer = _producer(bootstrap)
        try:
            producer.partitions_for(topic)
            futures = [
                producer.send(topic, key=event["customer_id"], value=event)
                for event in events
            ]
            producer.flush()
            return [fut.get(timeout=30) for fut in futures]
        except NotLeaderForPartitionError as err:
            last_err = err
            time.sleep(0.5 * (attempt + 1))
        finally:
            producer.close()
    raise RuntimeError(f"produce_events failed after retries: {last_err}") from last_err


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Produce synthetic order events to Kafka.")
    parser.add_argument("--count", type=int, default=20, help="Number of events to send.")
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--interval-s", type=float, default=0.0, help="Sleep between sends (0 = as fast as local broker allows).")
    parser.add_argument("--customer-id", type=int, default=None, help="Fix all events to one customer_id (same partition).")
    args = parser.parse_args(argv)

    ensure_topic(args.topic, bootstrap=args.bootstrap)
    events = [
        make_order_event(customer_id=args.customer_id)
        for _ in range(args.count)
    ]

    if args.interval_s > 0:
        producer = _producer(args.bootstrap)
        try:
            for event in events:
                meta = producer.send(args.topic, key=event["customer_id"], value=event).get(timeout=30)
                print(
                    json.dumps(
                        {
                            "event_id": event["event_id"],
                            "customer_id": event["customer_id"],
                            "partition": meta.partition,
                            "offset": meta.offset,
                        }
                    )
                )
                time.sleep(args.interval_s)
            producer.flush()
        finally:
            producer.close()
    else:
        metadata = produce_events(events, bootstrap=args.bootstrap, topic=args.topic)
        for event, meta in zip(events, metadata, strict=True):
            print(
                json.dumps(
                    {
                        "event_id": event["event_id"],
                        "customer_id": event["customer_id"],
                        "partition": meta.partition,
                        "offset": meta.offset,
                    }
                )
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
