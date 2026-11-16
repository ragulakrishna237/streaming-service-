"""Produce synthetic FI position, cashflow, and prepayment events."""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NotLeaderForPartitionError, TopicAlreadyExistsError
from kafka.producer.future import RecordMetadata

from producer.producer import wait_for_topic
from fi_streaming.topics import (
    BOOTSTRAP,
    NUM_PARTITIONS,
    TOPIC_CASHFLOWS,
    TOPIC_POSITIONS,
    TOPIC_PREPAY,
    make_cashflow_event,
    make_position_update,
    make_prepayment_signal,
)


def produce_fi_events(
    events: list[dict],
    *,
    bootstrap: str,
    topic: str,
) -> list[RecordMetadata]:
    last_err: Exception | None = None
    for attempt in range(5):
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            key_serializer=lambda k: str(k).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )
        try:
            producer.partitions_for(topic)
            futures = [producer.send(topic, key=event["account_id"], value=event) for event in events]
            producer.flush()
            return [fut.get(timeout=30) for fut in futures]
        except NotLeaderForPartitionError as err:
            last_err = err
            time.sleep(0.5 * (attempt + 1))
        finally:
            producer.close()
    raise RuntimeError(f"produce_fi_events failed after retries: {last_err}") from last_err


def ensure_fi_topics(*, bootstrap: str = BOOTSTRAP, num_partitions: int = NUM_PARTITIONS) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="fi-admin")
    try:
        admin.create_topics(
            [
                NewTopic(name=name, num_partitions=num_partitions, replication_factor=1)
                for name in (TOPIC_POSITIONS, TOPIC_CASHFLOWS, TOPIC_PREPAY)
            ],
            validate_only=False,
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
    for name in (TOPIC_POSITIONS, TOPIC_CASHFLOWS, TOPIC_PREPAY):
        wait_for_topic(name, bootstrap=bootstrap, num_partitions=num_partitions)


def produce_fi_batch(
    *,
    bootstrap: str = BOOTSTRAP,
    count: int = 10,
    account_id: str | None = None,
) -> dict[str, list[Any]]:
    positions = [
        make_position_update(**({"account_id": account_id} if account_id else {})) for _ in range(count)
    ]
    cashflows = [
        make_cashflow_event(**({"account_id": account_id} if account_id else {})) for _ in range(count)
    ]
    prepays = [
        make_prepayment_signal(**({"account_id": account_id} if account_id else {})) for _ in range(count)
    ]
    return {
        TOPIC_POSITIONS: produce_fi_events(positions, bootstrap=bootstrap, topic=TOPIC_POSITIONS),
        TOPIC_CASHFLOWS: produce_fi_events(cashflows, bootstrap=bootstrap, topic=TOPIC_CASHFLOWS),
        TOPIC_PREPAY: produce_fi_events(prepays, bootstrap=bootstrap, topic=TOPIC_PREPAY),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Produce synthetic FI Kafka events.")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    parser.add_argument("--account-id", default=None)
    args = parser.parse_args(argv)
    ensure_fi_topics(bootstrap=args.bootstrap)
    produce_fi_batch(bootstrap=args.bootstrap, count=args.count, account_id=args.account_id)
    print(json.dumps({"topics": [TOPIC_POSITIONS, TOPIC_CASHFLOWS, TOPIC_PREPAY], "count": args.count}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
