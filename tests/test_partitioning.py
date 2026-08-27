"""Keyed producer: same customer_id shares a partition; distinct keys can differ."""

from __future__ import annotations

from producer.producer import ensure_topic, make_order_event, produce_events


def test_same_customer_id_same_partition(kafka_bootstrap: str, unique_name: str) -> None:
    topic = f"orders-part-{unique_name}"
    ensure_topic(topic, bootstrap=kafka_bootstrap, num_partitions=3, replication_factor=1)

    events = [
        make_order_event(customer_id=42, event_id=f"same-key-{unique_name}-{i}")
        for i in range(2)
    ]
    metadata = produce_events(events, bootstrap=kafka_bootstrap, topic=topic)
    assert metadata[0].partition == metadata[1].partition


def test_different_keys_can_land_on_different_partitions(kafka_bootstrap: str, unique_name: str) -> None:
    topic = f"orders-keys-{unique_name}"
    ensure_topic(topic, bootstrap=kafka_bootstrap, num_partitions=3, replication_factor=1)

    events = [
        make_order_event(customer_id=customer_id, event_id=f"diff-key-{unique_name}-{customer_id}")
        for customer_id in range(1, 31)
    ]
    metadata = produce_events(events, bootstrap=kafka_bootstrap, topic=topic)
    partitions = {meta.partition for meta in metadata}
    assert len(partitions) >= 2
