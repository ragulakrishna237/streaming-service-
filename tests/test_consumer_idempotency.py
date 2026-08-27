"""Replay-safety: the same event_id does not create a second sink row."""

from __future__ import annotations

from pathlib import Path

from consumer.consumer import OrderEventSink, consume_available
from producer.producer import ensure_topic, make_order_event, produce_events


def test_replay_does_not_duplicate_sink_rows(kafka_bootstrap: str, unique_name: str, tmp_path: Path) -> None:
    topic = f"orders-idem-{unique_name}"
    group_id = f"sink-idem-{unique_name}"
    sink_path = tmp_path / "sink.db"
    n = 12

    ensure_topic(topic, bootstrap=kafka_bootstrap, num_partitions=3, replication_factor=1)
    events = [
        make_order_event(event_id=f"evt-{unique_name}-{i}", customer_id=(i % 7) + 1, order_id=2000 + i)
        for i in range(n)
    ]

    # Same payloads twice on the topic (new offsets, same event_id). One consume
    # pass reads both copies; the sink must still have N rows.

    produce_events(events, bootstrap=kafka_bootstrap, topic=topic)
    produce_events(events, bootstrap=kafka_bootstrap, topic=topic)
    processed = consume_available(
        bootstrap=kafka_bootstrap,
        topic=topic,
        group_id=group_id,
        sink_path=sink_path,
        idle_timeout_s=15.0,
        max_records=n * 2,
    )
    assert processed == n * 2

    sink = OrderEventSink(sink_path)
    try:
        assert sink.row_count() == n
        assert sink.unique_event_ids() == {event["event_id"] for event in events}
    finally:
        sink.close()
