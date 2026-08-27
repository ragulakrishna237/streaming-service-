"""Print per-partition consumer-group lag as JSON.

lag = end offset - committed offset (or end - beginning if the group has never committed).
This process does not subscribe, so it does not join the group or trigger a rebalance.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from kafka import KafkaConsumer, TopicPartition


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


def report_lag(
    *,
    bootstrap: str = BOOTSTRAP,
    topic: str = TOPIC,
    group_id: str = GROUP_ID,
) -> list[dict[str, int | None]]:
    """Return {partition, committed, end, lag} for each partition of topic."""
    probe = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )
    try:
        parts = probe.partitions_for_topic(topic)
        if not parts:
            return []
        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        ends = probe.end_offsets(tps)
        beginnings = probe.beginning_offsets(tps)
        rows: list[dict[str, int | None]] = []
        for tp in tps:
            committed = probe.committed(tp)
            end = int(ends[tp])
            beginning = int(beginnings[tp])
            if committed is None:
                lag = end - beginning
            else:
                lag = max(end - int(committed), 0)
            rows.append(
                {
                    "partition": tp.partition,
                    "committed": None if committed is None else int(committed),
                    "end": end,
                    "lag": lag,
                }
            )
        return rows
    finally:
        probe.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print consumer-group lag per partition.")
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group", default=GROUP_ID)
    args = parser.parse_args(argv)

    rows = report_lag(bootstrap=args.bootstrap, topic=args.topic, group_id=args.group)
    print(json.dumps(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
