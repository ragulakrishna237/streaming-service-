"""Fixed Income topics map to Holding Master and Transaction Master sinks."""

from __future__ import annotations

from pathlib import Path

from fi_streaming.consumer import FiMastersSink, consume_fi
from fi_streaming.producer import produce_fi_events
from fi_streaming.topics import (
    make_cashflow_event,
    make_position_update,
    make_prepayment_signal,
)
from producer.producer import ensure_topic


def test_fi_event_grain_has_account_transaction_application():
    pos = make_position_update(account_id="ACC-0001", transaction_id="TXN-1", application_id="APP-1")
    cf = make_cashflow_event(account_id="ACC-0001", transaction_id="TXN-2", application_id="APP-1")
    pp = make_prepayment_signal(account_id="ACC-0001", transaction_id="TXN-3", application_id="APP-1")
    for event in (pos, cf, pp):
        assert event["account_id"]
        assert event["transaction_id"]
        assert event["application_id"]
        assert event["event_id"]
        assert event["event_type"]


def test_fi_sink_idempotent_without_kafka(tmp_path: Path) -> None:
    sink = FiMastersSink(tmp_path / "fi.db")
    pos = make_position_update(event_id="hold-1", account_id="ACC-0001", instrument_id="UST_10Y")
    cf = make_cashflow_event(event_id="txn-1", account_id="ACC-0001", transaction_id="TXN-1")
    try:
        sink.upsert(pos, topic="fi.position.updates", partition=0, offset=0)
        sink.upsert(pos, topic="fi.position.updates", partition=0, offset=1)
        sink.upsert(cf, topic="fi.cashflow.events", partition=0, offset=0)
        sink.upsert(cf, topic="fi.cashflow.events", partition=0, offset=1)
        assert sink.holding_count() == 1
        assert sink.transaction_count() == 1
    finally:
        sink.close()


def test_fi_kafka_roundtrip(kafka_bootstrap: str, unique_name: str, tmp_path: Path) -> None:
    pos_topic = f"fi.pos.{unique_name}"
    txn_topic = f"fi.txn.{unique_name}"
    ensure_topic(pos_topic, bootstrap=kafka_bootstrap, num_partitions=3, replication_factor=1)
    ensure_topic(txn_topic, bootstrap=kafka_bootstrap, num_partitions=3, replication_factor=1)
    positions = [
        make_position_update(event_id=f"h-{unique_name}-{i}", account_id="ACC-0042", instrument_id="UST_10Y")
        for i in range(4)
    ]
    cashflows = [
        make_cashflow_event(event_id=f"t-{unique_name}-{i}", account_id="ACC-0042", transaction_id=f"TXN-{i}")
        for i in range(4)
    ]
    produce_fi_events(positions, bootstrap=kafka_bootstrap, topic=pos_topic)
    produce_fi_events(cashflows, bootstrap=kafka_bootstrap, topic=txn_topic)
    processed = consume_fi(
        bootstrap=kafka_bootstrap,
        group_id=f"fi-sink-{unique_name}",
        sink_path=tmp_path / "fi.db",
        idle_timeout_s=15.0,
        max_records=8,
        topics=[pos_topic, txn_topic],
    )
    assert processed == 8
    sink = FiMastersSink(tmp_path / "fi.db")
    try:
        assert sink.holding_count() == 4
        assert sink.transaction_count() == 4
    finally:
        sink.close()
