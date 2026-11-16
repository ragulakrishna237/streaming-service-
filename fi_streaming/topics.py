"""FI topic names and event factories. Synthetic only."""

from __future__ import annotations

import os
import random
import uuid
from datetime import UTC, datetime
from typing import Any

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
NUM_PARTITIONS = int(os.environ.get("KAFKA_NUM_PARTITIONS", "3"))

TOPIC_POSITIONS = os.environ.get("FI_TOPIC_POSITIONS", "fi.position.updates")
TOPIC_CASHFLOWS = os.environ.get("FI_TOPIC_CASHFLOWS", "fi.cashflow.events")
TOPIC_PREPAY = os.environ.get("FI_TOPIC_PREPAY", "fi.prepayment.signals")

FI_TOPICS = (TOPIC_POSITIONS, TOPIC_CASHFLOWS, TOPIC_PREPAY)

INSTRUMENTS = ("UST_10Y", "FNCL_5_5", "USD_IRS_10Y", "G2SF_5_0")
CASHFLOW_TYPES = ("coupon", "paydown", "fee", "maturity")
PREPAY_TYPES = ("cpr_signal", "smm_signal", "turnover")


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def make_position_update(**overrides: Any) -> dict[str, Any]:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "position_update",
        "account_id": f"ACC-{(random.randint(1, 12)):04d}",
        "transaction_id": f"TXN-{uuid.uuid4().hex[:8]}",
        "application_id": f"APP-{(random.randint(1, 20)):04d}",
        "instrument_id": random.choice(INSTRUMENTS),
        "position_qty": round(random.uniform(1e4, 5e6), 2),
        "market_value": round(random.uniform(1e4, 5e6), 2),
        "as_of_ts": utc_now_iso(),
        "event_ts": utc_now_iso(),
    }
    event.update(overrides)
    return event


def make_cashflow_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(CASHFLOW_TYPES),
        "account_id": f"ACC-{(random.randint(1, 12)):04d}",
        "transaction_id": f"TXN-{uuid.uuid4().hex[:8]}",
        "application_id": f"APP-{(random.randint(1, 20)):04d}",
        "instrument_id": random.choice(INSTRUMENTS),
        "amount": round(random.uniform(100.0, 25000.0), 2),
        "currency": "USD",
        "event_ts": utc_now_iso(),
    }
    event.update(overrides)
    return event


def make_prepayment_signal(**overrides: Any) -> dict[str, Any]:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(PREPAY_TYPES),
        "account_id": f"ACC-{(random.randint(1, 12)):04d}",
        "transaction_id": f"TXN-{uuid.uuid4().hex[:8]}",
        "application_id": f"APP-{(random.randint(1, 20)):04d}",
        "instrument_id": random.choice(("FNCL_5_5", "G2SF_5_0")),
        "cpr": round(random.uniform(0.02, 0.35), 4),
        "smm": round(random.uniform(0.001, 0.04), 4),
        "event_ts": utc_now_iso(),
    }
    event.update(overrides)
    return event
