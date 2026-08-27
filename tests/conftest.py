"""Shared Kafka wait helper for tests that talk to the local broker."""

from __future__ import annotations

import os
import time
import uuid

import pytest
from kafka.admin import KafkaAdminClient

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def wait_for_kafka(bootstrap: str = BOOTSTRAP, timeout_s: float = 90.0) -> None:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="pytest-wait", request_timeout_ms=2000)
            admin.list_topics()
            admin.close()
            return
        except Exception as err:  # noqa: BLE001 — broker may still be starting
            last_err = err
            time.sleep(1)
    raise RuntimeError(f"Kafka not reachable at {bootstrap}: {last_err}")


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    wait_for_kafka(BOOTSTRAP)
    return BOOTSTRAP


@pytest.fixture
def unique_name() -> str:
    return uuid.uuid4().hex[:12]
