# streaming-service

## Fixed Income Transaction & Holding Event Streams

Additive Kafka path under [`fi_streaming/`](fi_streaming/). Original `orders` topic, `producer/producer.py`, and `consumer/consumer.py` are unchanged.

| Topic | Grain | Maps to |
|---|---|---|
| `fi.position.updates` | `account_id` + `instrument_id` (+ `transaction_id`, `application_id`) | **Holding Master** |
| `fi.cashflow.events` | `account_id` + `transaction_id` | **Transaction Master** |
| `fi.prepayment.signals` | `account_id` + `application_id` | Transaction Master (prepay) feeding Holding Master factor |

```bash
python -m fi_streaming.producer --count 10
python -m fi_streaming.consumer --max-records 30 --timeout-s 20
```

Messages are keyed by `account_id`. The sink upserts SQLite `holding_events` / `transaction_events` on `event_id`. All payloads are synthetic.

---

Local Kafka demo of produce/consume mechanics using the same synthetic e-commerce **order events** domain as `airflow-dbt-warehouse` (customer, order status, order total), with an `event_id` for sink dedup — not Hacker News, not market data.

`kafka_streaming/` and `webscraping/` are the original Hacker-News scraper + FastAPI UI demo. They are not the mechanics demo described below.

Single-broker local demo, not a throughput benchmark.

## Architecture

```
  producer/producer.py
    synthetic order events, key = customer_id
           |
           v
  Kafka topic "orders"  (3 partitions, replication factor 1, one KRaft broker)
           |
           v
  consumer/consumer.py   group = orders-sink
    at-least-once: upsert SQLite then commit offset
           |
           v
  sink.db  (PRIMARY KEY event_id)

  consumer/lag_monitor.py  -> JSON {partition, committed, end, lag}
```

```mermaid
flowchart LR
  P["producer.py<br/>key: customer_id"] --> T["topic orders<br/>3 partitions, RF=1"]
  T --> C["consumer.py<br/>group orders-sink"]
  C --> S["sink.db<br/>upsert on event_id"]
  T -.-> L["lag_monitor.py"]
  C -.-> L
```

## What this shows

- **Partitioning / ordering:** same `customer_id` → same partition (`produce_events`). Global order would need one partition; this topic has three.
- **Delivery:** at-least-once (`enable.auto.commit=false`, `consumer.commit()` after `OrderEventSink.upsert`). Not Kafka transactions.
- **Lag:** `report_lag` compares committed vs end offsets per partition.
- **Rebalance:** `LoggingRebalanceListener` prints revoke/assign. Two-member capture: [evidence/rebalance.md](evidence/rebalance.md).

Details: [docs/kafka-concepts.md](docs/kafka-concepts.md). Evidence: [evidence/lag.md](evidence/lag.md), [evidence/rebalance.md](evidence/rebalance.md).

## How to run

Requires Docker (for the broker) and Python 3.11+. Commands assume the repo root.

```bash
python -m pip install -r requirements.txt
docker compose up -d --wait
python producer/producer.py --count 20
python consumer/consumer.py --max-records 20 --timeout-s 20
python consumer/lag_monitor.py
pytest tests/ -q
```

If host port **9092** is already in use, map **9094:9092** in `docker-compose.yml`, set `KAFKA_ADVERTISED_LISTENERS` `PLAINTEXT_HOST://localhost:9094`, and export `KAFKA_BOOTSTRAP=localhost:9094`.

Stop the broker with `docker compose down`.

CI (`.github/workflows/ci.yml`) starts the same compose stack on `ubuntu-latest` and runs pytest against `localhost:9092`.
