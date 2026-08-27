# Kafka concepts in this repo

This note is tied to the code under `producer/` and `consumer/`. If a behavior is not exercised by a runnable command or test here, it is marked **concept, not yet demonstrated here**.

This repo is a single-broker local demo of produce/consume mechanics. It is not a throughput benchmark.

## Partitioning and ordering

Kafka preserves **order only within a partition**. Global order across a topic requires a single partition.

This demo uses topic `orders` with **3 partitions** (see `ensure_topic` in `producer/producer.py`, `NUM_PARTITIONS = 3`). The producer in `produce_events` sets the record key to `customer_id`:

```python
producer.send(topic, key=event["customer_id"], value=event)
```

Same key → same partition (for a given partition count). That is the guarantee `test_same_customer_id_same_partition` checks. Distinct keys **can** land on different partitions; they are not required to. `test_different_keys_can_land_on_different_partitions` only asserts that a handful of distinct `customer_id`s occupy at least two partitions.

Tradeoff: keying by `customer_id` keeps one customer's events ordered with each other. It does **not** order two different customers relative to each other, and it does not order the topic as a whole.

To see the key on the wire, run:

```bash
python producer/producer.py --count 4 --customer-id 7
```

Each printed JSON line includes `partition`. All four should share one partition.

## Delivery semantics (at-least-once vs exactly-once)

| Mode | What it means here |
|---|---|
| At-most-once | Commit before processing. A crash can skip a message. **Not implemented.** |
| At-least-once | Process (write the sink), then commit. A crash after the write and before the commit redelivers. **This is what `consume_available` does.** |
| Exactly-once | Kafka transactions (and typically an idempotent producer) so the read-process-write cycle commits atomically. **Not implemented. Concept, not yet demonstrated here.** |

`consume_available` in `consumer/consumer.py` builds the consumer with `enable_auto_commit=False`. After each `OrderEventSink.upsert` it calls `consumer.commit` with that record's `offset + 1` (`OffsetAndMetadata`). A crash between upsert and commit redelivers the same Kafka record; the sink's `event_id` primary key keeps the row count unchanged.

Redelivery is made safe by the sink, not by Kafka EOS:

- `OrderEventSink.upsert` inserts into SQLite `order_events` with `PRIMARY KEY (event_id)` and `ON CONFLICT(event_id) DO UPDATE`.
- `test_replay_does_not_duplicate_sink_rows` produces N events, produces the **same** `event_id`s again (new offsets), consumes both copies, and asserts `sink.row_count() == N`.

That is at-least-once delivery plus an idempotent upsert. It is not Kafka exactly-once (no transactional `sendOffsetsToTransaction`).

A kill-and-restart of the consumer process is the same situation the test covers: any record whose sink write finished and whose offset was not yet committed is consumed again; `event_id` prevents a second row.

## Consumer lag

Lag is how far a consumer group is behind the log: for each partition, `lag = end - committed` (or `end - beginning` if the group has never committed).

`report_lag` in `consumer/lag_monitor.py` uses a `KafkaConsumer` that **does not subscribe**, so it does not join the group. It reads `beginning_offsets`, `end_offsets`, and `committed` and prints JSON `{partition, committed, end, lag}`.

Runnable:

```bash
python producer/producer.py --count 30
python consumer/lag_monitor.py --group orders-sink
python consumer/consumer.py --max-records 30 --timeout-s 20
python consumer/lag_monitor.py --group orders-sink
```

Captured numbers from a local run are in `evidence/lag.md`.

## Rebalancing

When members join or leave a consumer group, Kafka **revokes** and **assigns** partitions so that each partition is owned by one member of the group at a time. That pause is a rebalance.

`LoggingRebalanceListener` in `consumer/consumer.py` implements `on_partitions_revoked` and `on_partitions_assigned` and prints JSON. `consume_available` passes that listener to `consumer.subscribe`.

Starting a second process with the same `--group` is the intended demonstration. A two-member capture from this repo is in `evidence/rebalance.md` (`LoggingRebalanceListener` JSON: member A went from `[0, 1, 2]` to `[0, 1]`; member B received `[2]`).
