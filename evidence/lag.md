# Consumer lag (captured locally)

Single-broker KRaft Kafka (`apache/kafka:3.9.0` via `docker-compose.yml`). Commands run from the repo root after `docker compose up -d --wait`.

Topic had 3 partitions. Group had never committed (`committed` is `null`), so `report_lag` in `consumer/lag_monitor.py` used `lag = end - beginning`.

Produced **30** order events, then:

```text
python consumer/lag_monitor.py --topic <demo-topic> --group <demo-group>
```

**Before consume** (`lag_before`):

```json
[
  {"partition": 0, "committed": null, "end": 13, "lag": 13},
  {"partition": 1, "committed": null, "end": 6, "lag": 6},
  {"partition": 2, "committed": null, "end": 11, "lag": 11}
]
```

Total lag 13 + 6 + 11 = 30, matching the 30 produced records.

Then `consume_available` read those 30 records (upsert + commit). **After consume** (`lag_after`):

```json
[
  {"partition": 0, "committed": 13, "end": 13, "lag": 0},
  {"partition": 1, "committed": 6, "end": 6, "lag": 0},
  {"partition": 2, "committed": 11, "end": 11, "lag": 0}
]
```

This is one local run, not a throughput number.
