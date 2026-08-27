# Consumer rebalance (captured locally)

`LoggingRebalanceListener` in `consumer/consumer.py` prints JSON on `on_partitions_revoked` and `on_partitions_assigned`.

Topic `orders-rebalance-demo` (3 partitions). Two processes in group `orders-rebalance-demo`.

**Member A** started first (`python consumer/consumer.py --topic orders-rebalance-demo --group orders-rebalance-demo --forever`):

```json
{"event": "partitions_revoked", "partitions": []}
{"event": "partitions_assigned", "partitions": [0, 1, 2]}
```

It consumed the produced records, then **member B** joined the same group. Member A logged:

```json
{"event": "partitions_revoked", "partitions": [0, 1, 2]}
{"event": "partitions_assigned", "partitions": [0, 1]}
```

**Member B** logged:

```json
{"event": "partitions_revoked", "partitions": []}
{"event": "partitions_assigned", "partitions": [2]}
```

After the rebalance, partitions 0 and 1 stayed with A; partition 2 moved to B. Each partition is owned by one member of the group.

This is a two-process local capture on a single broker, not a statement about rebalance time in a larger cluster.
