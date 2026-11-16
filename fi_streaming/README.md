# Fixed Income Transaction & Holding Event Streams

New Kafka topics and producer/consumer code. The original `orders` topic and `producer/` / `consumer/` e-commerce path are untouched.

| Topic | Event types | Grain | Downstream master |
|---|---|---|---|
| `fi.position.updates` | position snapshots | `account_id` + `instrument_id` | **Holding Master** |
| `fi.cashflow.events` | coupon, paydown, fee | `account_id` + `transaction_id` | **Transaction Master** |
| `fi.prepayment.signals` | S-curve / CPR-style signals | `account_id` + `application_id` | Transaction Master (prepay) + Holding Master (factor) |

Messages are keyed by `account_id` so one account stays on one partition. The sink upserts SQLite tables `holding_events` and `transaction_events` on `event_id`.
