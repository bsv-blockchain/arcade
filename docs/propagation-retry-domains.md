# Propagation retry domains

Merkle registration retry and network propagation retry are independent failure domains.

`/watch` must succeed before a transaction is broadcast to Teranode (F-024). A failure to register has not yet consumed a network propagation attempt. The fast-path network budget `propagation.retry_max_attempts` counts only Teranode/network requeue decisions: no healthy endpoint, HTTP 202 with no verdict, timeout, transport error, opaque 4xx/5xx, batch-shape failure, missing-parent handling, chunk narrowing, and the other no-verdict broadcast results.

A Merkle `/watch` failure is requeued on its own counter, using the same `propagation.retry_backoff_ms` delay. That counter is not `retry_max_attempts`. Five registration failures leave the network budget at its initial value, so the first successful registration is still followed by a normal broadcast opportunity. The transaction is not broadcast while registration is failing.

Both budgets are bounded. When the Merkle registration budget is spent, the transaction is parked at the existing `PENDING_RETRY` status and the reaper retries it. Parking releases the dispatcher in-flight entry so the Kafka commit watermark can advance. It does not mark the transaction `REJECTED`, and it does not debit the network budget. Claim revocation is not a retry of either kind: the offset stays uncommitted and the next claim replays the transaction.

Structured logs carry `retry_stage=merkle` or `retry_stage=network`. Prometheus series use the same two stages and never a txid:

- `arcade_propagation_retry_total{stage="merkle"|"network"}`
- `arcade_propagation_merkle_register_failures_total{reason}` — `claim_revoked`, `auth_error`, `http_5xx`, `timeout`, `transport`, `register_error`
- `arcade_propagation_merkle_retry_exhausted_total` — Merkle budget spent
- `arcade_propagation_requeue_exhausted_total` — network budget spent
