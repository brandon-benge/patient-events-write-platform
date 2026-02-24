# Tradeoff's

## Why RDBMS vs Iceberg:

I did not bypass the RDBMS because the system requires per-entity serialized versioning, atomic commit-before-acknowledge semantics, and monotonic gating under client retry pressure. Iceberg provides snapshot-level ACID for analytics, not low-latency OLTP authority. Using Iceberg alone would require reimplementing database semantics in distributed services.

- Synchronous lookup by phi_id for updates (not eventual consistency).
- Atomic, serialized version assignment per phi_id (strictly monotonic current_version).
- Replay safety and idempotency guarantees under client retries.
- Back-pressure to clients (the API must know definitively whether the PHI commit succeeded before returning 202).
- Monotonic persisted confirmation (persisted_version) to ensure projection safety and prevent regression.

Iceberg does not provide:
- Per-key row-level locking or serialization.
- Enforced uniqueness constraints.
- Low-latency conditional updates (UPDATE ... WHERE persisted_version < v).
- Deterministic “commit-before-acknowledge” semantics for OLTP workflows.

---

## Why CDC vs Changelog Table(outbox) vs Authoritative Queue


I chose CDC because it provides commit-before-emit and replay with no dual-write ambiguity: the API performs one atomic DB transaction, and the stream is a faithful reflection of committed truth; outbox adds write/publisher overhead, and an authoritative queue would force event-sourcing or reintroduce dual-write correctness problems.

### Comparison

| Dimension | CDC (WAL / Debezium) | Changelog / Outbox Table | Authoritative Queue (Kafka-first) |
|------------|----------------------|---------------------------|------------------------------------|
| Source of Truth | RDBMS (rows are authoritative) | RDBMS (business row + outbox row) | Queue (log is authoritative) |
| Commit-Before-Emit | Guaranteed (event exists only after DB commit) | Guaranteed (same DB transaction) | Not guaranteed unless event-sourced |
| Dual-Write Risk | None (single DB write) | None inside DB, but requires publisher process | Yes unless DB is fully derived from queue |
| Write Amplification | Minimal (existing immutable version row) | Higher (extra outbox row + indexes) | Depends on architecture; often requires additional state store |
| Replay / Recovery | Resume from WAL offset / LSN | Resume from outbox high-water mark | Replay from topic offset |
| Operational Complexity | Manage replication slots + connector | Manage outbox poller + scaling | Manage ordering, idempotency, and state reconstruction |
| Ordering Guarantees | DB commit order (per shard) | DB commit order | Partition order only |
| Fit for This Design | Strong alignment (commit-before-ack, serialized versioning) | Acceptable but redundant | Conflicts with OLTP authority unless fully event-sourced |

---

## Redis Fail-Closed Admission vs Always-Accept + Reconcile


I intentionally chose a fail-closed admission model at the API layer using Redis for idempotency receipts and monotonic gating.

### Choice
- API rejects requests if Redis is unavailable.
- API does not return 202 until Redis reflects `PHI_COMMITTED`.
- Redis stores `(event_id → request_hash, status)` for replay detection and conflict enforcement.

### What This Buys
- Deterministic idempotency under client retries.
- Immediate conflict detection (409 on mismatched payload replay).
- Clear back-pressure semantics to clients.
- Prevents phantom creates/updates caused by partial failures.

### What It Costs
- Redis becomes a Tier-0 dependency for write availability.
- During Redis outages, traffic is rejected even if Postgres is healthy.
- Requires careful durability, replication, and eviction configuration.

### Alternative We Did Not Choose
Accept writes even if Redis is down and rely on DB-side deduplication plus background reconciliation.

Why we rejected it:
- Increases write amplification in the OLTP database.
- Weakens client-visible correctness semantics.
- Introduces more complex reconciliation logic.
