# Osprey Velocity Counter Plugin — Build Instructions

## Project Goal
Build an open-source velocity / behavioral counting plugin for Osprey (part of the ROOST open-source trust & safety ecosystem). The plugin lets SML rules query time-windowed counts (e.g. "messages in the last 10 seconds") without Osprey having native support for arbitrary sliding windows.

This is a learning project. The contributor is an experienced engineering manager / tester (not a daily coder) using AI-assisted development. Prefer clarity, alignment with existing Osprey patterns, and incremental progress over perfection.

## Core Design Decisions (already locked in)
- **Storage**: Redis as shared state across workers.
- **Key model**: One key per (entity type, entity ID, behavior name). Key shape: `{entity_type}:{entity_id}:{behavior}`. Examples: `user:12345:message_sent`, `user:12345:login_attempt`, `ip:10.0.0.1:request`. "One key per entity" was shorthand — the real grain is one key per entity-plus-behavior pair. A single undifferentiated stream of every event for an entity is too coarse for useful rules.
- **Multi-entity events**: A single Osprey action can carry multiple entities (user ID, IP, device fingerprint, etc.). Each is extracted via EntityJson and ticks its own key. One event can increment `user:42:message_sent` and `ip:10.0.0.1:request` simultaneously. This is where co-occurrence detection gets its power.
- **Ingestion**: Kafka consumer groups. Each partition assigned to one worker. At-least-once delivery is expected.
- **Dedup**: Redis SET NX with TTL per event ID to prevent double-counting on rebalances or retries. Combined with the counter update in one Lua script when possible.
- **Sliding window**: Sorted sets (ZADD with timestamp scores). Configurable cleanup strategy (raw integer config, default to lazy/opportunistic cleanup to stay aligned with Osprey's labels backend style).
- **Population split / API exemption**: Producers stamp a traffic-source designation in the event payload (e.g. API client vs UI user). EntityJson extracts it as a label or entity attribute. The plugin reads it before deciding which key or threshold applies — API/bulk traffic gets higher limits or exemption, keyed as `api-client:{key-id}:{behavior}` instead of `user:{id}:{behavior}`. This check must happen in the plugin itself, not only in SML rules, so a forgotten rule check cannot poison counter data. Thresholds only apply to the population they are meant for.
- **Plugin architecture**: Python package using setuptools entry points / pluggy. Registers as a UDF provider or always-on service that ticks on ingest (upstream of rules).
- **Failure mode**: Fail-open with alerting.
- **Config style**: Raw environment variables with sane defaults (match existing Osprey Config singleton pattern). Design the storage interface so a future backend swap is possible.
- **Formal methods**: Start with a TLA+ spec for the core invariants (no double-counted event IDs, window correctness, cleanup never deletes active ticks, thresholds only apply to the intended population). Use TLC to model-check. Spec is a design artifact + optional CI gate, not a hard requirement for the first PR.
- **Hot keys**: Known failure mode — one entity's key can dominate Redis throughput. No built-in detection in v1; document the failure mode and add sampling metrics later if latency creep appears. Key splitting is the likely default fix if needed.

## Future Features (not v1)
- **HyperLogLog distinct-count counters**: Velocity counts repetitions of the same behavior; HyperLogLog counts variety in an associated field (e.g. how many distinct IPs a user has logged in from). It never stores items — it hashes each value, tracks the longest run of leading zeros across buckets, and estimates cardinality from that with roughly 0.8% error at 16,384 buckets. Memory is fixed regardless of volume, versus an ever-growing Redis set. Revisit once the sliding-window velocity path is proven; not needed for the core plugin.

## Audit, Legal Hold & Retention Layer

### Snapshot on rule fire
When a rule trips, the plugin captures a **snapshot** of the counter's state at that exact moment — not a live pointer. The counter keeps ticking after the violation, so the investigator needs the frozen evidence, not post-incident noise.

Three Postgres tables:
1. **Snapshot header** — when it fired, which rule, which entity, which key.
2. **Ticks** — each row is one event ID and timestamp copied from the sorted set at capture time. This is the evidence. Index on entity and timestamp from day one; this table grows fast.
3. **Hold** — entity type, entity ID, who placed it, when, status (open / confirmed / cleared), when released. Keyed on entity, not individual counter keys, because an investigation rarely cares about one behavior — it wants everything that entity touched across all behaviors.

Order of operations on rule fire: read the full sorted set from Redis, write the header, write all ticks, place the hold. One burst before the next event moves the counter.

### Automated action is optional
Snapshot and hold are independent of enforcement. A rule can snapshot and hold with zero auto-action (flag for review only), auto-label or block with no snapshot (team trusts the rule), or do both — the common production setup: enforce immediately, preserve evidence for the appeal or audit that might come later. The hold exists precisely because auto-actions get wrong sometimes.

### Manual hold creation
Analysts must be able to place a hold before any threshold is crossed — on a hunch or a tip. Provide an explicit hold-creation API (UI or API call), separate from the rule-fire path.

### Disposition and deletion
After investigation: **confirmed** means keep per the org's retention schedule; **cleared** means delete the Postgres rows and lift the Redis hold. Actual retention periods, grace windows, and default dispositions are configurable via environment variables (same pattern as cleanup interval) — legal guidance varies by organization, so never hardcode them. Cleared is not instant: a grace window lets an appeal or second reviewer still pull the data before deletion.

### Soft delete
Deletion is soft first — mark rows as purged with a timestamp rather than physically removing them, in case legal later asks you to prove destruction. The durable record kept after purge is only the deletion log: who, when, what was destroyed.

### Legal hold mechanics
The hold flag is stored in Postgres as the source of truth (a legal hold is exactly the kind of thing you cannot afford to lose), with a cached copy in Redis for the fast cleanup-check path. Cleanup logic checks the flag before removing old ticks from Redis keys matching a held entity.

## Recommended Phased Plan
1. **TLA+ Spec** — Model the counter as a state machine under the entity-behavior key model. Invariants: no duplicate event IDs, ticks outside the window are eventually removed, claim-and-increment is atomic, thresholds only apply to the intended population. Run TLC until clean.
2. **Redis Lua script** — Implement exactly what the spec proved (dedup + sliding-window ZADD + optional cleanup + population-aware key selection). Keep it short and dumb.
3. **Osprey plugin packaging** — setup.py / pyproject.toml with entry point, register hook, and a thin Python wrapper that calls the Lua script. Mirror Osprey's pluggable label provider pattern.
4. **Audit layer** — Postgres snapshot/hold/disposition tables, soft delete, configurable retention, explicit hold API.
5. **Integration testing** — Run Osprey's Docker Compose demo locally. Feed synthetic events, assert counter behavior, verify fail-open + alerting paths, and test snapshot/hold/disposition flows.
6. **Benchmark (Redis vs Postgres)** — Controlled load test justifying the storage choice. Spin up Redis and Postgres instances, generate synthetic traffic at one thousand to ten thousand events per second, each event performing the dedup-plus-tick operation. For Postgres, simulate the row-lock pattern with SELECT FOR UPDATE or advisory locks. Measure p50, p95, p99 latency and sustained throughput until something breaks. Use redis-benchmark for Redis, pgbench or a small Python harness with asyncio for Postgres. Output: one chart of latency versus load for each store. This chart is the evidence in the PR that turns "Postgres is too slow" from an assertion into a measured argument. Include the methodology and raw numbers in the PR description.
7. **Draft PR** — Open early (skeleton + clear description is better than waiting for polish). Include the TLA+ spec as a design document with a plain-English summary of invariants, plus the benchmark chart.

## Constraints & Style
- Stay aligned with existing Osprey/ROOST patterns (lazy expiration, raw config values, pluggy hooks).
- Prefer simple, configurable behavior over premature optimization.
- Document assumptions and trade-offs in the PR description.
- Open a draft PR as soon as the skeleton exists so design feedback can arrive early.
- Never hardcode retention periods, grace windows, or dispositions — always configurable.

## Starting Point
Begin with Phase 1 (TLA+ spec) under the entity-behavior key model. Once the invariants are proven, move to the Lua implementation. Do not jump to full plugin packaging until the core counting logic is solid and matches the spec. The benchmark (Phase 6) can run in parallel with Phases 3–5 since it only needs the operation pattern, not the full plugin.

Repo: https://github.com/ZachTheZebra/osprey-velocity-counter
