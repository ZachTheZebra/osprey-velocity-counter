# Osprey Velocity Counter Plugin — Build Instructions

## Project Goal
Build an open-source velocity / behavioral counting plugin for Osprey (part of the ROOST open-source trust & safety ecosystem). The plugin lets SML rules query time-windowed counts (e.g. “messages in the last 10 seconds”) without Osprey having native support for arbitrary sliding windows.

This is a learning project. The contributor is an experienced engineering manager / tester (not a daily coder) using AI-assisted development. Prefer clarity, alignment with existing Osprey patterns, and incremental progress over perfection.

## Core Design Decisions (already locked in)
- **Storage**: Redis as shared state across workers. One key per entity (e.g. user ID).
- **Ingestion**: Kafka consumer groups. Each partition assigned to one worker. At-least-once delivery is expected.
- **Dedup**: Redis SET NX with TTL per event ID to prevent double-counting on rebalances or retries. Combined with the counter update in one Lua script when possible.
- **Sliding window**: Sorted sets (ZADD with timestamp scores). Configurable cleanup strategy (raw integer config, default to lazy/opportunistic cleanup to stay aligned with Osprey’s labels backend style).
- **Plugin architecture**: Python package using setuptools entry points / pluggy. Registers as a UDF provider or always-on service that ticks on ingest (upstream of rules).
- **Failure mode**: Fail-open with alerting.
- **Config style**: Raw environment variables with sane defaults (match existing Osprey Config singleton pattern). Design the storage interface so a future backend swap is possible.
- **Formal methods**: Start with a TLA+ spec for the core invariants (no double-counted event IDs, window correctness, cleanup never deletes active ticks). Use TLC to model-check. Spec is a design artifact + optional CI gate, not a hard requirement for the first PR.

## Recommended Phased Plan
1. **TLA+ Spec** — Model the counter as a state machine. Invariants: no duplicate event IDs, ticks outside the window are eventually removed, claim-and-increment is atomic. Run TLC until clean.
2. **Redis Lua script** — Implement exactly what the spec proved (dedup + sliding-window ZADD + optional cleanup). Keep it short and dumb.
3. **Osprey plugin packaging** — setup.py / pyproject.toml with entry point, register hook, and a thin Python wrapper that calls the Lua script. Mirror Osprey’s pluggable label provider pattern.
4. **Integration testing** — Run Osprey’s Docker Compose demo locally. Feed synthetic events, assert counter behavior, and verify fail-open + alerting paths.
5. **Draft PR** — Open early (skeleton + clear description is better than waiting for polish). Include the TLA+ spec as a design document with a plain-English summary of invariants.

## Constraints & Style
- Stay aligned with existing Osprey/ROOST patterns (lazy expiration, raw config values, pluggy hooks).
- Prefer simple, configurable behavior over premature optimization.
- Document assumptions and trade-offs in the PR description.
- Open a draft PR as soon as the skeleton exists so design feedback can arrive early.

## Starting Point
Begin with Phase 1 (TLA+ spec). Once the invariants are proven, move to the Lua implementation. Do not jump to full plugin packaging until the core counting logic is solid and matches the spec.

Repo: https://github.com/ZachTheZebra/osprey-velocity-counter
