# Event Watcher Roadmap

This roadmap is a prioritized list of improvements for the Event Watcher skill.

## P0 — Reliability & Correctness (must-have)
✅ Completed (2026-02-03)

## P1 — Filtering & Observability
✅ Completed (2026-02-03)

## P2 — Delivery & Ops
7. **Rate Limiting / Grouping**
   - Aggregate bursts into one notification
8. **Healthcheck + Graceful Shutdown**
   - Redis connectivity + stream existence checks
   - SIGTERM flush state
9. **Dead-letter Replay Tooling**
   - Inspect and replay failed events

## P3 — Convenience
10. **Natural-language → Filter Rules Helper**
    - Optional agent tool to convert NL into JSON rules
11. **pm2 ecosystem.config.js**
    - Standardized launch config

## Done
- P0: Redis consumer groups + batch reads + payload parsing
- P0: Multi-source framework (redis_stream + webhook) + webhook source
- P1: Advanced filter rules (AND/OR/regex)
- P1: Structured logs + counters
