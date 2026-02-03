# Event Watcher Issues

## P0
- [ ] Use Redis consumer groups (XGROUP/XREADGROUP) for at-least-once delivery
- [ ] Implement batch reads (count > 1) and backlog draining
- [ ] Add explicit payload parsing config (`payloadField`, `payloadEncoding`)
- [ ] Add multi-source adapter framework (redis_stream, sqs, kafka, webhook)

## P1
- [ ] Support AND/OR filter groups + regex operations
- [ ] Add structured logs + counters (received/matched/delivered/failed)

## P2
- [ ] Rate-limit / aggregate burst notifications
- [ ] Add healthcheck + graceful shutdown
- [ ] Dead-letter replay CLI

## P3
- [ ] Natural language → filter rules helper
- [ ] pm2 ecosystem.config.js generator
