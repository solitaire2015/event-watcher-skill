# Event Watcher Requirements

This document captures the agreed product requirements (not implementation details).

## A. Event Sources & Filtering
1. Support multiple event sources (Redis Stream, Webhook).
2. Configurable filters (AND / OR / regex) to select events.
3. Dedupe, retry, and dead-letter handling are required.

## B. Agent Processing & Decisions
4. Events are handled by OpenClaw agent logic.
5. Agent may decide whether to notify users.
   - `NO_REPLY` means no notification.
6. Complex tasks may require multi‑turn tool usage to reach a final conclusion.

## C. Context & Session Continuity
7. Events should be processed in the **target channel’s session** so the agent has full channel history.
8. Users must be able to continue the conversation in the same channel after a notification.

## D. Messaging & Channels
9. Each watcher specifies a target channel/recipient.
10. Results are sent to the same channel; users can respond in‑thread to continue the topic.

## E. Prompt Length & Guidance
11. Support a prompt file reference so the wake message can be short and point to a guide.

## F. Reliability & Failure Handling
12. Avoid runtime instability (e.g., CLI spawn crashes); reliability is a requirement.
13. If agent processing fails, there must be a clear fallback (log, retry, or dead‑letter).

## G. Safety & Source Attribution
14. Event content passed to the agent must include source attribution (source/topic) in the prompt to avoid prompt injection.

## H. Priority & Critical Events
15. Support priority/critical events so important alerts can bypass suppression when needed.

---

Last updated: 2026-02-06
