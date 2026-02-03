#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from typing import Any, Dict

import yaml
import redis

from sources.redis_stream import ensure_group, read_group, ack
from utils import normalize_event, match_filter, render_template, utc_now

DEFAULT_CONFIG = os.environ.get("EVENT_WATCHER_CONFIG", "event_watcher.yaml")
DEFAULT_STATE = os.environ.get("EVENT_WATCHER_STATE", "event_watcher_state.json")
DEAD_LETTER = os.environ.get("EVENT_WATCHER_DEAD_LETTER", "dead_letter.jsonl")
OPENCLAW_SESSION_KEY = os.environ.get("OPENCLAW_SESSION_KEY")


def load_config(path: str) -> dict:
    if not os.path.exists(path):
        raise SystemExit(f"Config not found: {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {"cursors": {}, "attempts": {}}
    with open(path, "r") as f:
        return json.load(f)


def save_state(path: str, state: dict) -> None:
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    ap.add_argument("--state", default=DEFAULT_STATE)
    ap.add_argument("--poll-interval", type=float, default=1.0)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.config)
    watchers = cfg.get("watchers", [])
    if not watchers:
        raise SystemExit("No watchers defined in config")

    state = load_state(args.state)
    r = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))

    # ensure consumer groups
    for w in watchers:
        if w.get("source") != "redis_stream":
            continue
        ensure_group(r, w["stream"], w.get("group", "eventwatcher"))

    while True:
        any_work = False
        for w in watchers:
            if w.get("source") != "redis_stream":
                continue
            name = w.get("name")
            stream = w.get("stream")
            if not stream:
                continue

            group = w.get("group", "eventwatcher")
            consumer = w.get("consumer", "watcher-1")
            batch = int(w.get("batch_count", 10))
            block_ms = int(w.get("block_ms", 1000))

            # first drain pending backlog, then read new
            for use_pending in (True, False):
                resp = read_group(r, stream, group, consumer, count=batch, block_ms=block_ms, use_pending=use_pending)
                if not resp:
                    continue
                any_work = True

                _, entries = resp[0]
                for event_id, fields in entries:
                    event_id = event_id.decode() if isinstance(event_id, bytes) else event_id
                    payload_field = w.get("payloadField")
                    payload_encoding = w.get("payloadEncoding", "hash")
                    event = normalize_event(stream, event_id, fields, payload_field, payload_encoding)

                    # validate payload
                    if event.get("payload") is None:
                        append_dead_letter(event, "payload_parse_failed")
                        ack(r, stream, group, event_id)
                        continue

                    # filter
                    if not match_filter(event, w.get("filter")):
                        ack(r, stream, group, event_id)
                        continue

                    # dedupe using redis key
                    ttl = int(w.get("dedupe_ttl_seconds", 1800))
                    dedupe_key = f"eventwatcher:dedupe:{stream}:{event_id}"
                    if not r.set(dedupe_key, "1", nx=True, ex=ttl):
                        ack(r, stream, group, event_id)
                        continue

                    # deliver
                    wake = w.get("wake", {})
                    session_key = wake.get("session_key") or OPENCLAW_SESSION_KEY
                    if not session_key:
                        append_dead_letter(event, "missing_session_key")
                        ack(r, stream, group, event_id)
                        continue

                    message = render_template(wake.get("message_template", ""), event)
                    timeout = int(w.get("ack_timeout_seconds", 30))
                    ok = send_to_openclaw(session_key, message, timeout)

                    if ok:
                        ack(r, stream, group, event_id)
                        state["attempts"].pop(event_id, None)
                        save_state(args.state, state)
                    else:
                        # retry
                        attempts = state["attempts"].get(event_id, 0) + 1
                        state["attempts"][event_id] = attempts
                        retry = w.get("retry", {})
                        max_retry = int(retry.get("max", 3))
                        backoff = retry.get("backoff_seconds", [60, 300, 900])
                        if attempts >= max_retry:
                            append_dead_letter(event, "send_failed")
                            ack(r, stream, group, event_id)
                            state["attempts"].pop(event_id, None)
                            save_state(args.state, state)
                        else:
                            wait = backoff[min(attempts - 1, len(backoff) - 1)]
                            time.sleep(wait)

        if args.once:
            break
        if not any_work:
            time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
