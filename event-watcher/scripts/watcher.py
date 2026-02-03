#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Dict

import yaml
import redis

DEFAULT_CONFIG = os.environ.get("EVENT_WATCHER_CONFIG", "event_watcher.yaml")
DEFAULT_STATE = os.environ.get("EVENT_WATCHER_STATE", "event_watcher_state.json")
DEAD_LETTER = os.environ.get("EVENT_WATCHER_DEAD_LETTER", "dead_letter.jsonl")
OPENCLAW_SESSION_KEY = os.environ.get("OPENCLAW_SESSION_KEY")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def normalize_event(stream: str, event_id: str, fields: Dict[str, Any]) -> dict:
    # decode bytes to str
    payload = {k.decode() if isinstance(k, bytes) else k: (v.decode() if isinstance(v, bytes) else v) for k, v in fields.items()}
    return {
        "event_id": event_id,
        "source": "redis_stream",
        "topic": stream,
        "timestamp": utc_now(),
        "payload": payload,
    }


def get_field(data: dict, path: str) -> Any:
    cur = data
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def match_filter(event: dict, rule: dict | None) -> bool:
    if not rule:
        return True
    field = rule.get("field")
    op = rule.get("op")
    value = rule.get("value")
    actual = get_field(event, field) if field else None
    if op == "==":
        return actual == value
    if op == "!=":
        return actual != value
    if op == ">":
        return actual is not None and actual > value
    if op == "<":
        return actual is not None and actual < value
    if op == "in":
        return actual in value if isinstance(value, (list, tuple, set)) else False
    if op == "contains":
        return value in actual if isinstance(actual, (list, str)) else False
    return False


def render_template(template: str, event: dict) -> str:
    if not template:
        return f"New event: {event['event_id']}"
    out = template
    for key in ["event_id", "source", "topic", "timestamp", "payload"]:
        out = out.replace(f"{{{{{key}}}}}", str(event.get(key)))
    return out


def send_to_openclaw(session_key: str, message: str, timeout: int) -> bool:
    # Use openclaw CLI to send an agent turn to a specific session id
    cmd = ["openclaw", "agent", "--session-id", session_key, "--message", message, "--timeout", str(timeout)]
    try:
        subprocess.run(cmd, check=True, timeout=timeout + 5)
        return True
    except Exception:
        return False


def append_dead_letter(event: dict, reason: str) -> None:
    entry = {
        "event_id": event.get("event_id"),
        "reason": reason,
        "last_attempt": utc_now(),
        "payload": event,
    }
    with open(DEAD_LETTER, "a") as f:
        f.write(json.dumps(entry) + "\n")


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

    while True:
        any_work = False
        for w in watchers:
            name = w.get("name")
            stream = w.get("stream")
            if not stream:
                continue

            last_id = state["cursors"].get(name, "0-0")
            resp = r.xread({stream: last_id}, count=1, block=1000)
            if not resp:
                continue
            any_work = True

            _, entries = resp[0]
            for event_id, fields in entries:
                event_id = event_id.decode() if isinstance(event_id, bytes) else event_id
                event = normalize_event(stream, event_id, fields)

                # filter
                if not match_filter(event, w.get("filter")):
                    state["cursors"][name] = event_id
                    save_state(args.state, state)
                    continue

                # dedupe using redis key
                ttl = int(w.get("dedupe_ttl_seconds", 1800))
                dedupe_key = f"eventwatcher:dedupe:{stream}:{event_id}"
                if not r.set(dedupe_key, "1", nx=True, ex=ttl):
                    state["cursors"][name] = event_id
                    save_state(args.state, state)
                    continue

                # deliver
                wake = w.get("wake", {})
                session_key = wake.get("session_key") or OPENCLAW_SESSION_KEY
                if not session_key:
                    append_dead_letter(event, "missing_session_key")
                    state["cursors"][name] = event_id
                    save_state(args.state, state)
                    continue

                message = render_template(wake.get("message_template", ""), event)
                timeout = int(w.get("ack_timeout_seconds", 30))
                ok = send_to_openclaw(session_key, message, timeout)

                if ok:
                    state["cursors"][name] = event_id
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
                        state["cursors"][name] = event_id
                        state["attempts"].pop(event_id, None)
                        save_state(args.state, state)
                    else:
                        # sleep backoff for this watcher
                        wait = backoff[min(attempts - 1, len(backoff) - 1)]
                        time.sleep(wait)

        if args.once:
            break
        if not any_work:
            time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
