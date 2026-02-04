#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

import redis
import yaml

DEFAULT_CONFIG = os.environ.get("EVENT_WATCHER_CONFIG", "event_watcher.yaml")


def load_config(path: str) -> dict:
    if not os.path.exists(path):
        raise SystemExit(f"Config not found: {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def check_redis(r, stream: str) -> bool:
    try:
        r.ping()
    except Exception:
        return False
    try:
        r.xinfo_stream(stream)
        return True
    except Exception:
        # stream missing or not accessible
        return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    ap.add_argument("--strict", action="store_true", help="Fail if webhook log path is missing")
    args = ap.parse_args()

    cfg = load_config(args.config)
    watchers = cfg.get("watchers", [])
    if not watchers:
        raise SystemExit("No watchers defined in config")

    r = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    ok = True

    for w in watchers:
        source = w.get("source")
        name = w.get("name")
        if source == "redis_stream":
            stream = w.get("stream")
            if not stream:
                print(f"FAIL {name}: missing stream")
                ok = False
                continue
            if not check_redis(r, stream):
                print(f"FAIL {name}: redis/stream check failed ({stream})")
                ok = False
            else:
                print(f"OK {name}: redis_stream {stream}")
        elif source == "webhook":
            path = w.get("webhook_log_path", os.environ.get("EVENT_WATCHER_WEBHOOK_LOG", "webhook_events.jsonl"))
            if os.path.exists(path):
                print(f"OK {name}: webhook log {path}")
            else:
                msg = f"WARN {name}: webhook log missing ({path})"
                if args.strict:
                    print(msg.replace("WARN", "FAIL"))
                    ok = False
                else:
                    print(msg)
        else:
            print(f"WARN {name}: unknown source {source}")

    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
