#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
from typing import Any, Dict, List, Tuple

import yaml

from utils import render_template

DEFAULT_CONFIG = os.environ.get("EVENT_WATCHER_CONFIG", "event_watcher.yaml")
DEAD_LETTER = os.environ.get("EVENT_WATCHER_DEAD_LETTER", "dead_letter.jsonl")
OPENCLAW_SESSION_KEY = os.environ.get("OPENCLAW_SESSION_KEY")


def load_config(path: str) -> dict:
    if not os.path.exists(path):
        raise SystemExit(f"Config not found: {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def send_to_openclaw(session_key: str, message: str, timeout: int) -> bool:
    cmd = ["openclaw", "agent", "--session-id", session_key, "--message", message, "--timeout", str(timeout)]
    try:
        subprocess.run(cmd, check=True, timeout=timeout + 5)
        return True
    except Exception:
        return False


def load_dead_letter(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    entries: List[dict] = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception:
                continue
    return entries


def write_dead_letter(path: str, entries: List[dict]) -> None:
    with open(path, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


def watcher_key(w: dict) -> Tuple[str, str]:
    source = w.get("source")
    if source == "redis_stream":
        return ("redis_stream", w.get("stream"))
    if source == "webhook":
        return ("webhook", w.get("topic", "webhook"))
    return (source, None)


def find_watcher(watchers: List[dict], event: dict) -> dict | None:
    source = event.get("source")
    topic = event.get("topic")
    for w in watchers:
        w_source, w_topic = watcher_key(w)
        if source == w_source and topic == w_topic:
            return w
    return None


def match_filters(entry: dict, args) -> bool:
    if args.reason and entry.get("reason") != args.reason:
        return False
    payload = entry.get("payload") or {}
    if args.source and payload.get("source") != args.source:
        return False
    if args.topic and payload.get("topic") != args.topic:
        return False
    return True


def list_entries(entries: List[dict], args) -> None:
    count = 0
    for entry in entries:
        if not match_filters(entry, args):
            continue
        payload = entry.get("payload") or {}
        print(f"{entry.get('event_id')}: reason={entry.get('reason')} source={payload.get('source')} topic={payload.get('topic')}")
        count += 1
        if args.limit and count >= args.limit:
            break
    print(f"Total: {count}")


def replay_entries(entries: List[dict], watchers: List[dict], args) -> None:
    kept: List[dict] = []
    replayed = 0
    failed = 0
    skipped = 0

    for idx, entry in enumerate(entries):
        if not match_filters(entry, args):
            kept.append(entry)
            continue

        payload = entry.get("payload")
        if not payload:
            skipped += 1
            kept.append(entry)
            continue

        watcher = find_watcher(watchers, payload)
        if not watcher:
            skipped += 1
            kept.append(entry)
            continue

        wake = watcher.get("wake", {})
        session_key = args.session_key or wake.get("session_key") or OPENCLAW_SESSION_KEY
        if not session_key:
            skipped += 1
            kept.append(entry)
            continue

        message = render_template(wake.get("message_template", ""), payload)
        timeout = int(watcher.get("ack_timeout_seconds", 30))

        if args.dry_run:
            print(f"DRY RUN: {entry.get('event_id')} -> {session_key}")
            replayed += 1
            kept.append(entry)
        else:
            ok = send_to_openclaw(session_key, message, timeout)
            if ok:
                replayed += 1
                if args.keep:
                    kept.append(entry)
            else:
                failed += 1
                kept.append(entry)

        if args.limit and replayed >= args.limit:
            kept.extend(entries[idx + 1:])
            break

    if not args.dry_run and not args.keep:
        write_dead_letter(args.path, kept)

    print(f"Replayed: {replayed} | Failed: {failed} | Skipped: {skipped}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    ap.add_argument("--path", default=DEAD_LETTER)
    sub = ap.add_subparsers(dest="cmd", required=True)

    ls = sub.add_parser("list")
    ls.add_argument("--reason")
    ls.add_argument("--source")
    ls.add_argument("--topic")
    ls.add_argument("--limit", type=int)

    replay = sub.add_parser("replay")
    replay.add_argument("--reason")
    replay.add_argument("--source")
    replay.add_argument("--topic")
    replay.add_argument("--limit", type=int)
    replay.add_argument("--dry-run", action="store_true")
    replay.add_argument("--keep", action="store_true", help="Keep entries in dead-letter after successful replay")
    replay.add_argument("--session-key", help="Override session key for replayed events")

    args = ap.parse_args()
    entries = load_dead_letter(args.path)

    if args.cmd == "list":
        list_entries(entries, args)
        return

    cfg = load_config(args.config)
    watchers = cfg.get("watchers", [])
    if not watchers:
        raise SystemExit("No watchers defined in config")

    if args.cmd == "replay":
        replay_entries(entries, watchers, args)


if __name__ == "__main__":
    main()
