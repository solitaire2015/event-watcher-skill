import json
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.utils import normalize_event


def test_payload_json():
    fields = {b"payload": json.dumps({"type": "rain"}).encode()}
    event = normalize_event("stream", "1-1", fields, "payload", "json")
    assert event["payload"]["type"] == "rain"


def test_payload_hash():
    fields = {b"type": b"sunny"}
    event = normalize_event("stream", "1-1", fields, None, "hash")
    assert event["payload"]["type"] == "sunny"
