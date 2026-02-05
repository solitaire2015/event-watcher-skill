import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.utils import render_template


def test_nested_template():
    event = {
        "event_id": "1",
        "payload": {"city": "beijing", "temp": 5},
    }
    out = render_template("City={{payload.city}} Temp={{payload.temp}}", event)
    assert out == "City=beijing Temp=5"


def test_unknown_path():
    event = {"event_id": "1", "payload": {"city": "beijing"}}
    out = render_template("X={{payload.zip}}", event)
    assert out == "X="
