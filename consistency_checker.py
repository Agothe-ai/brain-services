#!/usr/bin/env python3
"""
L29 - Logical Consistency service.

Performs a lightweight contradiction scan over the current brain status and
recent summaries.
"""

from __future__ import annotations

import threading
import time

from flask import Flask, jsonify

from agothe_runtime_support import (
    load_brain_status,
    load_session_summaries,
    log_kairos,
    now_iso,
    publish_event,
    save_brain_status,
)

HOST = "127.0.0.1"
PORT = 5575
POLL_SECONDS = 60

app = Flask(__name__)

_state = {
    "service": "consistency_checker",
    "status": "starting",
    "updated_at": None,
    "findings": [],
}
_lock = threading.Lock()
_last_signature: tuple | None = None


def _scan_findings(status: dict) -> list[dict]:
    findings: list[dict] = []
    services = status.get("services", {})
    body_services = status.get("body_state", {}).get("services", {})

    for name, online in services.items():
        body_online = body_services.get(name, {}).get("online")
        if body_online is not None and body_online != online:
            findings.append({
                "type": "service_state_mismatch",
                "service": name,
                "services_value": online,
                "body_state_value": body_online,
            })

    summaries = load_session_summaries(3)
    normalized = [item.get("summary", "").lower() for item in summaries if item.get("summary")]
    for index, item in enumerate(normalized):
        if "offline" in item and "online" in item:
            findings.append({
                "type": "summary_conflict",
                "summary_index": index,
                "summary": summaries[index].get("summary"),
            })

    return findings


def _run() -> None:
    global _last_signature

    while True:
        status = load_brain_status()
        findings = _scan_findings(status)
        status["consistency_checker"] = {
            "updated_at": now_iso(),
            "findings": findings,
            "finding_count": len(findings),
        }
        layer = status["layer_states"]["L29"]
        layer["status"] = "live"
        layer["updated_at"] = now_iso()
        layer["details"] = {"finding_count": len(findings)}
        save_brain_status(status, checkpoint=False)

        signature = tuple(sorted((item["type"], item.get("service", ""), item.get("summary_index", -1)) for item in findings))
        if signature != _last_signature:
            event = {
                "timestamp": now_iso(),
                "source": "consistency_checker",
                "event_type": "consistency_scan",
                "findings": findings,
                "salience": 0.85 if findings else 0.35,
            }
            publish_event(event)
            log_kairos("consistency_scan", event, "consistency_checker")
            _last_signature = signature

        with _lock:
            _state.update({
                "status": "ok",
                "updated_at": now_iso(),
                "findings": findings,
            })

        time.sleep(POLL_SECONDS)


@app.get("/health")
def health():
    with _lock:
        payload = dict(_state)
    return jsonify({
        "status": "ok",
        "service": payload["service"],
        "port": PORT,
        "finding_count": len(payload["findings"]),
        "updated_at": payload["updated_at"],
    }), 200


@app.get("/scan")
def scan():
    with _lock:
        payload = dict(_state)
    return jsonify(payload), 200


def main() -> None:
    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    app.run(host=HOST, port=PORT, debug=False)


if __name__ == "__main__":
    main()
