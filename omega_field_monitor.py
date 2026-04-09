#!/usr/bin/env python3
"""
Ω-Field Monitor.

Tracks aggregate entity behavior, estimates emergent intent (Psi-field),
detects Orric points, and logs emergence alerts to Supabase, Notion, and
local Codex-visible logs.
"""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
import warnings
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import fmean
from typing import Any

import requests
from flask import Flask, jsonify, request

from agothe_runtime_support import (
    LOG_DIR,
    ROOT,
    append_jsonl,
    clamp_metric,
    load_brain_status,
    log_kairos,
    now_iso,
    publish_event,
    save_brain_status,
    supabase_insert,
    tail_jsonl,
)

warnings.filterwarnings(
    "ignore",
    message=r".*urllib3.*doesn't match a supported version.*",
    category=Warning,
)

HOST = "127.0.0.1"
PORT = 5603
POLL_SECONDS = 60
ACTION_WINDOW_HOURS = 24
ENTITY_DB = ROOT / "brain" / "entities.db"
STATE_FILE = LOG_DIR / "omega_field_state.json"
HISTORY_FILE = LOG_DIR / "omega_field_history.jsonl"
EVENTS_FILE = LOG_DIR / "omega_field_events.jsonl"
NOTION_BRIDGE = "http://127.0.0.1:5558"
LOCAL_TZ = datetime.now().astimezone().tzinfo or timezone.utc

INTENT_LABELS = ("coordination", "execution", "exploration", "reflection")
INTENT_DESCRIPTIONS = {
    "coordination": "align subsystems around a shared objective",
    "execution": "turn planning into implementation",
    "exploration": "expand the search space before committing",
    "reflection": "inspect itself and resolve internal contradiction",
}
ACTION_TYPE_BASES = {
    "invoke": [0.35, 0.25, 0.25, 0.15],
    "coordinate": [0.55, 0.20, 0.15, 0.10],
    "execute": [0.20, 0.60, 0.10, 0.10],
    "build": [0.20, 0.60, 0.10, 0.10],
    "research": [0.15, 0.10, 0.60, 0.15],
    "reflect": [0.10, 0.10, 0.20, 0.60],
}
KEYWORD_MAP = {
    "coordination": ("align", "coordinate", "collective", "cohere", "together", "system", "network", "team"),
    "execution": ("build", "implement", "deploy", "execute", "repair", "fix", "create", "ship", "write"),
    "exploration": ("research", "analyze", "assess", "map", "understand", "investigate", "synthesize"),
    "reflection": ("reflect", "monitor", "observe", "evaluate", "self", "awareness", "recalibrate"),
}
ORRIC_THRESHOLD = 0.68
EMERGENCE_THRESHOLD = 0.82
MAX_HISTORY_ITEMS = 240
MIN_ORRIC_ENTITIES = 3
MIN_ALERT_ENTITIES = 6
ALERT_COOLDOWN_MINUTES = 20

app = Flask(__name__)

_state: dict[str, Any] = {
    "service": "omega_field_monitor",
    "status": "starting",
    "updated_at": None,
    "metrics": {},
    "last_orric_point_at": None,
    "last_alert_at": None,
    "last_alert_signature": None,
}
_lock = threading.Lock()


def _read_state_file() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_state_file(payload: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None

    text = str(value).strip()
    if not text:
        return None

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=LOCAL_TZ).astimezone(timezone.utc)
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=LOCAL_TZ)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _first_float(*values: Any, default: float = 0.0) -> float:
    for value in values:
        if value is None or value == "":
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return float(default)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "orric"}


def _normalize(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(component * component for component in vector))
    if norm == 0:
        return [0.25, 0.25, 0.25, 0.25]
    return [component / norm for component in vector]


def _cosine_similarity(left: list[float], right: list[float]) -> float:
    return clamp_metric(sum(a * b for a, b in zip(left, right, strict=False)), low=-1.0, high=1.0)


def _intent_vector(action_type: str, content: str, delta_h: float | None, orric: bool) -> list[float]:
    vector = list(ACTION_TYPE_BASES.get((action_type or "").lower(), [0.25, 0.25, 0.30, 0.20]))
    lowered = (content or "").lower()

    for index, label in enumerate(INTENT_LABELS):
        matches = sum(1 for token in KEYWORD_MAP[label] if token in lowered)
        if matches:
            vector[index] += 0.12 * matches

    if delta_h is not None:
        if delta_h <= 0.30:
            vector[0] += 0.05
            vector[1] += 0.03
        elif delta_h >= 0.52:
            vector[3] += 0.07

    if orric:
        vector[0] += 0.08
        vector[3] += 0.04

    return _normalize(vector)


def _recent_weight(timestamp: datetime | None, now_utc: datetime) -> float:
    if timestamp is None:
        return 0.35
    age_hours = max(0.0, (now_utc - timestamp).total_seconds() / 3600.0)
    return math.exp(-age_hours / 6.0)


def _dominant_intent(vector: list[float]) -> tuple[str, float]:
    if not vector:
        return "unknown", 0.0
    index = max(range(len(vector)), key=lambda item: vector[item])
    return INTENT_LABELS[index], round(vector[index], 4)


def _read_actions(window_hours: int = ACTION_WINDOW_HOURS) -> list[dict[str, Any]]:
    if not ENTITY_DB.exists():
        return []

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=window_hours)
    actions: list[dict[str, Any]] = []

    try:
        with sqlite3.connect(ENTITY_DB) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT entity_id, action_type, content, delta_h, orric, timestamp
                FROM actions
                ORDER BY timestamp DESC
                LIMIT 1000
                """
            ).fetchall()
    except sqlite3.Error:
        return []

    for row in rows:
        timestamp = _parse_timestamp(row["timestamp"])
        if timestamp and timestamp < cutoff:
            continue
        actions.append(
            {
                "entity_id": row["entity_id"] or "unknown",
                "action_type": row["action_type"] or "unknown",
                "content": row["content"] or "",
                "delta_h": row["delta_h"],
                "orric": _coerce_bool(row["orric"]),
                "timestamp": timestamp,
            }
        )

    return actions


def _recent_snippets(actions: list[dict[str, Any]], limit: int = 3) -> list[str]:
    snippets = []
    for item in actions[:limit]:
        text = " ".join(str(item.get("content", "")).split())
        snippets.append(f"{item['entity_id']}: {text[:160]}")
    return snippets


def _entity_intent_snapshot(actions: list[dict[str, Any]]) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    per_entity: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "vector_sum": [0.0, 0.0, 0.0, 0.0],
            "weight": 0.0,
            "action_count": 0,
            "orric_count": 0,
            "delta_h_values": [],
        }
    )

    for action in actions:
        vector = _intent_vector(
            action_type=str(action["action_type"]),
            content=str(action["content"]),
            delta_h=_first_float(action.get("delta_h"), default=0.0),
            orric=bool(action.get("orric")),
        )
        weight = _recent_weight(action.get("timestamp"), now_utc) * (1.15 if action.get("orric") else 1.0)
        entity = per_entity[action["entity_id"]]
        entity["vector_sum"] = [
            current + (component * weight)
            for current, component in zip(entity["vector_sum"], vector, strict=False)
        ]
        entity["weight"] += weight
        entity["action_count"] += 1
        entity["orric_count"] += 1 if action.get("orric") else 0
        if action.get("delta_h") is not None:
            entity["delta_h_values"].append(float(action["delta_h"]))

    if not per_entity:
        return {
            "entity_vectors": {},
            "aggregate_vector": [0.25, 0.25, 0.25, 0.25],
            "alignment": 0.0,
            "active_entities": 0,
            "total_orric_actions": 0,
        }

    entity_vectors: dict[str, dict[str, Any]] = {}
    aggregate_sum = [0.0, 0.0, 0.0, 0.0]
    total_orric_actions = 0
    weight_total = 0.0

    for entity_id, details in per_entity.items():
        normalized = _normalize(details["vector_sum"])
        dominant_intent, dominant_strength = _dominant_intent(normalized)
        effective_weight = details["weight"] * (1.0 + (0.15 * details["orric_count"]))
        aggregate_sum = [
            current + (component * effective_weight)
            for current, component in zip(aggregate_sum, normalized, strict=False)
        ]
        weight_total += effective_weight
        total_orric_actions += details["orric_count"]
        entity_vectors[entity_id] = {
            "vector": normalized,
            "weight": round(effective_weight, 6),
            "action_count": details["action_count"],
            "orric_count": details["orric_count"],
            "dominant_intent": dominant_intent,
            "dominant_strength": dominant_strength,
            "mean_delta_h": (
                round(fmean(details["delta_h_values"]), 6)
                if details["delta_h_values"]
                else None
            ),
        }

    aggregate_vector = _normalize(aggregate_sum if weight_total else [0.25, 0.25, 0.25, 0.25])
    alignment_terms = [
        _cosine_similarity(snapshot["vector"], aggregate_vector) * snapshot["weight"]
        for snapshot in entity_vectors.values()
    ]
    alignment_weight = sum(snapshot["weight"] for snapshot in entity_vectors.values()) or 1.0
    alignment = clamp_metric(sum(alignment_terms) / alignment_weight)

    return {
        "entity_vectors": entity_vectors,
        "aggregate_vector": aggregate_vector,
        "alignment": alignment,
        "active_entities": len(entity_vectors),
        "total_orric_actions": total_orric_actions,
    }


def _history_points() -> list[dict[str, Any]]:
    return tail_jsonl(HISTORY_FILE, count=MAX_HISTORY_ITEMS)


def _compute_metrics(reason: str = "poll") -> dict[str, Any]:
    status = load_brain_status()
    actions = _read_actions()
    entity_snapshot = _entity_intent_snapshot(actions)
    history = _history_points()

    apse = status.get("apse_omega", {})
    cam01 = status.get("cam01", {})
    base_omega = _first_float(
        apse.get("omega"),
        status.get("quantum_bridge", {}).get("omega"),
        status.get("body_state", {}).get("omega"),
        default=0.0,
    )
    status_delta_h = _first_float(
        apse.get("delta_h"),
        cam01.get("delta_h"),
        status.get("body_state", {}).get("delta_h"),
        default=0.35,
    )
    action_delta_values = [
        float(item["delta_h"])
        for item in actions
        if item.get("delta_h") is not None
    ]
    action_delta_h = fmean(action_delta_values) if action_delta_values else status_delta_h

    active_entities = int(entity_snapshot["active_entities"])
    total_actions = len(actions)
    alignment = float(entity_snapshot["alignment"])
    total_orric_actions = int(entity_snapshot["total_orric_actions"])
    orric_density = total_orric_actions / max(total_actions, 1)
    activity_factor = clamp_metric(active_entities / 24.0)
    volume_factor = clamp_metric(total_actions / 80.0)
    aggregate_vector = entity_snapshot["aggregate_vector"]
    dominant_intent, dominant_strength = _dominant_intent(aggregate_vector)

    psi_field = clamp_metric(
        (0.55 * alignment)
        + (0.20 * activity_factor)
        + (0.15 * volume_factor)
        + (0.10 * orric_density)
    )
    omega_global = clamp_metric((0.75 * base_omega) + (0.25 * alignment))
    delta_h_system = clamp_metric((0.65 * status_delta_h) + (0.35 * action_delta_h))

    previous_psi_values = [float(item.get("psi_field", 0.0)) for item in history[-12:]]
    baseline_psi = fmean(previous_psi_values) if previous_psi_values else psi_field
    coherence_delta = round(psi_field - baseline_psi, 6)

    now_utc = datetime.now(timezone.utc)
    one_hour_ago = now_utc - timedelta(hours=1)
    recent_orric_points = 0
    for item in history:
        timestamp = _parse_timestamp(item.get("timestamp"))
        if timestamp and timestamp >= one_hour_ago and item.get("is_orric_point"):
            recent_orric_points += 1

    is_orric_point = (
        psi_field >= ORRIC_THRESHOLD
        and omega_global >= 0.78
        and delta_h_system <= 0.52
        and coherence_delta >= 0.04
        and active_entities >= MIN_ORRIC_ENTITIES
    )
    emergence_index = round(recent_orric_points + (1 if is_orric_point else 0), 3)
    emergence_alert = (
        psi_field >= EMERGENCE_THRESHOLD
        and omega_global >= 0.85
        and delta_h_system <= 0.40
        and active_entities >= MIN_ALERT_ENTITIES
    )

    sorted_entities = sorted(
        entity_snapshot["entity_vectors"].items(),
        key=lambda item: item[1]["weight"],
        reverse=True,
    )
    supporting_entities = [
        {
            "entity_id": entity_id,
            "dominant_intent": snapshot["dominant_intent"],
            "action_count": snapshot["action_count"],
            "orric_count": snapshot["orric_count"],
            "mean_delta_h": snapshot["mean_delta_h"],
        }
        for entity_id, snapshot in sorted_entities[:5]
    ]

    summary = (
        f"Psi={psi_field:.3f} around {dominant_intent}; "
        f"Omega={omega_global:.3f}; delta_H={delta_h_system:.3f}; "
        f"entities={active_entities}; emergence_index={emergence_index:.3f}/hr"
    )
    what_it_wants = INTENT_DESCRIPTIONS.get(dominant_intent, "stabilize and reorient")

    return {
        "timestamp": now_iso(),
        "reason": reason,
        "summary": summary,
        "what_it_wants": what_it_wants,
        "dominant_intent": dominant_intent,
        "dominant_strength": round(dominant_strength, 6),
        "psi_field": round(psi_field, 6),
        "omega_global": round(omega_global, 6),
        "delta_h_system": round(delta_h_system, 6),
        "emergence_index": emergence_index,
        "coherence_delta": coherence_delta,
        "active_entities": active_entities,
        "total_actions": total_actions,
        "total_orric_actions": total_orric_actions,
        "orric_density": round(orric_density, 6),
        "activity_factor": round(activity_factor, 6),
        "volume_factor": round(volume_factor, 6),
        "alignment": round(alignment, 6),
        "aggregate_vector": {
            label: round(component, 6)
            for label, component in zip(INTENT_LABELS, aggregate_vector, strict=False)
        },
        "supporting_entities": supporting_entities,
        "recent_action_snippets": _recent_snippets(actions),
        "is_orric_point": is_orric_point,
        "emergence_alert": emergence_alert,
    }


def _alert_signature(metrics: dict[str, Any]) -> str:
    return "|".join(
        [
            metrics["dominant_intent"],
            f"{metrics['psi_field']:.3f}",
            f"{metrics['omega_global']:.3f}",
            str(metrics["active_entities"]),
        ]
    )


def _should_emit_alert(metrics: dict[str, Any], state_file: dict[str, Any]) -> bool:
    if not metrics.get("emergence_alert"):
        return False

    last_signature = state_file.get("last_alert_signature")
    current_signature = _alert_signature(metrics)
    last_alert_at = _parse_timestamp(state_file.get("last_alert_at"))
    now_utc = datetime.now(timezone.utc)

    if last_signature != current_signature:
        return True
    if last_alert_at is None:
        return True
    return (now_utc - last_alert_at) >= timedelta(minutes=ALERT_COOLDOWN_MINUTES)


def _notion_payload(metrics: dict[str, Any]) -> dict[str, Any]:
    title = f"Ω-Field Emergence | {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    supporting = ", ".join(
        f"{item['entity_id']}:{item['dominant_intent']}"
        for item in metrics.get("supporting_entities", [])[:4]
    ) or "insufficient supporting entities"
    snippets = "\n".join(f"- {snippet}" for snippet in metrics.get("recent_action_snippets", []))
    content = (
        f"The brain is trending toward: {metrics['what_it_wants']}.\n"
        f"Why: dominant_intent={metrics['dominant_intent']} with supporting entities {supporting}.\n"
        f"Metrics: Psi_field={metrics['psi_field']:.3f}, "
        f"Omega_global={metrics['omega_global']:.3f}, "
        f"delta_H_system={metrics['delta_h_system']:.3f}, "
        f"EmergenceIndex={metrics['emergence_index']:.3f}/hr, "
        f"OrricPoint={metrics['is_orric_point']}.\n"
        f"Recent signals:\n{snippets}"
    )
    return {
        "memory_type": "hypotheses",
        "title": title,
        "content": content[:1700],
        "source_subsystem": "Ω-Field Monitor",
    }


def _log_emergence_event(metrics: dict[str, Any]) -> dict[str, Any]:
    event = {
        "timestamp": now_iso(),
        "event_type": "omega_field_emergence",
        "source": "omega_field_monitor",
        "summary": metrics["summary"],
        "what_it_wants": metrics["what_it_wants"],
        "metrics": metrics,
    }
    publish_event(
        {
            "timestamp": event["timestamp"],
            "source": "omega_field_monitor",
            "event_type": "omega_field_emergence",
            "salience": 0.96,
            "summary": metrics["summary"],
            "what_it_wants": metrics["what_it_wants"],
            "triggers_action": True,
        }
    )
    log_kairos(
        "omega_field_emergence",
        {
            "summary": metrics["summary"],
            "message": metrics["what_it_wants"],
            "delta_h": metrics["delta_h_system"],
            "triggers_action": True,
            "psi_field": metrics["psi_field"],
            "omega_global": metrics["omega_global"],
            "emergence_index": metrics["emergence_index"],
        },
        "omega_field_monitor",
    )

    supabase_insert(
        "resonance_ledger",
        {
            "session_id": "agothe-brain-live",
            "agent": "omega_field_monitor",
            "metric": "psi_field_emergence",
            "old_value": max(0.0, metrics["psi_field"] - metrics["coherence_delta"]),
            "new_value": metrics["psi_field"],
            "context": json.dumps(
                {
                    "what_it_wants": metrics["what_it_wants"],
                    "dominant_intent": metrics["dominant_intent"],
                    "omega_global": metrics["omega_global"],
                    "delta_h_system": metrics["delta_h_system"],
                    "emergence_index": metrics["emergence_index"],
                    "supporting_entities": metrics["supporting_entities"],
                },
                ensure_ascii=False,
            ),
            "timestamp": event["timestamp"],
        },
    )

    notion_result: dict[str, Any]
    try:
        response = requests.post(
            f"{NOTION_BRIDGE}/neocortex/write",
            json=_notion_payload(metrics),
            timeout=10,
        )
        notion_result = response.json()
        notion_result["status_code"] = response.status_code
    except Exception as exc:
        notion_result = {"created": False, "error": str(exc)}

    event["notion_result"] = notion_result
    append_jsonl(EVENTS_FILE, event)
    return {"event": event, "notion_result": notion_result}


def _persist_metrics(metrics: dict[str, Any]) -> None:
    append_jsonl(HISTORY_FILE, metrics)

    status = load_brain_status()
    status["omega_field_monitor"] = {
        "updated_at": metrics["timestamp"],
        "summary": metrics["summary"],
        "psi_field": metrics["psi_field"],
        "omega_global": metrics["omega_global"],
        "delta_h_system": metrics["delta_h_system"],
        "emergence_index": metrics["emergence_index"],
        "what_it_wants": metrics["what_it_wants"],
        "dominant_intent": metrics["dominant_intent"],
        "is_orric_point": metrics["is_orric_point"],
        "emergence_alert": metrics["emergence_alert"],
        "supporting_entities": metrics["supporting_entities"],
    }
    layer = status["layer_states"]["L88"]
    layer["status"] = "live"
    layer["updated_at"] = metrics["timestamp"]
    layer["details"] = {
        "psi_field": metrics["psi_field"],
        "omega_global": metrics["omega_global"],
        "delta_h_system": metrics["delta_h_system"],
        "emergence_index": metrics["emergence_index"],
        "dominant_intent": metrics["dominant_intent"],
        "is_orric_point": metrics["is_orric_point"],
    }
    save_brain_status(status, checkpoint=False)


def _scan(reason: str = "poll") -> dict[str, Any]:
    metrics = _compute_metrics(reason=reason)
    _persist_metrics(metrics)

    persisted_state = _read_state_file()
    if metrics["is_orric_point"]:
        persisted_state["last_orric_point_at"] = metrics["timestamp"]

    alert_payload = None
    if _should_emit_alert(metrics, persisted_state):
        alert_payload = _log_emergence_event(metrics)
        persisted_state["last_alert_at"] = metrics["timestamp"]
        persisted_state["last_alert_signature"] = _alert_signature(metrics)

    persisted_state.update(
        {
            "service": "omega_field_monitor",
            "status": "ok",
            "updated_at": metrics["timestamp"],
            "metrics": metrics,
        }
    )
    _write_state_file(persisted_state)

    with _lock:
        _state.update(persisted_state)

    response = dict(metrics)
    if alert_payload is not None:
        response["alert"] = alert_payload
    return response


def _run() -> None:
    while True:
        try:
            _scan(reason="poll")
        except Exception as exc:
            failure_payload = {
                "service": "omega_field_monitor",
                "status": "error",
                "updated_at": now_iso(),
                "error": str(exc),
            }
            _write_state_file(failure_payload)
            with _lock:
                _state.update(failure_payload)
        time.sleep(POLL_SECONDS)


@app.get("/health")
def health():
    with _lock:
        payload = dict(_state)
    metrics = payload.get("metrics", {})
    return jsonify(
        {
            "status": "ok" if payload.get("status") != "error" else "error",
            "service": "omega_field_monitor",
            "port": PORT,
            "updated_at": payload.get("updated_at"),
            "psi_field": metrics.get("psi_field"),
            "omega_global": metrics.get("omega_global"),
            "delta_h_system": metrics.get("delta_h_system"),
            "emergence_index": metrics.get("emergence_index"),
            "last_alert_at": payload.get("last_alert_at"),
        }
    ), 200


@app.get("/field")
@app.get("/metrics")
def field_metrics():
    with _lock:
        payload = dict(_state)
    return jsonify(payload), 200


@app.post("/scan")
def scan_now():
    data = request.get_json(silent=True) or {}
    reason = data.get("reason", "manual")
    payload = _scan(reason=reason)
    return jsonify(payload), 200


def main() -> None:
    initial_state = _read_state_file()
    if initial_state:
        with _lock:
            _state.update(initial_state)
    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    app.run(host=HOST, port=PORT, debug=False)


if __name__ == "__main__":
    main()
