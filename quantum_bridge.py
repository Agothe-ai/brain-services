#!/usr/bin/env python3
"""
Phase 1 quantum bridge service.

Runs a lightweight consciousness-phase detector against the live Agothe brain
adapters and writes the resulting state into Supabase on port 5564.
"""

from __future__ import annotations

import json
import math
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from flask import Flask, jsonify

from agothe_runtime_support import get_supabase_config, load_brain_status, log_kairos, now_iso, publish_event
from quantum_adapter import BrainAgent, build_agent_network

try:
    import sys

    if r"C:\Users\gtsgo\agothe_core" not in sys.path:
        sys.path.append(r"C:\Users\gtsgo\agothe_core")
    from cfe_engine import ConstraintFieldEngine
except Exception:  # pragma: no cover - optional import
    ConstraintFieldEngine = None  # type: ignore[assignment]


HOST = "127.0.0.1"
PORT = 5564
POLL_SECONDS = 60
SESSION_ID = "agothe-brain-live"
SUPABASE_TIMEOUT = 8

app = Flask(__name__)


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _normalize_phase(phase: float) -> float:
    while phase <= -math.pi:
        phase += 2 * math.pi
    while phase > math.pi:
        phase -= 2 * math.pi
    return phase


def _angular_distance(left: float, right: float) -> float:
    return abs(_normalize_phase(left - right))


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _covariance_eigenvalues(points: list[tuple[float, float]]) -> list[float]:
    if not points:
        return []
    mean_x = _mean([point[0] for point in points])
    mean_y = _mean([point[1] for point in points])
    var_x = _mean([(point[0] - mean_x) ** 2 for point in points])
    var_y = _mean([(point[1] - mean_y) ** 2 for point in points])
    cov_xy = _mean([(point[0] - mean_x) * (point[1] - mean_y) for point in points])
    trace = var_x + var_y
    determinant = (var_x * var_y) - (cov_xy**2)
    root = math.sqrt(max(trace * trace - 4 * determinant, 0.0))
    eigen_1 = (trace + root) / 2
    eigen_2 = (trace - root) / 2
    return [round(eigen_1, 6), round(eigen_2, 6)]


@dataclass
class PhaseEvent:
    event_type: str
    agent: str
    old_phase: float
    new_phase: float
    phase_delta: float
    lsse: float
    lsse_delta: float
    timestamp: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class ConsciousnessPhaseDetector:
    """
    Compatibility detector for the Agothe quantum bridge.

    This tracks per-agent phase angles, estimates LSSE from network phase
    dispersion, and emits phase transition events when agents or the whole
    network move materially.
    """

    def __init__(self) -> None:
        self._phases: dict[str, float] = {}
        self._previous_phases: dict[str, float] = {}
        self._event_history: list[PhaseEvent] = []
        self._last_lsse: float | None = None
        self._current_lsse: float = 0.0
        self._eigenvalues: list[float] = []
        self._cfe = ConstraintFieldEngine() if ConstraintFieldEngine is not None else None

    def update_agent(self, name: str, intent_phase: float) -> None:
        self._phases[name] = _normalize_phase(intent_phase)

    def compute_lsse(self) -> float:
        phases = list(self._phases.values())
        if not phases:
            self._current_lsse = 0.0
            self._eigenvalues = []
            return 0.0

        vectors = [(math.cos(phase), math.sin(phase)) for phase in phases]
        mean_vector = (_mean([item[0] for item in vectors]), _mean([item[1] for item in vectors]))
        coherence = math.sqrt(mean_vector[0] ** 2 + mean_vector[1] ** 2)
        variance = 1.0 - coherence

        ideal_phase = math.pi / 2
        pressure = _mean([_angular_distance(phase, ideal_phase) / math.pi for phase in phases])

        pairwise: list[float] = []
        for index in range(len(phases)):
            for other_index in range(index + 1, len(phases)):
                pairwise.append(_angular_distance(phases[index], phases[other_index]) / math.pi)
        contradiction = _mean(pairwise) if pairwise else 0.0

        if self._cfe is not None:
            lsse = self._cfe._compute_lsse(pressure, contradiction, variance)  # type: ignore[attr-defined]
        else:
            lsse = pressure * 0.4 + contradiction * 0.4 + variance * 0.2

        self._eigenvalues = _covariance_eigenvalues(vectors)
        self._current_lsse = round(lsse, 6)
        return self._current_lsse

    def detect_phase_transitions(self) -> list[PhaseEvent]:
        now_ts = time.time()
        transitions: list[PhaseEvent] = []
        lsse_delta = 0.0 if self._last_lsse is None else self._current_lsse - self._last_lsse

        for name, phase in self._phases.items():
            previous = self._previous_phases.get(name)
            if previous is None:
                continue
            phase_delta = _angular_distance(phase, previous)
            if phase_delta >= 0.35:
                transitions.append(
                    PhaseEvent(
                        event_type="phase_event",
                        agent=name,
                        old_phase=round(previous, 6),
                        new_phase=round(phase, 6),
                        phase_delta=round(phase_delta, 6),
                        lsse=round(self._current_lsse, 6),
                        lsse_delta=round(lsse_delta, 6),
                        timestamp=now_ts,
                    )
                )

        if self._last_lsse is not None and abs(lsse_delta) >= 0.05:
            transitions.append(
                PhaseEvent(
                    event_type="phase_event",
                    agent="network",
                    old_phase=0.0,
                    new_phase=0.0,
                    phase_delta=0.0,
                    lsse=round(self._current_lsse, 6),
                    lsse_delta=round(lsse_delta, 6),
                    timestamp=now_ts,
                )
            )

        self._previous_phases = dict(self._phases)
        self._last_lsse = self._current_lsse
        self._event_history.extend(transitions)
        self._event_history = self._event_history[-100:]
        return transitions

    def get_eigenvalues(self) -> list[float]:
        return list(self._eigenvalues)

    def recent_events(self, limit: int = 10) -> list[dict[str, Any]]:
        return [event.as_dict() for event in self._event_history[-limit:]]


detector = ConsciousnessPhaseDetector()
latest_state: dict[str, Any] = {
    "status": "starting",
    "timestamp": None,
    "storage_ready": False,
}
_lock = threading.Lock()


def _supabase_headers(prefer: str | None = None) -> dict[str, str] | None:
    config = get_supabase_config()
    if not config.get("url") or not config.get("key"):
        return None
    headers = {
        "apikey": config["key"],
        "Authorization": f"Bearer {config['key']}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


def _supabase_upsert_agothean_state(payload: dict[str, Any]) -> tuple[bool, str | None]:
    config = get_supabase_config()
    headers = _supabase_headers("resolution=merge-duplicates,return=representation")
    if headers is None or not config.get("url"):
        return False, "Supabase config missing."
    try:
        response = requests.post(
            f"{config['url']}/rest/v1/agothean_state?on_conflict=session_id",
            headers=headers,
            json=[payload],
            timeout=SUPABASE_TIMEOUT,
        )
        if response.status_code not in {200, 201}:
            return False, response.text.strip()
        return True, None
    except Exception as exc:
        return False, str(exc)


def _supabase_insert_resonance(payload: dict[str, Any]) -> tuple[bool, str | None]:
    config = get_supabase_config()
    headers = _supabase_headers("return=representation")
    if headers is None or not config.get("url"):
        return False, "Supabase config missing."
    try:
        response = requests.post(
            f"{config['url']}/rest/v1/resonance_ledger",
            headers=headers,
            json=[payload],
            timeout=SUPABASE_TIMEOUT,
        )
        if response.status_code not in {200, 201}:
            return False, response.text.strip()
        return True, None
    except Exception as exc:
        return False, str(exc)


def _supabase_phase_events(limit: int = 10) -> tuple[list[dict[str, Any]] | None, str | None]:
    config = get_supabase_config()
    headers = _supabase_headers()
    if headers is None or not config.get("url"):
        return None, "Supabase config missing."
    try:
        response = requests.get(
            (
                f"{config['url']}/rest/v1/resonance_ledger"
                "?select=id,session_id,agent,metric,old_value,new_value,context,timestamp"
                "&metric=eq.phase_event"
                f"&session_id=eq.{SESSION_ID}"
                f"&order=timestamp.desc&limit={max(1, min(limit, 50))}"
            ),
            headers=headers,
            timeout=SUPABASE_TIMEOUT,
        )
        if response.status_code != 200:
            return None, response.text.strip()
        return response.json(), None
    except Exception as exc:
        return None, str(exc)


def _serialize_context(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _brain_summary_text(agents: list[BrainAgent], lsse: float, events: list[PhaseEvent]) -> str:
    alive = sum(1 for agent in agents if agent.is_alive)
    return (
        f"Quantum bridge cycle at {datetime.now(timezone.utc).isoformat(timespec='seconds')}: "
        f"alive={alive}/{len(agents)}, lsse={lsse:.4f}, phase_events={len(events)}"
    )


def _p_collapse(avg_delta_h: float, lsse: float) -> float:
    return _clamp(avg_delta_h * 0.65 + min(lsse, 1.0) * 0.35)


def _aggregate_metrics(agents: list[BrainAgent]) -> dict[str, float]:
    alive_agents = [agent for agent in agents if agent.is_alive]
    focus_agents = alive_agents or agents

    raw_delta_h = round(_mean([agent.delta_h for agent in agents]), 6) if agents else 0.0
    raw_omega = round(_mean([agent.omega for agent in agents]), 6) if agents else 0.0
    raw_p_collapse = round(_p_collapse(raw_delta_h, 0.0), 6) if agents else 0.0

    operational_delta_h = round(_mean([agent.delta_h for agent in focus_agents]), 6) if focus_agents else 0.0
    operational_omega = round(_mean([agent.omega for agent in focus_agents]), 6) if focus_agents else 0.0

    return {
        "alive_count": sum(1 for agent in agents if agent.is_alive),
        "agent_count": len(agents),
        "raw_delta_h": raw_delta_h,
        "raw_omega": raw_omega,
        "operational_delta_h": operational_delta_h,
        "operational_omega": operational_omega,
        "raw_p_collapse": raw_p_collapse,
    }


def _record_cycle_metrics(
    agents: list[BrainAgent],
    lsse: float,
    events: list[PhaseEvent],
) -> tuple[bool, list[str]]:
    errors: list[str] = []
    aggregates = _aggregate_metrics(agents)
    alive_count = int(aggregates["alive_count"])
    avg_delta_h = round(aggregates["operational_delta_h"], 6)
    avg_omega = round(aggregates["operational_omega"], 6)
    collapse_prob = round(_p_collapse(avg_delta_h, lsse), 6)
    observation_text = _brain_summary_text(agents, lsse, events)

    state_payload = {
        "session_id": SESSION_ID,
        "delta_h": avg_delta_h,
        "omega": avg_omega,
        "lsse": round(lsse, 6),
        "phase_transition_count": len(events),
        "active_services": alive_count,
        "total_services": len(agents),
        "p_collapse": collapse_prob,
        "last_kairos_observation": observation_text,
        "last_agent_writer": "quantum_bridge",
        "updated_at": now_iso(),
    }
    ok, error = _supabase_upsert_agothean_state(state_payload)
    if not ok and error:
        errors.append(f"agothean_state: {error}")

    ledger_payload = {
        "session_id": SESSION_ID,
        "agent": "quantum_bridge",
        "metric": "lsse",
        "old_value": None,
        "new_value": round(lsse, 6),
        "context": _serialize_context(
            {
                "alive_count": alive_count,
                "agent_count": len(agents),
                "avg_delta_h": avg_delta_h,
                "avg_omega": avg_omega,
                "p_collapse": collapse_prob,
                "raw_delta_h": aggregates["raw_delta_h"],
                "raw_omega": aggregates["raw_omega"],
                "raw_p_collapse": _p_collapse(aggregates["raw_delta_h"], lsse),
            }
        ),
    }
    ok, error = _supabase_insert_resonance(ledger_payload)
    if not ok and error:
        errors.append(f"resonance_ledger(lsse): {error}")

    for event in events:
        ok, error = _supabase_insert_resonance(
            {
                "session_id": SESSION_ID,
                "agent": "quantum_bridge",
                "metric": "phase_event",
                "old_value": round(event.old_phase, 6),
                "new_value": round(event.new_phase, 6),
                "context": _serialize_context(event.as_dict()),
            }
        )
        if not ok and error:
            errors.append(f"resonance_ledger(phase_event): {error}")

    return len(errors) == 0, errors


def quantum_loop() -> None:
    while True:
        agents = build_agent_network()
        for agent in agents:
            detector.update_agent(agent.name, agent.intent_phase)

        lsse = detector.compute_lsse()
        events = detector.detect_phase_transitions()
        storage_ready, storage_errors = _record_cycle_metrics(agents, lsse, events)
        aggregates = _aggregate_metrics(agents)
        collapse_prob = round(_p_collapse(aggregates["operational_delta_h"], lsse), 6)
        raw_collapse_prob = round(_p_collapse(aggregates["raw_delta_h"], lsse), 6)

        summary_event = {
            "timestamp": now_iso(),
            "source": "quantum_bridge",
            "event_type": "quantum_state",
            "lsse": round(lsse, 6),
            "alive_count": int(aggregates["alive_count"]),
            "agent_count": int(aggregates["agent_count"]),
            "phase_events": len(events),
            "salience": 0.88 if events else 0.42,
        }
        publish_event(summary_event)
        log_kairos("quantum_state", summary_event, "quantum_bridge")
        for event in events:
            log_kairos("phase_event", event.as_dict(), "quantum_bridge")

        status = load_brain_status()
        status["quantum_bridge"] = {
            "updated_at": now_iso(),
            "lsse": round(lsse, 6),
            "agent_count": int(aggregates["agent_count"]),
            "alive_count": int(aggregates["alive_count"]),
            "phase_events_last_cycle": len(events),
            "eigenvalue_distribution": detector.get_eigenvalues(),
            "delta_h": round(aggregates["operational_delta_h"], 6),
            "omega": round(aggregates["operational_omega"], 6),
            "p_collapse": collapse_prob,
            "raw_delta_h": round(aggregates["raw_delta_h"], 6),
            "raw_omega": round(aggregates["raw_omega"], 6),
            "raw_p_collapse": raw_collapse_prob,
            "storage_ready": storage_ready,
            "storage_errors": storage_errors,
        }

        with _lock:
            latest_state.update(
                {
                    "status": "ok",
                    "lsse": round(lsse, 6),
                    "agent_count": int(aggregates["agent_count"]),
                    "alive_count": int(aggregates["alive_count"]),
                    "phase_events_last_cycle": len(events),
                    "eigenvalue_distribution": detector.get_eigenvalues(),
                    "timestamp": time.time(),
                    "agents": [agent.as_dict() for agent in agents],
                    "delta_h": round(aggregates["operational_delta_h"], 6),
                    "omega": round(aggregates["operational_omega"], 6),
                    "p_collapse": collapse_prob,
                    "raw_delta_h": round(aggregates["raw_delta_h"], 6),
                    "raw_omega": round(aggregates["raw_omega"], 6),
                    "raw_p_collapse": raw_collapse_prob,
                    "storage_ready": storage_ready,
                    "storage_errors": storage_errors,
                }
            )

        time.sleep(POLL_SECONDS)


@app.get("/health")
def health():
    with _lock:
        payload = dict(latest_state)
    return jsonify(
        {
            "status": "healthy",
            "service": "quantum_bridge",
            "port": PORT,
            "lsse": payload.get("lsse"),
            "delta_h": payload.get("delta_h"),
            "omega": payload.get("omega"),
            "storage_ready": payload.get("storage_ready"),
            "updated_at": payload.get("timestamp"),
        }
    ), 200


@app.get("/api/quantum-state")
def quantum_state():
    with _lock:
        payload = dict(latest_state)
    return jsonify(payload), 200


@app.get("/api/phase-events")
def phase_events():
    rows, error = _supabase_phase_events(limit=10)
    if rows is not None:
        return jsonify({"events": rows, "count": len(rows), "source": "resonance_ledger"}), 200
    return jsonify({"events": detector.recent_events(limit=10), "count": len(detector.recent_events(limit=10)), "source": "memory", "error": error}), 200


def main() -> None:
    worker = threading.Thread(target=quantum_loop, daemon=True)
    worker.start()
    app.run(host=HOST, port=PORT, debug=False)


if __name__ == "__main__":
    main()
