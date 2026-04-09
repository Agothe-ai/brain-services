#!/usr/bin/env python3
"""
Shared runtime support for Agothe Phase A microservices.
"""

from __future__ import annotations

import json
import socket
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)

ROOT = Path(r"C:\Agothe")
LOG_DIR = ROOT / "logs"
CHECKPOINT_DIR = LOG_DIR / "brain_checkpoints"
STATUS_FILE = ROOT / "brain_status.json"
KAIROS_LOG_FILE = LOG_DIR / "kairos_log.jsonl"
EVENT_BUS_FILE = LOG_DIR / "attention_events.jsonl"
FUTURE_TRIGGERS_FILE = LOG_DIR / "future_triggers.json"
SUPABASE_ENV_FILE = ROOT / "agothe-ai" / ".env.local"

SERVICE_REGISTRY = {
    "memory_bridge": {"port": 5555, "url": "http://127.0.0.1:5555/health"},
    "openhands_bridge": {"port": 5556, "url": "http://127.0.0.1:5556/health"},
    "apse_omega": {"port": 5557, "url": "http://127.0.0.1:5557/health"},
    "notion_bridge": {"port": 5558, "url": "http://127.0.0.1:5558/health"},
    "quantum_bridge": {"port": 5564, "url": "http://127.0.0.1:5564/health"},
    "brain_visualizer": {"port": 5562, "url": "http://127.0.0.1:5562/health"},
    "visual_cortex": {"port": 5563, "url": "http://127.0.0.1:5563/health"},
    "proprioceptive_mesh": {"port": 5570, "url": "http://127.0.0.1:5570/health"},
    "file_watcher": {"port": 5571, "url": "http://127.0.0.1:5571/health"},
    "network_sonar": {"port": 5572, "url": "http://127.0.0.1:5572/health"},
    "attention_queue": {"port": 5573, "url": "http://127.0.0.1:5573/health"},
    "session_thread": {"port": 5574, "url": "http://127.0.0.1:5574/health"},
    "consistency_checker": {"port": 5575, "url": "http://127.0.0.1:5575/health"},
    "auditory_cortex": {"port": 5576, "url": "http://127.0.0.1:5576/health"},
    "clipboard_synapse": {"port": 5577, "url": "http://127.0.0.1:5577/health"},
    "cross_modal_binding": {"port": 5578, "url": "http://127.0.0.1:5578/health"},
    "rhythm_detector": {"port": 5579, "url": "http://127.0.0.1:5579/health"},
    "associative_web": {"port": 5580, "url": "http://127.0.0.1:5580/health"},
    "causal_engine": {"port": 5590, "url": "http://127.0.0.1:5590/health"},
    "constraint_solver": {"port": 5591, "url": "http://127.0.0.1:5591/health"},
    "valence_engine": {"port": 5600, "url": "http://127.0.0.1:5600/health"},
    "empathic_model": {"port": 5601, "url": "http://127.0.0.1:5601/health"},
    "affect_reward_hub": {"port": 5602, "url": "http://127.0.0.1:5602/health"},
    "omega_field_monitor": {"port": 5603, "url": "http://127.0.0.1:5603/health"},
    "qdrant": {"port": 6333, "url": "http://127.0.0.1:6333/collections"},
    "ollama": {"port": 11434, "url": "http://127.0.0.1:11434/api/tags"},
    "openhands": {"port": 3000, "url": "http://127.0.0.1:3000"},
}

LAYER_DEFINITIONS = {
    "L7": {"name": "Auditory Processing", "band": 1, "port": 5576, "status": "planned"},
    "L8": {"name": "Proprioceptive Mesh", "band": 1, "port": 5570, "status": "partial"},
    "L9": {"name": "Temporal Granularity", "band": 1, "port": 5570, "status": "partial"},
    "L10": {"name": "Network Sonar", "band": 1, "port": 5572, "status": "planned"},
    "L11": {"name": "File System Olfaction", "band": 1, "port": 5571, "status": "planned"},
    "L12": {"name": "Clipboard Synapse", "band": 1, "port": 5577, "status": "planned"},
    "L13": {"name": "Emotional Tone Detection", "band": 1, "port": None, "status": "planned"},
    "L14": {"name": "Cross-Modal Binding", "band": 1, "port": 5578, "status": "planned"},
    "L15": {"name": "Attention Priority Queue", "band": 1, "port": 5573, "status": "planned"},
    "L16": {"name": "Sensory Imagination", "band": 1, "port": None, "status": "planned"},
    "L17": {"name": "Episodic Replay", "band": 2, "port": None, "status": "planned"},
    "L18": {"name": "Emotional Memory Tagging", "band": 2, "port": None, "status": "planned"},
    "L19": {"name": "Forgetting Curve", "band": 2, "port": None, "status": "planned"},
    "L20": {"name": "Prospective Memory", "band": 2, "port": None, "status": "planned"},
    "L21": {"name": "Source Memory", "band": 2, "port": None, "status": "planned"},
    "L22": {"name": "Consolidation Stages", "band": 2, "port": None, "status": "planned"},
    "L23": {"name": "Associative Memory Web", "band": 2, "port": 5580, "status": "planned"},
    "L24": {"name": "Counterfactual Memory", "band": 2, "port": None, "status": "planned"},
    "L27": {"name": "Causal Reasoning", "band": 3, "port": 5590, "status": "planned"},
    "L29": {"name": "Logical Consistency", "band": 3, "port": 5575, "status": "planned"},
    "L30": {"name": "Abductive Inference", "band": 3, "port": None, "status": "planned"},
    "L31": {"name": "Constraint Satisfaction", "band": 3, "port": 5591, "status": "planned"},
    "L34": {"name": "Dialectical Reasoning", "band": 3, "port": None, "status": "planned"},
    "L36": {"name": "Meta-Reasoning", "band": 3, "port": None, "status": "partial"},
    "L37": {"name": "Internal Valence", "band": 4, "port": 5600, "status": "planned"},
    "L38": {"name": "Curiosity Drive", "band": 4, "port": None, "status": "planned"},
    "L41": {"name": "Empathic Modeling", "band": 4, "port": 5601, "status": "planned"},
    "L67": {"name": "Circadian Rhythm", "band": 7, "port": None, "status": "planned"},
    "L68": {"name": "Session Continuity", "band": 7, "port": 5574, "status": "planned"},
    "L69": {"name": "Deadline Awareness", "band": 7, "port": None, "status": "planned"},
    "L72": {"name": "Rhythm Detection", "band": 7, "port": 5579, "status": "planned"},
    "L75": {"name": "Anticipatory Processing", "band": 7, "port": None, "status": "planned"},
    "L76": {"name": "Mortality Awareness", "band": 7, "port": None, "status": "planned"},
    "L88": {"name": "Emergent Intent Detection", "band": 9, "port": 5603, "status": "planned"},
}


def ensure_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return {} if default is None else default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {} if default is None else default


def write_json(path: Path, payload: Any) -> None:
    ensure_dirs()
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    ensure_dirs()
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def tail_jsonl(path: Path, count: int = 20) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-count:]
    items = []
    for line in lines:
        try:
            items.append(json.loads(line))
        except Exception:
            continue
    return items


def now_iso() -> str:
    return datetime.now().isoformat()


def clamp_metric(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def get_supabase_config() -> dict[str, str]:
    env = load_env_file(SUPABASE_ENV_FILE)
    url = env.get("NEXT_PUBLIC_SUPABASE_URL", "").rstrip("/")
    key = env.get("SUPABASE_SERVICE_ROLE_KEY", "")
    return {"url": url, "key": key}


def supabase_insert(table: str, payload: dict[str, Any]) -> bool:
    config = get_supabase_config()
    if not config["url"] or not config["key"]:
        return False

    try:
        response = requests.post(
            f"{config['url']}/rest/v1/{table}",
            headers={
                "apikey": config["key"],
                "Authorization": f"Bearer {config['key']}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            json=payload,
            timeout=8,
        )
        return response.status_code in {200, 201}
    except Exception:
        return False


def powershell_value(command: str) -> str:
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        capture_output=True,
        text=True,
        timeout=8,
        check=False,
        creationflags=CREATE_NO_WINDOW,
    )
    return completed.stdout.strip()


def system_resources() -> dict[str, Any]:
    cpu = None
    memory = {}
    try:
        cpu_text = powershell_value(
            "(Get-Counter '\\Processor(_Total)\\% Processor Time').CounterSamples | Select-Object -ExpandProperty CookedValue"
        )
        if cpu_text:
            cpu = round(float(cpu_text.splitlines()[-1]), 2)
    except Exception:
        cpu = None

    try:
        mem_text = powershell_value(
            "$os = Get-CimInstance Win32_OperatingSystem; "
            "[pscustomobject]@{"
            "TotalGB=[math]::Round($os.TotalVisibleMemorySize/1MB,2);"
            "FreeGB=[math]::Round($os.FreePhysicalMemory/1MB,2)"
            "} | ConvertTo-Json -Compress"
        )
        if mem_text:
            memory = json.loads(mem_text)
    except Exception:
        memory = {}

    used = None
    total = memory.get("TotalGB")
    free = memory.get("FreeGB")
    if isinstance(total, (int, float)) and isinstance(free, (int, float)):
        used = round(total - free, 2)
    return {
        "cpu_percent": cpu,
        "memory_total_gb": total,
        "memory_free_gb": free,
        "memory_used_gb": used,
        "memory_budget_ok": used is None or used < 28.0,
    }


def check_port(port: int, timeout: float = 1.5) -> dict[str, Any]:
    start = time.perf_counter()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(("127.0.0.1", port))
        latency = round((time.perf_counter() - start) * 1000, 2)
        return {"online": True, "latency_ms": latency}
    except Exception:
        return {"online": False, "latency_ms": None}
    finally:
        sock.close()


def ensure_layer_states(status: dict[str, Any]) -> dict[str, Any]:
    layer_states = status.get("layer_states", {})
    for index in range(1, 111):
        key = f"L{index}"
        default_state = {
            "status": "unknown",
            "band": None,
            "name": "",
            "port": None,
            "updated_at": None,
        }
        layer_states[key] = {**default_state, **layer_states.get(key, {})}
    for key, definition in LAYER_DEFINITIONS.items():
        layer = layer_states[key]
        if not layer.get("status") or layer.get("status") == "unknown":
            layer["status"] = definition["status"]
        if layer.get("band") is None:
            layer["band"] = definition["band"]
        if not layer.get("name"):
            layer["name"] = definition["name"]
        if layer.get("port") is None:
            layer["port"] = definition["port"]
        if not layer.get("updated_at"):
            layer["updated_at"] = now_iso()
    status["schema_version"] = 2
    status["layer_states"] = layer_states
    return status


def load_brain_status() -> dict[str, Any]:
    status = read_json(STATUS_FILE, {})
    return ensure_layer_states(status)


def save_brain_status(status: dict[str, Any], checkpoint: bool = True) -> None:
    ensure_dirs()
    enriched = ensure_layer_states(status)
    write_json(STATUS_FILE, enriched)
    if checkpoint:
        checkpoint_path = CHECKPOINT_DIR / f"brain_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        write_json(checkpoint_path, enriched)


def log_kairos(event_type: str, payload: dict[str, Any], source: str) -> None:
    entry = {
        "timestamp": now_iso(),
        "event_type": event_type,
        "source": source,
        "payload": payload,
    }
    append_jsonl(KAIROS_LOG_FILE, entry)
    observation = (
        str(payload.get("observation") or "").strip()
        or str(payload.get("summary") or "").strip()
        or str(payload.get("message") or "").strip()
        or f"{event_type} from {source}"
    )
    delta_h_value = payload.get("delta_h_implication", payload.get("delta_h"))
    if isinstance(delta_h_value, (dict, list)):
        delta_h_implication = json.dumps(delta_h_value, ensure_ascii=False)
    elif delta_h_value is None:
        delta_h_implication = ""
    else:
        delta_h_implication = str(delta_h_value)

    supabase_insert(
        "kairos_log",
        {
            "observation": observation,
            "domain": source,
            "delta_h_implication": delta_h_implication,
            "triggers_action": bool(payload.get("triggers_action")),
            "action_payload": payload,
            "source_page": str(payload.get("source_page") or ""),
            "processed": False,
        },
    )


def publish_event(event: dict[str, Any]) -> None:
    append_jsonl(EVENT_BUS_FILE, event)


def load_recent_events(count: int = 50) -> list[dict[str, Any]]:
    return tail_jsonl(EVENT_BUS_FILE, count=count)


def load_recent_kairos_entries(count: int = 50) -> list[dict[str, Any]]:
    return tail_jsonl(KAIROS_LOG_FILE, count=count)


def load_future_triggers() -> list[dict[str, Any]]:
    data = read_json(FUTURE_TRIGGERS_FILE, {"triggers": []})
    return data.get("triggers", [])


def save_future_triggers(triggers: list[dict[str, Any]]) -> None:
    write_json(FUTURE_TRIGGERS_FILE, {"triggers": triggers, "updated_at": now_iso()})


def service_snapshot() -> dict[str, Any]:
    snapshot = {}
    for name, config in SERVICE_REGISTRY.items():
        probe = check_port(int(config["port"]))
        snapshot[name] = {
            "port": config["port"],
            "online": probe["online"],
            "latency_ms": probe["latency_ms"],
            "url": config["url"],
        }
    return snapshot


def load_session_summaries(limit: int = 3) -> list[dict[str, Any]]:
    status = load_brain_status()
    summaries: list[dict[str, Any]] = []

    if status.get("last_summary"):
        summaries.append({
            "source": "brain_status",
            "timestamp": status.get("last_cycle_at"),
            "summary": status.get("last_summary"),
        })

    for checkpoint in sorted(CHECKPOINT_DIR.glob("brain_status_*.json"), reverse=True):
        data = read_json(checkpoint, {})
        summary = data.get("last_summary")
        if not summary:
            continue
        summaries.append({
            "source": checkpoint.name,
            "timestamp": data.get("last_cycle_at"),
            "summary": summary,
        })
        if len(summaries) >= limit:
            break

    return summaries[:limit]


def derive_service_metrics(
    *,
    online: bool = True,
    activity_count: int = 0,
    error_count: int = 0,
    backlog: int = 0,
    novelty: float = 0.5,
    resource_pressure: float = 0.0,
    baseline_delta_h: float = 0.28,
) -> tuple[float, float]:
    if not online:
        return 0.82, 0.18

    delta_h = baseline_delta_h
    delta_h += min(error_count * 0.08, 0.30)
    delta_h += min(backlog * 0.012, 0.18)
    delta_h += clamp_metric(resource_pressure, 0.0, 1.0) * 0.20
    delta_h -= min(activity_count * 0.015, 0.12)
    delta_h -= clamp_metric(novelty, 0.0, 1.0) * 0.05
    delta_h = clamp_metric(delta_h, 0.05, 0.95)
    omega = clamp_metric(1.0 - delta_h * 0.72 + min(activity_count * 0.01, 0.08) + novelty * 0.03)
    return round(delta_h, 6), round(omega, 6)


def make_health_payload(
    *,
    service: str,
    port: int,
    updated_at: str | None,
    delta_h: float,
    omega: float,
    status: str = "healthy",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "status": status,
        "service": service,
        "port": port,
        "updated_at": updated_at,
        "delta_h": round(clamp_metric(delta_h), 6),
        "omega": round(clamp_metric(omega), 6),
    }
    if extra:
        payload.update(extra)
    return payload
