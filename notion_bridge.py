#!/usr/bin/env python3
"""
PHASE 11 - Enhanced Notion Bridge v2
Full Notion workspace orchestration with caching and batch operations
"""
from flask import Flask, request, jsonify
import requests as req
import os
import json
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

BASE_DIR = Path(r"C:\Agothe")
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"
ENV_CANDIDATES = [
    BASE_DIR / "mcp_servers" / ".env",
    BASE_DIR / ".env",
]
DRY_RUN_LOG = LOG_DIR / "notion_dry_run.log"
LAST_DRY_RUN_CHECK = LOG_DIR / "notion_dry_run_check.json"


def _truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _load_env_file(path: Path):
    if not path.exists():
        return

    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception:
        return


def _load_env_candidates() -> str:
    for candidate in ENV_CANDIDATES:
        if candidate.exists():
            _load_env_file(candidate)
            return str(candidate)
    return "process"


ENV_SOURCE = _load_env_candidates()
LOG_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

app = Flask(__name__)

NOTION_TOKEN = os.getenv("NOTION_API_KEY") or os.getenv("NOTION_TOKEN", "ntn_YOUR_TOKEN_HERE")
NOTION_API = "https://api.notion.com/v1"
CACHE_TTL = 300  # 5 minutes
MEMORY_BRIDGE = os.getenv("MEMORY_BRIDGE_URL", "http://127.0.0.1:5555")
BRIDGE_MODE = "dry" if _truthy_env(os.getenv("DRY_RUN")) else "live"
BRIDGE_STARTED_AT = datetime.now(timezone.utc).isoformat()

NEOCORTEX_MEMORY_MAP = {
    "facts": ["codex", "facts"],
    "concepts": ["codex", "concepts"],
    "hypotheses": ["codex", "hypotheses"],
    "decisions": ["codex", "decisions"],
    "projects": ["codex", "projects"],
}

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}


def _current_mode() -> str:
    return BRIDGE_MODE


def _effective_dry_run(data: dict | None) -> bool:
    payload = data or {}
    requested = payload.get("dry_run")
    if _current_mode() == "dry":
        return True
    return bool(requested) if requested is not None else False


def _content_digest(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()[:16]


def _write_dry_run_record(operation: str, payload: dict) -> dict:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "operation": operation,
        "mode": _current_mode(),
        "payload": payload,
    }
    with DRY_RUN_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    LAST_DRY_RUN_CHECK.write_text(json.dumps(record, indent=2), encoding="utf-8")
    return {
        "dry_run": True,
        "mode": _current_mode(),
        "log_path": str(DRY_RUN_LOG),
        "content_digest": payload.get("content_digest"),
        "recorded_at": record["timestamp"],
    }


def _build_dry_run_payload(data: dict, extra: dict | None = None) -> dict:
    content = data.get("content", "")
    payload = {
        "page_id": data.get("page_id"),
        "parent_id": data.get("parent_id"),
        "parent_type": data.get("parent_type"),
        "title": data.get("title"),
        "memory_type": data.get("memory_type"),
        "source_subsystem": data.get("source_subsystem"),
        "content_length": len(content),
        "content_digest": _content_digest(content),
    }
    if extra:
        payload.update(extra)
    return payload

class NotionCache:
    def __init__(self, ttl=300):
        self.cache = {}
        self.ttl = ttl
    
    def get(self, key):
        if key in self.cache:
            data, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return data
            else:
                del self.cache[key]
        return None
    
    def set(self, key, value):
        self.cache[key] = (value, time.time())
    
    def clear(self):
        self.cache.clear()

cache = NotionCache(CACHE_TTL)


def notion_get(path, **kwargs):
    return req.get(f"{NOTION_API}{path}", headers=HEADERS, timeout=10, **kwargs)


def notion_post(path, payload):
    return req.post(f"{NOTION_API}{path}", headers=HEADERS, json=payload, timeout=10)


def notion_patch(path, payload):
    return req.patch(f"{NOTION_API}{path}", headers=HEADERS, json=payload, timeout=10)


def get_workspace_overview():
    cache_key = "workspace:overview"
    cached = cache.get(cache_key)
    if cached:
        return cached

    resp = notion_post("/search", {
        "filter": {"value": "database", "property": "object"},
        "page_size": 100
    }).json()

    databases = []
    for db in resp.get("results", []):
        title = "".join([t.get("plain_text", "") for t in db.get("title", [])])
        databases.append({
            "id": db.get("id"),
            "title": title,
            "url": db.get("url", ""),
            "created_time": db.get("created_time")
        })

    response = {
        "workspace": "Agothe Notion Workspace",
        "databases": databases,
        "count": len(databases)
    }
    cache.set(cache_key, response)
    return response


def find_database_for_memory_type(memory_type: str):
    fragments = NEOCORTEX_MEMORY_MAP.get((memory_type or "").lower(), [])
    if not fragments:
        return None

    for database in get_workspace_overview().get("databases", []):
        title = database.get("title", "").lower()
        if all(fragment in title for fragment in fragments):
            return database
    return None


def notion_search_titles(query: str, limit: int = 20):
    resp = notion_post("/search", {
        "query": query,
        "page_size": min(limit, 100)
    }).json()
    results = []
    for item in resp.get("results", []):
        title = ""
        if item.get("object") == "page":
            for key, prop in item.get("properties", {}).items():
                if prop.get("type") == "title":
                    title = "".join(part.get("plain_text", "") for part in prop.get("title", []))
                    break
        elif item.get("object") == "database":
            title = "".join(part.get("plain_text", "") for part in item.get("title", []))

        if title:
            results.append({
                "id": item.get("id"),
                "title": title,
                "url": item.get("url", ""),
                "type": item.get("object", "page"),
                "last_edited_time": item.get("last_edited_time")
            })
    return results


def extract_page_title(page: dict):
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title":
            return "".join(part.get("plain_text", "") for part in prop.get("title", []))
    return ""


def parse_notion_time(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def memory_search(query: str, top_k: int = 5):
    try:
        response = req.post(
            f"{MEMORY_BRIDGE}/search",
            json={"query": query, "top_k": top_k},
            timeout=10,
        )
        if response.status_code == 200:
            return response.json().get("results", [])
    except Exception:
        pass
    return []


def memory_upsert(text: str, metadata: dict):
    try:
        req.post(
            f"{MEMORY_BRIDGE}/upsert",
            json={"text": text, "metadata": metadata},
            timeout=10,
        )
    except Exception:
        pass


def find_relation_property(properties: dict):
    for key, prop in properties.items():
        if prop.get("type") == "relation" and key.lower().replace(" ", "") in {"relatedto", "related"}:
            return key
    return None

def check_token():
    """Verify Notion token is valid"""
    if NOTION_TOKEN == "ntn_YOUR_TOKEN_HERE" or not NOTION_TOKEN.startswith("ntn_"):
        return False
    try:
        resp = req.get(f"{NOTION_API}/users/me", headers=HEADERS, timeout=5)
        return resp.status_code == 200
    except:
        return False

def extract_text_from_blocks(blocks):
    """Extract plain text from Notion blocks"""
    text = []
    for block in blocks.get("results", []):
        block_type = block.get("type")
        block_data = block.get(block_type, {})
        
        if "rich_text" in block_data:
            for rt in block_data["rich_text"]:
                if rt.get("type") == "text":
                    text.append(rt["text"]["content"])
    
    return "\n".join(text)

@app.route("/health", methods=["GET"])
def health():
    """Health check"""
    valid = check_token()
    return jsonify({
        "status": "ok" if valid else "unconfigured",
        "notion": "connected" if valid else "no_token",
        "bridge_version": "2.0",
        "cache_size": len(cache.cache),
        "mode": _current_mode(),
        "dry_run": _current_mode() == "dry",
        "env_source": ENV_SOURCE,
        "dry_run_log": str(DRY_RUN_LOG),
        "started_at": BRIDGE_STARTED_AT,
        "message": "Notion bridge ready" if valid else "Set NOTION_API_KEY or NOTION_TOKEN environment variable"
    })

@app.route("/search", methods=["POST"])
def search_notion():
    """Search Notion for pages/databases"""
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400
    
    try:
        data = request.json or {}
        query = data.get("query", "")
        limit = data.get("limit", 10)
        
        cache_key = f"search:{query}"
        cached = cache.get(cache_key)
        if cached:
            return jsonify(cached)
        
        resp = req.post(f"{NOTION_API}/search", headers=HEADERS, json={
            "query": query,
            "page_size": min(limit, 100)
        }, timeout=10).json()
        
        results = []
        for r in resp.get("results", []):
            title = ""
            if r["object"] == "page":
                props = r.get("properties", {})
                for p in props.values():
                    if p.get("type") == "title":
                        title = "".join([t.get("plain_text", "") for t in p.get("title", [])])
                        break
            elif r["object"] == "database":
                title = "".join([t.get("plain_text", "") for t in r.get("title", [])])
            
            if title:
                results.append({
                    "id": r.get("id", ""),
                    "type": r["object"],
                    "title": title,
                    "url": r.get("url", ""),
                    "created_time": r.get("created_time"),
                    "last_edited_time": r.get("last_edited_time")
                })
        
        response = {
            "query": query,
            "results": results,
            "count": len(results)
        }
        cache.set(cache_key, response)
        return jsonify(response)
    except Exception as e:
        print(f"[SEARCH ERROR] {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/read-page", methods=["POST"])
def read_page():
    """Read a Notion page's full content"""
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400
    
    try:
        data = request.json or {}
        page_id = data.get("page_id", "").replace("-", "")
        
        cache_key = f"page:{page_id}"
        cached = cache.get(cache_key)
        if cached:
            return jsonify(cached)
        
        # Get page properties
        page = req.get(f"{NOTION_API}/pages/{page_id}", headers=HEADERS, timeout=10).json()
        
        # Get page content
        blocks = req.get(f"{NOTION_API}/blocks/{page_id}/children?page_size=100", 
                        headers=HEADERS, timeout=10).json()
        
        # Extract text content
        content = extract_text_from_blocks(blocks)
        
        # Extract title from properties
        title = ""
        props = page.get("properties", {})
        for p in props.values():
            if p.get("type") == "title":
                title = "".join([t.get("plain_text", "") for t in p.get("title", [])])
                break
        
        response = {
            "id": page.get("id"),
            "title": title,
            "url": page.get("url", ""),
            "created_time": page.get("created_time"),
            "last_edited_time": page.get("last_edited_time"),
            "content": content[:3000],  # First 3000 chars
            "blocks_count": len(blocks.get("results", [])),
            "has_children": page.get("has_children", False)
        }
        
        cache.set(cache_key, response)
        print(f"[READ PAGE] {title} - {len(content)} chars")
        return jsonify(response)
    except Exception as e:
        print(f"[READ ERROR] {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/read", methods=["GET", "POST"])
@app.route("/read", methods=["GET", "POST"])
def read_page_alias():
    if request.method == "GET":
        payload = {"page_id": request.args.get("page_id", "")}
    else:
        payload = request.json or {}

    with app.test_request_context(json=payload):
        return read_page()

@app.route("/query-database", methods=["POST"])
def query_database():
    """Query a Notion database with filters"""
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400
    
    try:
        data = request.json or {}
        db_id = data.get("database_id", "").replace("-", "")
        limit = data.get("limit", 20)
        
        cache_key = f"db:{db_id}:{limit}"
        cached = cache.get(cache_key)
        if cached:
            return jsonify(cached)
        
        resp = req.post(f"{NOTION_API}/databases/{db_id}/query", headers=HEADERS, json={
            "page_size": min(limit, 100)
        }, timeout=10).json()
        
        pages = []
        for r in resp.get("results", []):
            title = ""
            props = r.get("properties", {})
            for p in props.values():
                if p.get("type") == "title":
                    title = "".join([t.get("plain_text", "") for t in p.get("title", [])])
                    break
            
            if title:
                pages.append({
                    "id": r.get("id", ""),
                    "title": title,
                    "url": r.get("url", ""),
                    "created_time": r.get("created_time"),
                    "last_edited_time": r.get("last_edited_time")
                })
        
        response = {
            "database_id": db_id,
            "pages": pages,
            "count": len(pages)
        }
        cache.set(cache_key, response)
        print(f"[QUERY DB] Found {len(pages)} pages")
        return jsonify(response)
    except Exception as e:
        print(f"[QUERY ERROR] {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/create-page", methods=["POST"])
def create_page():
    """Create a new Notion page"""
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400
    
    try:
        data = request.json or {}
        parent_id = data.get("parent_id", "").replace("-", "")
        parent_type = data.get("parent_type", "page_id")  # or database_id
        title = data.get("title", "Untitled")
        content = data.get("content", "")
        properties = data.get("properties", {})
        
        effective_dry_run = _effective_dry_run(data)
        body = {
            "parent": {parent_type: parent_id},
            "properties": {
                "title": {
                    "title": [{"text": {"content": title}}]
                }
            }
        }
        
        # Merge custom properties
        body["properties"].update(properties)
        
        # Add content as children
        if content:
            body["children"] = [{
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"text": {"content": content[:2000]}}]
                }
            }]
        
        if effective_dry_run:
            dry_run = _write_dry_run_record(
                "create_page",
                _build_dry_run_payload(data, {"target_parent": parent_id, "title": title}),
            )
            return jsonify({
                "created": False,
                "title": title,
                "runtime_mode": _current_mode(),
                **dry_run,
            })

        resp = req.post(f"{NOTION_API}/pages", headers=HEADERS, json=body, timeout=10).json()
        
        print(f"[CREATE PAGE] {title}")
        cache.clear()  # Invalidate cache
        
        return jsonify({
            "id": resp.get("id"),
            "url": resp.get("url"),
            "title": title,
            "created": True
        })
    except Exception as e:
        print(f"[CREATE ERROR] {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/update-page", methods=["POST"])
def update_page():
    """Update a Notion page's properties"""
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400
    
    try:
        data = request.json or {}
        page_id = data.get("page_id", "").replace("-", "")
        properties = data.get("properties", {})
        
        effective_dry_run = _effective_dry_run(data)
        if effective_dry_run:
            dry_run = _write_dry_run_record(
                "update_page",
                _build_dry_run_payload(data, {"target_page": page_id, "property_count": len(properties)}),
            )
            return jsonify({
                "updated": False,
                "id": page_id,
                "runtime_mode": _current_mode(),
                **dry_run,
            })

        resp = req.patch(f"{NOTION_API}/pages/{page_id}", headers=HEADERS, json={
            "properties": properties
        }, timeout=10).json()
        
        print(f"[UPDATE PAGE] {page_id[:8]}")
        cache.clear()  # Invalidate cache
        
        return jsonify({
            "id": resp.get("id"),
            "url": resp.get("url"),
            "updated": True
        })
    except Exception as e:
        print(f"[UPDATE ERROR] {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/get-workspace", methods=["GET"])
def get_workspace():
    """Get workspace overview (all databases)"""
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400
    
    try:
        response = get_workspace_overview()
        print(f"[WORKSPACE] {response.get('count', 0)} databases")
        return jsonify(response)
    except Exception as e:
        print(f"[WORKSPACE ERROR] {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/neocortex/read", methods=["POST"])
def neocortex_read():
    """Semantic read phase using local memory + Notion title search."""
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400

    try:
        data = request.json or {}
        intent_vector = data.get("intent_vector") or data.get("query", "")
        top_k = int(data.get("top_k", 5))
        if not intent_vector.strip():
            return jsonify({"error": "intent_vector required"}), 400

        memory_hits = memory_search(intent_vector, top_k=top_k)
        search_hits = notion_search_titles(intent_vector, limit=max(top_k * 2, 10))
        linked_pages = []
        for hit in memory_hits:
            notion_id = hit.get("notion_id")
            if notion_id:
                linked_pages.append({"id": notion_id, "score": hit.get("score", 0), "source": hit.get("source")})

        return jsonify({
            "intent_vector": intent_vector,
            "semantic_hits": memory_hits,
            "search_hits": search_hits[:top_k],
            "linked_pages": linked_pages,
            "count": {
                "semantic_hits": len(memory_hits),
                "search_hits": min(len(search_hits), top_k),
                "linked_pages": len(linked_pages)
            }
        })
    except Exception as e:
        print(f"[NEOCORTEX READ ERROR] {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/neocortex/write", methods=["POST"])
def neocortex_write():
    """Write phase for facts, concepts, hypotheses, decisions, and projects."""
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400

    try:
        data = request.json or {}
        memory_type = (data.get("memory_type") or "facts").lower()
        title = data.get("title", "Untitled")
        content = data.get("content", "")
        source_subsystem = data.get("source_subsystem", "unknown")
        notion_database_id = data.get("notion_database_id")

        database = None
        if notion_database_id:
            database = {"id": notion_database_id}
        else:
            database = find_database_for_memory_type(memory_type)

        if not database:
            return jsonify({"error": f"No database found for memory_type={memory_type}"}), 404

        properties = data.get("properties", {})
        properties.setdefault("title", {"title": [{"text": {"content": title}}]})
        effective_dry_run = _effective_dry_run(data)
        body = {
            "parent": {"database_id": database["id"].replace("-", "")},
            "properties": properties
        }
        blocks = []
        if content:
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": content[:1800]}}]}
            })
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{
                    "text": {
                        "content": f"Source subsystem: {source_subsystem} | Written: {datetime.now(timezone.utc).isoformat()}"
                    }
                }]
            }
        })
        if blocks:
            body["children"] = blocks

        if effective_dry_run:
            dry_run = _write_dry_run_record(
                "neocortex_write",
                _build_dry_run_payload(
                    data,
                    {
                        "database_id": database["id"],
                        "memory_type": memory_type,
                        "title": title,
                    },
                ),
            )
            return jsonify({
                "created": False,
                "memory_type": memory_type,
                "title": title,
                "runtime_mode": _current_mode(),
                **dry_run,
            })

        created = notion_post("/pages", body).json()
        page_id = created.get("id")
        if page_id:
            memory_upsert(
                content or title,
                {
                    "memory_type": memory_type,
                    "source": source_subsystem,
                    "notion_id": page_id,
                    "title": title,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                },
            )
        cache.clear()
        return jsonify({
            "created": True,
            "memory_type": memory_type,
            "title": title,
            "id": page_id,
            "url": created.get("url", "")
        })
    except Exception as e:
        print(f"[NEOCORTEX WRITE ERROR] {e}")
        return jsonify({"error": str(e)}), 500


def append_page_content(page_id: str, content: str, dry_run: bool = False) -> dict:
    safe_page_id = (page_id or "").replace("-", "")
    payload = {
        "page_id": safe_page_id,
        "content": content,
        "content_digest": _content_digest(content),
        "content_length": len(content or ""),
    }

    if dry_run:
        return _write_dry_run_record("append_page_content", payload)

    body = {
        "children": [{
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"text": {"content": (content or "")[:1800]}}]
            }
        }]
    }
    response = notion_patch(f"/blocks/{safe_page_id}/children", body).json()
    return {
        "updated": True,
        "id": response.get("id", safe_page_id),
        "results": len(response.get("results", [])),
        "content_digest": payload["content_digest"],
    }


@app.route("/api/write", methods=["POST"])
@app.route("/write", methods=["POST"])
def api_write():
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400

    data = request.json or {}
    page_id = data.get("page_id", "")
    content = data.get("content", "")
    if not page_id:
        return jsonify({"error": "page_id required"}), 400

    try:
        result = append_page_content(page_id, content, dry_run=_effective_dry_run(data))
        result["runtime_mode"] = _current_mode()
        return jsonify(result)
    except Exception as e:
        print(f"[API WRITE ERROR] {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/neocortex/cross-link", methods=["POST"])
def neocortex_cross_link():
    """Cross-link phase using title similarity and optional relation updates."""
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400

    try:
        data = request.json or {}
        page_id = (data.get("page_id") or "").replace("-", "")
        query = data.get("query", "")
        threshold = float(data.get("threshold", 0.85))
        apply_updates = bool(data.get("apply", False))
        if not page_id:
            return jsonify({"error": "page_id required"}), 400

        page = notion_get(f"/pages/{page_id}").json()
        title = extract_page_title(page)
        candidates = notion_search_titles(query or title, limit=25)
        matches = []
        for candidate in candidates:
            if candidate["id"].replace("-", "") == page_id:
                continue
            similarity = SequenceMatcher(None, title.lower(), candidate["title"].lower()).ratio()
            if similarity >= threshold:
                matches.append({**candidate, "similarity": round(similarity, 3)})

        applied = False
        relation_property = find_relation_property(page.get("properties", {}))
        if apply_updates and relation_property and matches:
            relation_ids = [{"id": item["id"]} for item in matches[:10]]
            notion_patch(f"/pages/{page_id}", {
                "properties": {
                    relation_property: {"relation": relation_ids}
                }
            })
            applied = True
            cache.clear()

        return jsonify({
            "page_id": page_id,
            "title": title,
            "relation_property": relation_property,
            "matches": matches[:10],
            "applied": applied
        })
    except Exception as e:
        print(f"[NEOCORTEX CROSS-LINK ERROR] {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/neocortex/consolidate", methods=["POST"])
def neocortex_consolidate():
    """Safe consolidation plan with optional stale-fact archiving."""
    if not check_token():
        return jsonify({"error": "Notion token not configured"}), 400

    try:
        data = request.json or {}
        memory_type = (data.get("memory_type") or "facts").lower()
        dry_run = bool(data.get("dry_run", True))
        stale_days = int(data.get("stale_days", 90))

        database = find_database_for_memory_type(memory_type)
        if not database:
            return jsonify({"error": f"No database found for memory_type={memory_type}"}), 404

        queried = notion_post(f"/databases/{database['id'].replace('-', '')}/query", {"page_size": 100}).json()
        pages = queried.get("results", [])
        normalized = {}
        stale_candidates = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=stale_days)

        for page in pages:
            title = extract_page_title(page).strip()
            if not title:
                continue
            key = re.sub(r"\s+", " ", title.lower())
            normalized.setdefault(key, []).append({
                "id": page.get("id"),
                "title": title,
                "last_edited_time": page.get("last_edited_time")
            })
            edited = parse_notion_time(page.get("last_edited_time"))
            if edited and edited < cutoff:
                stale_candidates.append({
                    "id": page.get("id"),
                    "title": title,
                    "last_edited_time": page.get("last_edited_time")
                })

        duplicate_groups = [group for group in normalized.values() if len(group) > 1]
        archived = []
        if not dry_run:
            for page in stale_candidates:
                notion_patch(f"/pages/{page['id'].replace('-', '')}", {"archived": True})
                archived.append(page["id"])
            cache.clear()

        return jsonify({
            "memory_type": memory_type,
            "database_id": database["id"],
            "dry_run": dry_run,
            "duplicate_groups": duplicate_groups,
            "stale_candidates": stale_candidates,
            "archived": archived,
            "operations": [
                "Merge duplicate Codex Facts into canonical entries",
                "Promote validated Hypotheses to Concepts",
                "Archive stale Facts older than threshold with no cross-links",
                "Recalculate vector embeddings for updated Concepts"
            ]
        })
    except Exception as e:
        print(f"[NEOCORTEX CONSOLIDATE ERROR] {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/info", methods=["GET"])
def get_info():
    """Bridge info and capabilities"""
    valid = check_token()
    return jsonify({
        "bridge": "notion",
        "version": "2.0",
        "connected": valid,
        "mode": _current_mode(),
        "token_set": not NOTION_TOKEN.startswith("ntn_YOUR"),
        "cache_ttl": CACHE_TTL,
        "endpoints": {
            "/search": "POST - Search Notion",
            "/read-page": "POST - Read page content",
            "/api/read": "GET|POST - Passdown-compatible page read",
            "/query-database": "POST - Query database",
            "/create-page": "POST - Create page",
            "/update-page": "POST - Update page",
            "/api/write": "POST - Passdown-compatible page append/write",
            "/neocortex/read": "POST - Semantic read phase",
            "/neocortex/write": "POST - Neocortex write phase",
            "/neocortex/cross-link": "POST - Cross-link similar pages",
            "/neocortex/consolidate": "POST - Safe consolidation plan",
            "/get-workspace": "GET - Get workspace overview",
            "/health": "GET - Health check",
            "/info": "GET - This info"
        },
        "setup": "Set NOTION_API_KEY or NOTION_TOKEN" if not valid else "Ready"
    })

if __name__ == "__main__":
    print("=" * 60)
    print("Notion Bridge v2.0 - Phase 11 Deployment")
    print("=" * 60)
    print("Listening on 0.0.0.0:5558")
    print("")
    print("Notion Workspace Orchestration:")
    print("  - Search: /search")
    print("  - Read pages: /read-page")
    print("  - Passdown read: /api/read")
    print("  - Query databases: /query-database")
    print("  - Create pages: /create-page")
    print("  - Update pages: /update-page")
    print("  - Passdown write: /api/write")
    print("  - Workspace overview: /get-workspace")
    print(f"  - Runtime mode: {_current_mode()}")
    print("")
    
    if not check_token():
        print("WARNING: NOTION_API_KEY / NOTION_TOKEN not set or invalid")
    else:
        print("[OK] Notion workspace connected")
    
    print("")
    app.run(host="0.0.0.0", port=5558, debug=False)
