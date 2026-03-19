import json
import os
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from review.runtime_query_ui import (
    run_runtime_api_pipeline,
    start_async_valid_dedupe,
    get_async_task_status,
)

_SERVER = None
_SERVER_THREAD = None
_SERVER_LOCK = threading.Lock()


def _normalize_runtime_response(query: str, pipeline_result: dict) -> dict:
    status = pipeline_result.get("status")
    path = pipeline_result.get("path")
    result = pipeline_result.get("result") or {}

    nested = result.get("primary_failed") or pipeline_result.get("primary_failed") or result.get("generate_failed") or {}
    if not isinstance(nested, dict):
        nested = {}

    api_schema = result.get("api_schema") or nested.get("api_schema") or {}
    bound_sql = (api_schema.get("bound_sql") or result.get("sql") or "") if isinstance(api_schema, dict) else result.get("sql") or ""
    slot_mapping = (api_schema.get("slot_mapping") or {}) if isinstance(api_schema, dict) else {}
    slot_values = result.get("params") or nested.get("params") or {}
    selected_table = result.get("selected_table") or nested.get("selected_table") or api_schema.get("table") or None

    normalized = {
        "status": status,
        "query": query,
        "route": path,
        "table_name": selected_table,
        "candidate_tables": result.get("candidate_tables") or nested.get("candidate_tables") or [],
        "topk_api_names": result.get("topk_api_names") or nested.get("topk_api_names") or [],
        "api_name": api_schema.get("name") if isinstance(api_schema, dict) else None,
        "api_description": api_schema.get("description") if isinstance(api_schema, dict) else None,
        "bound_sql": bound_sql,
        "slot_mapping": slot_mapping,
        "slot_values": slot_values,
        "filled_sql": result.get("invoked_sql") or nested.get("invoked_sql"),
        "review_task_id": result.get("review_task_id") or nested.get("review_task_id"),
        "error": result.get("error") or result.get("reason") or nested.get("error") or nested.get("reason"),
        "raw": pipeline_result,
    }
    return normalized


class _RuntimeAPIHandler(BaseHTTPRequestHandler):
    config = {}

    def _send_json(self, status_code: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length", "0") or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            return json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            return {}

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(200, {
                "status": "ok",
                "time": datetime.now().isoformat(),
                "service": "runtime_api_bridge",
            })
            return

        if parsed.path == "/maintenance/task":
            query = parse_qs(parsed.query)
            task_id = (query.get("task_id") or [""])[0]
            if not task_id:
                self._send_json(400, {"status": "error", "error": "missing task_id"})
                return
            self._send_json(200, get_async_task_status(task_id))
            return

        self._send_json(404, {"status": "error", "error": "not_found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        payload = self._read_json()
        cfg = dict(self.config)

        if parsed.path in {"/runtime/resolve", "/runtime/resolve-with-generate"}:
            query = (payload.get("query") or "").strip()
            if not query:
                self._send_json(400, {"status": "error", "error": "query is required"})
                return

            enable_generate = parsed.path == "/runtime/resolve-with-generate"
            result = run_runtime_api_pipeline(
                query=query,
                valid_path=payload.get("valid_path") or cfg.get("valid_path") or "",
                review_queue=payload.get("review_queue") or cfg.get("review_queue") or "",
                output_dir=payload.get("output_dir") or cfg.get("output_dir") or ".",
                table_name=payload.get("table_name") or cfg.get("table_name") or "base_staff",
                table_desc=payload.get("table_desc") or cfg.get("table_desc") or "",
                schema_path=payload.get("schema_path") or cfg.get("schema_path") or None,
                recorrect_path=payload.get("recorrect_path") or cfg.get("recorrect_path") or None,
                top_k=int(payload.get("top_k") or cfg.get("top_k") or 5),
                enable_generate_fallback=enable_generate,
            )
            self._send_json(200, _normalize_runtime_response(query, result))
            return

        if parsed.path == "/maintenance/dedupe-valid-async":
            valid_path = payload.get("valid_path") or cfg.get("valid_path") or ""
            if not valid_path:
                self._send_json(400, {"status": "error", "error": "valid_path is required"})
                return
            task_id = start_async_valid_dedupe(valid_path)
            self._send_json(200, {
                "status": "accepted",
                "task_id": task_id,
                "query_task": f"/maintenance/task?task_id={task_id}",
            })
            return

        self._send_json(404, {"status": "error", "error": "not_found"})


def start_runtime_api_bridge(
    host: str = "127.0.0.1",
    port: int = 7862,
    valid_path: str = "",
    review_queue: str = "",
    output_dir: str = ".",
    table_name: str = "base_staff",
    table_desc: str = "",
    schema_path: str | None = None,
    recorrect_path: str | None = None,
    top_k: int = 5,
):
    global _SERVER, _SERVER_THREAD
    with _SERVER_LOCK:
        if _SERVER is not None:
            return False

        _RuntimeAPIHandler.config = {
            "valid_path": valid_path,
            "review_queue": review_queue,
            "output_dir": output_dir,
            "table_name": table_name,
            "table_desc": table_desc,
            "schema_path": schema_path,
            "recorrect_path": recorrect_path,
            "top_k": top_k,
        }

        os.makedirs(output_dir or ".", exist_ok=True)
        server = ThreadingHTTPServer((host, int(port)), _RuntimeAPIHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        _SERVER = server
        _SERVER_THREAD = thread
        return True


def stop_runtime_api_bridge():
    global _SERVER, _SERVER_THREAD
    with _SERVER_LOCK:
        if _SERVER is None:
            return
        _SERVER.shutdown()
        _SERVER.server_close()
        _SERVER = None
        _SERVER_THREAD = None
