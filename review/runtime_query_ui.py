import os
import json
import re
import threading
import uuid
import traceback
from datetime import datetime, date
from typing import Optional

try:
    import gradio as gr
except ImportError:
    gr = None

from core.utils import save_jsonl_dedup_sql, save_jsonl_upsert_sql, fill_sql_with_values, load_jsonl
from core.llm import call_llm_json
from core.database import execute_sql, db_manager
from core.config_loader import get_config_loader
from runtime.registry import APIRegistry
from runtime.router import RuntimeRouter
from review.submitter import ReviewSubmitter
from validation.intent_verify import IntentVerifier
from schema.models import APISchema


_REGISTRY_CACHE = {}
_ASYNC_TASKS = {}
_RUNTIME_CONFIG_READY = False
_VERSION_MGR = None  # Injected by ReviewInterface for binlog support


def _get_runtime_topk():
    """Read table_top_k / api_top_k from config (cached after first load)."""
    try:
        cfg = get_config_loader().load()
        return (
            getattr(cfg.runtime, 'table_top_k', 3),
            getattr(cfg.runtime, 'api_top_k', 5),
        )
    except Exception:
        return 3, 5


def set_version_manager(mgr):
    """Allow ReviewInterface to inject the VersionManager instance."""
    global _VERSION_MGR
    _VERSION_MGR = mgr


def _log_valid_write(record: dict, old_record=None, op="insert", source="runtime"):
    """Helper: log a valid dataset write to binlog if VersionManager is available."""
    if _VERSION_MGR is not None:
        _VERSION_MGR.log_operation("valid", op, record, old_record, {"source": source})


def invalidate_registry_cache():
    """Force clear the registry cache so next _get_router call rebuilds from disk."""
    _REGISTRY_CACHE.clear()


def _ensure_runtime_config_loaded():
    global _RUNTIME_CONFIG_READY
    if _RUNTIME_CONFIG_READY:
        return
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, "config.yaml")
        loader = get_config_loader(config_path)
        loader.load()
        loader.update_all_configs()
        _RUNTIME_CONFIG_READY = True
    except Exception:
        _RUNTIME_CONFIG_READY = False


def _file_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path) if path and os.path.exists(path) else 0.0
    except Exception:
        return 0.0


def _derive_recorrect_path(valid_path: str, recorrect_path: Optional[str] = None) -> str:
    if recorrect_path and recorrect_path.strip():
        return recorrect_path
    return os.path.join(os.path.dirname(valid_path) or ".", "recorrect.jsonl")


def _build_merged_registry_file(valid_path: str, recorrect_path: str) -> str:
    merged_path = os.path.join(os.path.dirname(valid_path) or ".", "runtime_registry_merged.jsonl")
    valid_records = load_jsonl(valid_path) if os.path.exists(valid_path) else []
    # Also include runtime_valid records (RAG-generated, manually corrected, etc.)
    runtime_valid_path = os.path.join(os.path.dirname(valid_path) or ".", "runtime_valid.jsonl")
    runtime_valid_records = load_jsonl(runtime_valid_path) if os.path.exists(runtime_valid_path) else []
    # Legacy: still include recorrect if it exists (backwards compat)
    recorrect_records = load_jsonl(recorrect_path) if os.path.exists(recorrect_path) else []

    # Deduplicate by normalized SQL
    seen_sql = set()
    merged_records = []
    for rec in valid_records + runtime_valid_records + recorrect_records:
        api = rec.get("api_schema") or {}
        sql = (api.get("bound_sql") or rec.get("sql") or "").strip().lower()
        norm = re.sub(r"\s+", "", sql)
        if norm and norm in seen_sql:
            continue
        if norm:
            seen_sql.add(norm)
        merged_records.append(rec)

    os.makedirs(os.path.dirname(merged_path) or ".", exist_ok=True)
    with open(merged_path, "w", encoding="utf8") as f:
        for rec in merged_records:
            f.write(json.dumps(rec, ensure_ascii=False, default=_json_default) + "\n")
    return merged_path


def _json_default(obj):
    """Handle non-JSON-serializable types (date, datetime, etc)."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if hasattr(obj, "__dict__"):
        return str(obj)
    return str(obj)


def _serialize_verification(ver):
    if ver is None:
        return None
    if hasattr(ver, "model_dump"):
        return ver.model_dump()
    if hasattr(ver, "dict"):
        return ver.dict()
    if isinstance(ver, dict):
        return ver
    return str(ver)


def _get_router(valid_path: str, review_queue: str, recorrect_path: Optional[str] = None) -> RuntimeRouter:
    recorrect = _derive_recorrect_path(valid_path, recorrect_path)
    cache_key = (valid_path, review_queue, recorrect)

    valid_mtime = _file_mtime(valid_path)
    recorrect_mtime = _file_mtime(recorrect)
    cached = _REGISTRY_CACHE.get(cache_key)
    if cached and cached.get("mtimes") == (valid_mtime, recorrect_mtime):
        return cached["router"]

    merged_registry_path = _build_merged_registry_file(valid_path, recorrect)
    registry = APIRegistry(merged_registry_path)
    submitter = ReviewSubmitter(review_queue)
    router = RuntimeRouter(registry, submitter, enable_verify=True)
    _REGISTRY_CACHE[cache_key] = {
        "router": router,
        "mtimes": (valid_mtime, recorrect_mtime),
        "merged_registry_path": merged_registry_path,
    }
    return router


def _rag_generate_from_candidates(
    query: str,
    candidates: list,
    table_name: str,
    ) -> Optional[dict]:
    """
    RAG-style generation: use top-k candidate APIs (input_schema, bound_sql,
    slot_mapping, description) as context to let LLM generate a new API + SQL
    for the given query.  Because the context comes from verified APIs, the
    generated output is grounded and should not hallucinate field names.

    Returns dict with keys: api_schema (APISchema), sql, description
    or None on failure.
    """
    if not candidates:
        return None

    # Build RAG context from candidates
    rag_examples = []
    for api in candidates:
        api_dict = api.model_dump() if hasattr(api, "model_dump") else (api.dict() if hasattr(api, "dict") else {})
        rag_examples.append({
            "name": api_dict.get("name", ""),
            "description": api_dict.get("description", ""),
            "bound_sql": api_dict.get("bound_sql", ""),
            "inputSchema": api_dict.get("inputSchema", {}),
            "slot_mapping": api_dict.get("slot_mapping", {}),
            "table": api_dict.get("table", table_name),
        })

    # ------------------------------------------------------------------
    # Extract all column names that actually appear in candidate SQLs
    # to build a strict whitelist – prevents LLM from hallucinating columns.
    # ------------------------------------------------------------------
    import re as _re_col
    _seen_columns: set[str] = set()
    _sql_kw = {
        "select", "from", "where", "and", "or", "not", "in", "is", "null",
        "like", "between", "as", "on", "join", "left", "right", "inner",
        "outer", "cross", "group", "by", "order", "having", "limit",
        "offset", "asc", "desc", "distinct", "count", "sum", "avg", "min",
        "max", "case", "when", "then", "else", "end", "cast", "concat",
        "true", "false", "union", "all", "exists", "insert", "into",
        "values", "update", "set", "delete", "create", "table", "if",
        "coalesce", "ifnull", "nullif", "over", "partition", "row_number",
    }
    for _api in rag_examples:
        _sql = _api.get("bound_sql", "")
        if not _sql:
            continue
        # Capture word tokens that look like column names (table.col or col)
        for _tok in _re_col.findall(r'(?:[\w]+\.)?(\w+)', _sql):
            _low = _tok.lower()
            # Skip SQL keywords, pure digits, slot placeholders
            if _low in _sql_kw or _tok.isdigit() or _low == table_name.lower():
                continue
            _seen_columns.add(_tok)
        # Also include inputSchema property names as valid columns
        _props = _api.get("inputSchema", {}).get("properties", {})
        for _pname in _props:
            _seen_columns.add(_pname)

    _column_whitelist = sorted(_seen_columns)

    import json as _json
    prompt = f"""You are a SQL API generator.  Given a user query and several
existing API examples from the same database table, generate a NEW API
(with bound_sql, inputSchema, slot_mapping) that correctly answers the
user query.

IMPORTANT RULES:
1. You MUST ONLY use columns from the following whitelist extracted from
   existing APIs.  Do NOT invent or guess any column name that is not in
   this list.  If a column you need does not exist here, pick the closest
   match or omit it.
   ALLOWED COLUMNS: {_json.dumps(_column_whitelist, ensure_ascii=False)}
2. The bound_sql should use `:slot` placeholders for variable parameters
   extracted from the query (e.g. :name, :city).  Fixed business logic
   conditions should NOT use placeholders.
3. LIKE patterns should use CONCAT('%', :slot, '%').
4. Output ONLY valid JSON, no explanation.

User query: {query}

Table: {table_name}

Existing API examples (for reference):
{_json.dumps(rag_examples, ensure_ascii=False, indent=2)}

Output JSON format:
{{
  "name": "api_name_in_snake_case",
  "description": "Chinese description of what this API does (generic, no concrete values)",
  "bound_sql": "SELECT ... FROM {table_name} WHERE ...",
  "inputSchema": {{
    "type": "object",
    "properties": {{"slot_name": {{"type": "string", "description": "..."}}}},
    "required": ["slot_name"]
  }},
  "slot_mapping": {{"slot_name": "slot_name"}}
}}
"""

    result = call_llm_json(prompt, retry=2)
    if not isinstance(result, dict):
        return None

    bound_sql = result.get("bound_sql")
    if not bound_sql or not isinstance(bound_sql, str) or not bound_sql.strip():
        return None

    name = result.get("name") or f"{table_name}_rag_generated"
    description = result.get("description") or query
    input_schema = result.get("inputSchema") or {"type": "object", "properties": {}, "required": []}
    slot_mapping = result.get("slot_mapping") or {}

    # Infer slot_mapping from SQL if not provided
    import re as _re
    sql_slots = list(dict.fromkeys(_re.findall(r":(\w+)", bound_sql)))
    if not slot_mapping:
        slot_mapping = {s: s for s in sql_slots}

    # Determine query_type
    sql_lower = bound_sql.lower()
    qtype = "exact_query"
    if "count(" in sql_lower or "sum(" in sql_lower or "avg(" in sql_lower:
        qtype = "aggregate"
    if "group by" in sql_lower:
        qtype = "group_aggregate"

    api_schema = APISchema(
        name=name,
        description=description,
        inputSchema=input_schema,
        outputSchema={},
        bound_sql=bound_sql,
        slot_mapping=slot_mapping,
        query_type=qtype,
        table=table_name,
        examples=[],
    )

    return {
        "api_schema": api_schema,
        "sql": bound_sql,
        "description": description,
    }


def _auto_refine(query: str, router: RuntimeRouter, attempts: int = 3):
    last = None
    for i in range(1, attempts + 1):
        last = router.route(query)
        if last.status == "success" and last.verification and last.verification.type == "CORRECT":
            return last, True
    return last, False


def _save_result(output_dir: str, filename: str, record: dict):
    path = os.path.join(output_dir, filename)
    written = save_jsonl_dedup_sql(path, record)
    return path, written


def _to_dict(obj):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict"):
        return obj.dict()
    return None


def _submit_existing_path_review_task(
    query: str,
    submitter: ReviewSubmitter,
    wrong_api: Optional[dict],
    candidate_tables: list[str],
    reason: str,
    invoked_sql: str = "",
    params: Optional[dict] = None,
):
    instruction = f"runtime_api_primary_failed: {reason}"
    try:
        return submitter.submit_runtime_correction(
            query=query,
            correct_api=None,
            wrong_api=wrong_api,
            distinction_instruction=instruction,
            candidate_tables=candidate_tables,
            invoked_sql=invoked_sql,
            params=params,
        )
    except Exception:
        return None


def _runtime_existing_path_once(
    query: str,
    valid_path: str,
    review_queue: str,
    output_dir: str,
    recorrect_path: Optional[str] = None,
    top_k: int = 5,
):
    router = _get_router(valid_path, review_queue, recorrect_path=recorrect_path)
    submitter = ReviewSubmitter(review_queue)
    verifier = IntentVerifier()

    table_top_k, _ = _get_runtime_topk()
    candidate_tables = router.registry.get_candidate_tables(query, top_k=table_top_k)
    selected_table = candidate_tables[0] if candidate_tables else None
    candidates = router.recaller.recall(query, table_hint=selected_table)
    candidates = candidates[: max(1, top_k)]

    if not candidates:
        task_id = _submit_existing_path_review_task(
            query=query,
            submitter=submitter,
            wrong_api=None,
            candidate_tables=candidate_tables,
            reason="no_api_candidates",
        )
        return {
            "status": "error",
            "stage": "existing_api",
            "candidate_tables": candidate_tables,
            "selected_table": selected_table,
            "topk_api_names": [],
            "reason": "no_api_candidates",
            "review_task_id": task_id,
        }

    best_api = router.recaller.select_best(query, candidates)
    if not best_api:
        task_id = _submit_existing_path_review_task(
            query=query,
            submitter=submitter,
            wrong_api=None,
            candidate_tables=candidate_tables,
            reason="select_best_failed",
        )
        return {
            "status": "error",
            "stage": "existing_api",
            "candidate_tables": candidate_tables,
            "selected_table": selected_table,
            "topk_api_names": [c.name for c in candidates],
            "reason": "select_best_failed",
            "review_task_id": task_id,
        }

    params = router.slot_filler.fill(query, best_api) or {}
    valid, missing = router.slot_filler.validate(params, best_api)
    if not valid:
        task_id = _submit_existing_path_review_task(
            query=query,
            submitter=submitter,
            wrong_api=_to_dict(best_api),
            candidate_tables=candidate_tables,
            reason=f"missing_slots:{missing}",
        )
        return {
            "status": "error",
            "stage": "existing_api",
            "candidate_tables": candidate_tables,
            "selected_table": selected_table,
            "topk_api_names": [c.name for c in candidates],
            "reason": "missing_slots",
            "missing_slots": missing,
            "api_schema": _to_dict(best_api),
            "review_task_id": task_id,
        }

    invoked_sql = fill_sql_with_values(best_api.bound_sql, params)
    exec_result = execute_sql(None, invoked_sql)
    verify_ok = exec_result.get("status") == "success" and verifier.verify(query, invoked_sql, exec_result)
    if not verify_ok:
        reason = exec_result.get("error") if exec_result.get("status") != "success" else "intent_verify_failed"
        task_id = _submit_existing_path_review_task(
            query=query,
            submitter=submitter,
            wrong_api=_to_dict(best_api),
            candidate_tables=candidate_tables,
            reason=reason,
            invoked_sql=invoked_sql,
            params=params,
        )
        return {
            "status": "error",
            "stage": "existing_api",
            "candidate_tables": candidate_tables,
            "selected_table": selected_table,
            "topk_api_names": [c.name for c in candidates],
            "reason": reason,
            "api_schema": _to_dict(best_api),
            "params": params,
            "invoked_sql": invoked_sql,
            "exec_result": exec_result,
            "review_task_id": task_id,
        }

    return {
        "status": "success",
        "stage": "existing_api",
        "candidate_tables": candidate_tables,
        "selected_table": selected_table,
        "topk_api_names": [c.name for c in candidates],
        "api_schema": _to_dict(best_api),
        "params": params,
        "invoked_sql": invoked_sql,
        "exec_result": exec_result,
    }


def _runtime_generate_path_once(
    query: str,
    table_name: str,
    table_desc: str,
    valid_path: str,
    review_queue: str,
    output_dir: str,
    schema_path: Optional[str] = None,
):
    router = _get_router(valid_path, review_queue)
    verifier = IntentVerifier()

    # Use RAG generation from existing candidates instead of schema-based fallback
    table_top_k, _ = _get_runtime_topk()
    candidate_tables = router.registry.get_candidate_tables(query, top_k=table_top_k)
    selected_table = candidate_tables[0] if candidate_tables else (table_name or "")
    candidates = router.recaller.recall(query, table_hint=selected_table)
    candidates = candidates[:5]

    fb = _rag_generate_from_candidates(query, candidates, selected_table)
    if not fb or not fb.get("api_schema"):
        return {
            "status": "error",
            "stage": "generate_api",
            "reason": "fallback_generate_failed",
        }

    api_schema = fb.get("api_schema")
    params = router.slot_filler.fill(query, api_schema) or {}
    bound_sql = getattr(api_schema, "bound_sql", "") or fb.get("sql") or ""
    invoked_sql = fill_sql_with_values(bound_sql, params)
    exec_result = execute_sql(None, invoked_sql)
    verify_ok = exec_result.get("status") == "success" and verifier.verify(query, invoked_sql, exec_result)

    if not verify_ok:
        reason = exec_result.get("error") if exec_result.get("status") != "success" else "intent_verify_failed"
        return {
            "status": "error",
            "stage": "generate_api",
            "reason": reason,
            "api_schema": _to_dict(api_schema),
            "params": params,
            "invoked_sql": invoked_sql,
            "exec_result": exec_result,
        }

    generated_record = {
        "query": query,
        "api_schema": _to_dict(api_schema),
        "params": params,
        "invoked_sql": invoked_sql,
        "exec_result": exec_result,
        "source": "runtime_generated",
        "source_stage": "runtime",
        "source_method": "realtime_generate",
        "source_channel": "runtime_api",
        "review_status": "auto_pass",
        "created_at": datetime.now().isoformat(),
    }
    _save_result(output_dir, "runtime_generated.jsonl", generated_record)
    save_jsonl_dedup_sql(valid_path, generated_record)
    _log_valid_write(generated_record, source="realtime_generate")

    return {
        "status": "success",
        "stage": "generate_api",
        "api_schema": _to_dict(api_schema),
        "params": params,
        "invoked_sql": invoked_sql,
        "exec_result": exec_result,
    }


def run_runtime_api_pipeline(
    query: str,
    valid_path: str,
    review_queue: str,
    output_dir: str,
    table_name: str,
    table_desc: str,
    schema_path: Optional[str] = None,
    recorrect_path: Optional[str] = None,
    top_k: int = 5,
    enable_generate_fallback: bool = False,
):
    _ensure_runtime_config_loaded()
    db_manager.connect()

    primary = _runtime_existing_path_once(
        query=query,
        valid_path=valid_path,
        review_queue=review_queue,
        output_dir=output_dir,
        recorrect_path=recorrect_path,
        top_k=top_k,
    )
    if primary.get("status") == "success":
        return {
            "status": "success",
            "path": "existing_api",
            "result": primary,
        }

    generated = None
    if enable_generate_fallback:
        table_for_generate = primary.get("selected_table") or table_name
        generated = _runtime_generate_path_once(
            query=query,
            table_name=table_for_generate,
            table_desc=table_desc,
            valid_path=valid_path,
            review_queue=review_queue,
            output_dir=output_dir,
            schema_path=schema_path,
        )
        if generated.get("status") == "success":
            return {
                "status": "success",
                "path": "generate_api",
                "result": generated,
                "primary_failed": primary,
            }

    invalid_rec = {
        "source": "runtime_api",
        "query": query,
        "status": "error",
        "error": primary.get("reason") or "runtime_pipeline_failed",
        "review_status": "needs_manual",
        "source_stage": "runtime",
        "source_method": "pipeline_failed",
        "source_channel": "runtime_api",
        "primary_failed": primary,
        "generate_failed": generated,
        "created_at": datetime.now().isoformat(),
    }
    _save_result(output_dir, "runtime_invalid.jsonl", invalid_rec)

    return {
        "status": "error",
        "path": "none",
        "result": invalid_rec,
    }


def _dedupe_valid_file(valid_path: str):
    records = load_jsonl(valid_path) if os.path.exists(valid_path) else []
    latest_by_key = {}
    for idx, rec in enumerate(records):
        api = rec.get("api_schema") or {}
        name = api.get("name") or rec.get("api_name") or ""
        sql = (api.get("bound_sql") or rec.get("sql") or "").strip()
        norm_sql = re.sub(r"\s+", "", sql).lower()
        key = (name, norm_sql)
        latest_by_key[key] = (idx, rec)

    deduped = [item[1] for item in sorted(latest_by_key.values(), key=lambda x: x[0])]
    removed = max(0, len(records) - len(deduped))

    with open(valid_path, "w", encoding="utf8") as f:
        for rec in deduped:
            f.write(json.dumps(rec, ensure_ascii=False, default=_json_default) + "\n")

    return {
        "original": len(records),
        "remaining": len(deduped),
        "removed": removed,
    }


def start_async_valid_dedupe(valid_path: str) -> str:
    task_id = f"dedupe_{uuid.uuid4().hex[:12]}"
    _ASYNC_TASKS[task_id] = {
        "task_id": task_id,
        "status": "running",
        "valid_path": valid_path,
        "started_at": datetime.now().isoformat(),
    }

    def _worker():
        try:
            stats = _dedupe_valid_file(valid_path)
            _ASYNC_TASKS[task_id].update({
                "status": "done",
                "finished_at": datetime.now().isoformat(),
                "stats": stats,
            })
        except Exception as e:
            _ASYNC_TASKS[task_id].update({
                "status": "failed",
                "finished_at": datetime.now().isoformat(),
                "error": str(e),
                "traceback": traceback.format_exc(),
            })

    threading.Thread(target=_worker, daemon=True).start()
    return task_id


def get_async_task_status(task_id: str) -> dict:
    return _ASYNC_TASKS.get(task_id, {"task_id": task_id, "status": "not_found"})


def _parse_json_text(text: str, default):
    if not text or not str(text).strip():
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def _extract_slots(sql: str) -> list[str]:
    return list(dict.fromkeys(re.findall(r":(\w+)", sql or "")))


def _infer_input_schema_from_sql_str(sql: str) -> str:
    """从SQL中提取:slot参数，推断inputSchema，返回JSON字符串。"""
    slots = _extract_slots(sql)
    properties = {}
    required = []
    for slot in slots:
        slot_lower = slot.lower()
        if slot_lower.endswith("_id") or slot_lower in {"id", "age", "year", "month", "day", "count", "num", "number"}:
            slot_type = "integer"
        elif any(k in slot_lower for k in ["amount", "price", "salary", "score", "rate", "ratio", "percent"]):
            slot_type = "number"
        else:
            slot_type = "string"
        properties[slot] = {"type": slot_type, "description": f"筛选参数：{slot}"}
        required.append(slot)
    schema = {"type": "object", "properties": properties, "required": required}
    return json.dumps(schema, ensure_ascii=False, indent=2)


def _extract_query_value_candidates(query: str) -> list[str]:
    text = (query or "").strip()
    if not text:
        return []

    values = []
    patterns = [
        r"查询([\u4e00-\u9fa5A-Za-z0-9_·]{1,20})的信息",
        r"查一下([\u4e00-\u9fa5A-Za-z0-9_·]{1,20})的信息",
        r"([\u4e00-\u9fa5A-Za-z0-9_·]{1,20})的(信息|资料|记录|详情)",
        r"([\u4e00-\u9fa5A-Za-z0-9_·]{1,20})地区",
        r"([\u4e00-\u9fa5A-Za-z0-9_·]{1,20})部门",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            candidate = (match.group(1) or "").strip()
            if candidate and candidate not in values:
                values.append(candidate)

    if not values:
        for token in re.findall(r"[\u4e00-\u9fa5A-Za-z0-9_·]{2,20}", text):
            if token in {
                "查询", "查一下", "帮我", "请", "信息", "资料", "记录", "详情", "员工",
                "地区", "部门", "人数", "数量", "总数", "名单", "列表", "最新", "最近",
            }:
                continue
            if token not in values:
                values.append(token)
            if len(values) >= 3:
                break
    return values


def _anonymize_text_with_values(text: str, values: list[str]) -> str:
    result = text or ""
    for value in sorted([v for v in values if v], key=len, reverse=True):
        result = result.replace(value, "某")
    result = re.sub(r"某+", "某", result)
    return result


def _llm_parameterize_sql_and_desc(sql: str, query: str, api_desc: str) -> tuple[Optional[str], Optional[str]]:
    """让LLM从query中识别参数并产出参数化SQL与去值化描述。"""
    if not (sql or "").strip():
        return None, None

    prompt = f"""
你是SQL参数化助手。给定用户query、人工编辑SQL、以及当前API描述，请完成：
1) 识别query里的可变参数值；
2) 将SQL中的这些具体值改为:slot占位符，产出bound_sql；
3) 产出不包含具体值的api_desc（泛化描述）。

要求：
- 只替换真正来自query的变量值，固定业务条件不要替换；
- slot命名优先使用对应字段名（如name_formal、city、department），必须是英文/数字/下划线；
- like '%值%' 这种场景请改为 CONCAT('%', :slot, '%')；
- api_desc不要出现具体人名、地名、编号等值；
- 仅输出JSON，不要解释。

query:
{query}

api_desc:
{api_desc}

sql:
{sql}

输出JSON格式：
{{
  "bound_sql": "...",
  "api_desc": "..."
}}
""".strip()

    try:
        result = call_llm_json(prompt, retry=2)
    except Exception:
        result = None

    if not isinstance(result, dict):
        return None, None

    bound_sql = result.get("bound_sql")
    normalized_desc = result.get("api_desc")

    if not isinstance(bound_sql, str) or not bound_sql.strip():
        bound_sql = None
    if not isinstance(normalized_desc, str) or not normalized_desc.strip():
        normalized_desc = None

    if bound_sql:
        slots = _extract_slots(bound_sql)
        if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", slot or "") for slot in slots):
            bound_sql = None

    return bound_sql, normalized_desc


def _extract_literals_from_sql(sql: str) -> list[str]:
    """从SQL中提取所有字面值（单引号内的字符串）。"""
    literals = []
    for match in re.finditer(r"'([^']*)'", sql or ""):
        literal = match.group(1)
        if literal and literal not in literals:
            literals.append(literal)
    return literals


def _fallback_fill_params_from_manual_sql(
    manual_sql: str,
    bound_sql: str,
    required_slots: list[str]
) -> dict:
    """从manual_sql的字面值反推到槽位，作为兜底填充。"""
    literals = _extract_literals_from_sql(manual_sql)
    if not required_slots or not literals:
        return {}

    result = {}
    for idx, slot in enumerate(required_slots):
        if idx < len(literals):
            result[slot] = literals[idx]
    return result


def _parameterize_sql_by_query(sql: str, query: str) -> tuple[str, list[str]]:
    raw_sql = sql or ""
    if not raw_sql.strip():
        return raw_sql, []

    value_candidates = _extract_query_value_candidates(query)
    if not value_candidates:
        return raw_sql, []

    slot_names = []

    def _replacement(match: re.Match) -> str:
        literal = match.group(1) or ""
        core = literal.strip("%")
        if not core:
            return match.group(0)
        if core not in value_candidates and literal not in value_candidates:
            return match.group(0)

        prefix_sql = raw_sql[:match.start()]
        col_match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*(?:=|<>|!=|>=|<=|>|<|like)\s*$", prefix_sql, flags=re.IGNORECASE)
        slot = (col_match.group(1) if col_match else "param").lower()
        if slot in slot_names:
            idx = 2
            while f"{slot}_{idx}" in slot_names:
                idx += 1
            slot = f"{slot}_{idx}"
        slot_names.append(slot)

        is_like = bool(re.search(r"like\s*$", prefix_sql, flags=re.IGNORECASE))
        if is_like:
            starts_pct = literal.startswith("%")
            ends_pct = literal.endswith("%")
            if starts_pct and ends_pct:
                return f"CONCAT('%', :{slot}, '%')"
            if starts_pct:
                return f"CONCAT('%', :{slot})"
            if ends_pct:
                return f"CONCAT(:{slot}, '%')"
        return f":{slot}"

    templated_sql = re.sub(r"'([^']*)'", _replacement, raw_sql)
    return templated_sql, slot_names


def _build_api_schema_from_inputs(
    table_name: str,
    query: str,
    api_name: str,
    api_desc: str,
    input_schema_text: str,
    sql: str,
) -> APISchema:
    llm_templated_sql, llm_desc = _llm_parameterize_sql_and_desc(sql, query, api_desc)
    if llm_templated_sql:
        templated_sql = llm_templated_sql
    else:
        templated_sql, _ = _parameterize_sql_by_query(sql, query)

    # 优先从SQL推断inputSchema，忽略手工填写的input_schema_text
    input_schema = _parse_json_text(_infer_input_schema_from_sql_str(templated_sql), {"type": "object", "properties": {}, "required": []})
    if not isinstance(input_schema, dict):
        input_schema = {"type": "object", "properties": {}, "required": []}

    slots = _extract_slots(templated_sql)
    slot_mapping = {slot: slot for slot in slots}

    qtype = "exact_query"
    sql_lower = (templated_sql or "").lower()
    if "count(" in sql_lower or "sum(" in sql_lower or "avg(" in sql_lower:
        qtype = "aggregate"
    if "group by" in sql_lower:
        qtype = "group_aggregate"

    sanitized_desc = (llm_desc or "").strip()
    if not sanitized_desc:
        query_values = _extract_query_value_candidates(query)
        sanitized_desc = _anonymize_text_with_values((api_desc or "").strip(), query_values)
    if not sanitized_desc:
        sanitized_query = _anonymize_text_with_values(query, _extract_query_value_candidates(query))
        sanitized_desc = _infer_api_desc_from_query(sanitized_query, templated_sql)
    if not sanitized_desc:
        sanitized_desc = "查询相关信息"

    return APISchema(
        name=(api_name or "runtime_manual_api").strip(),
        description=sanitized_desc,
        inputSchema=input_schema,
        outputSchema={},
        bound_sql=templated_sql or "",
        slot_mapping=slot_mapping,
        query_type=qtype,
        table=table_name or "base_staff",
        examples=[],
    )


def run_manual_api_sql(
    query: str,
    valid_path: str,
    table_name: str,
    output_dir: str,
    review_queue: str,
    api_name: str,
    api_desc: str,
    input_schema_text: str,
    sql: str,
):
    if not query.strip():
        return "请输入query", "", "", ""
    if not (sql or "").strip():
        return "请输入SQL", "", "", ""

    db_manager.connect()

    if not output_dir:
        output_dir = os.path.dirname(valid_path) or "."

    router = _get_router(valid_path, review_queue)
    intent_verifier = IntentVerifier()

    manual_sql = (sql or "").strip()
    manual_exec_result = execute_sql(None, manual_sql)
    if manual_exec_result.get("status") != "success":
        rec = {
            "source": "runtime_manual",
            "query": query,
            "api_name": (api_name or "").strip() or "runtime_manual_api",
            "sql": manual_sql,
            "status": "error",
            "error": manual_exec_result.get("error"),
            "verification": {
                "pass": False,
                "reason": "manual_sql_execute_failed",
            },
            "runtime_source": "query_ui_manual",
            "review_status": "needs_manual",
            "review_method": "manual_sql_first",
            "created_at": datetime.now().isoformat(),
        }
        _, written = _save_result(output_dir, "runtime_invalid.jsonl", rec)
        note = (
            f"手工SQL执行失败：{manual_exec_result.get('error')}"
            + ("；写入 runtime_invalid.jsonl" if written else "；跳过写入 runtime_invalid.jsonl（重复SQL）")
        )
        return "失败（手工SQL不可执行）", manual_sql, json.dumps(rec, ensure_ascii=False, indent=2), note

    api_schema = _build_api_schema_from_inputs(table_name, query, api_name, api_desc, input_schema_text, manual_sql)
    params = {}
    fill_error = None
    try:
        params = router.slot_filler.fill(query, api_schema) or {}
    except Exception as e:
        import traceback
        fill_error = f"slot_filler.fill异常: {str(e)}"
        print(f"[WARN] {fill_error}\n{traceback.format_exc()}")

    required_slots = api_schema.inputSchema.get("required", []) if isinstance(api_schema.inputSchema, dict) else []
    if not required_slots:
        required_slots = list((api_schema.slot_mapping or {}).keys())
    missing_slots = [
        slot for slot in required_slots
        if slot not in params or params.get(slot) is None or (isinstance(params.get(slot), str) and not params.get(slot).strip())
    ]

    if missing_slots:
        fallback_params = _fallback_fill_params_from_manual_sql(manual_sql, api_schema.bound_sql, required_slots)
        filled_count = 0
        for slot, val in fallback_params.items():
            if slot in missing_slots:
                params[slot] = val
                missing_slots.remove(slot)
                filled_count += 1
        if filled_count > 0:
            fill_error = f"(通过manual_sql字面值兜底填充了{filled_count}个槽位) " + (fill_error or "")

    if missing_slots:
        rec = {
            "source": "runtime_manual",
            "query": query,
            "api_name": api_schema.name,
            "api_schema": api_schema.model_dump(),
            "sql": api_schema.bound_sql,
            "manual_sql": manual_sql,
            "roundtrip_sql": "",
            "params": params,
            "fill_diagnostic": fill_error or "未知错误",
            "status": "error",
            "manual_sql_exec": {
                "status": manual_exec_result.get("status"),
                "error": manual_exec_result.get("error"),
                "row_count": manual_exec_result.get("row_count"),
            },
            "roundtrip_exec": {
                "status": "skipped",
                "error": f"缺少槽位参数: {missing_slots}" + (f"; {fill_error}" if fill_error else ""),
                "row_count": None,
            },
            "row_count": None,
            "columns": None,
            "data": None,
            "verification": {
                "pass": False,
                "reason": "missing_slot_values",
            },
            "runtime_source": "query_ui_manual",
            "review_status": "needs_manual",
            "review_method": "manual_api_sql",
            "created_at": datetime.now().isoformat(),
        }
        _, written = _save_result(output_dir, "runtime_invalid.jsonl", rec)
        final_note = (
            f"手工SQL可执行，但参数提取有问题，缺少槽位: {', '.join(missing_slots)}"
            + (f". 诊断: {fill_error}" if fill_error else "")
            + ("；写入 runtime_invalid.jsonl" if written else "；跳过写入 runtime_invalid.jsonl（重复SQL）")
        )
        return "失败（缺少参数槽位）", api_schema.bound_sql, json.dumps(rec, ensure_ascii=False, indent=2, default=_json_default), final_note

    roundtrip_sql = fill_sql_with_values(api_schema.bound_sql, params)
    roundtrip_result = execute_sql(None, roundtrip_sql)

    roundtrip_ok = roundtrip_result.get("status") == "success"
    verify_ok = roundtrip_ok and intent_verifier.verify(query, roundtrip_sql, roundtrip_result)
    pass_flag = roundtrip_ok and verify_ok

    rec = {
        "source": "runtime_manual",
        "query": query,
        "api_name": api_schema.name,
        "api_schema": api_schema.model_dump(),
        "sql": api_schema.bound_sql,
        "manual_sql": manual_sql,
        "roundtrip_sql": roundtrip_sql,
        "params": params,
        "status": "success" if pass_flag else "error",
        "manual_sql_exec": {
            "status": manual_exec_result.get("status"),
            "error": manual_exec_result.get("error"),
            "row_count": manual_exec_result.get("row_count"),
        },
        "roundtrip_exec": {
            "status": roundtrip_result.get("status"),
            "error": roundtrip_result.get("error"),
            "row_count": roundtrip_result.get("row_count"),
        },
        "row_count": roundtrip_result.get("row_count"),
        "columns": roundtrip_result.get("columns"),
        "data": roundtrip_result.get("data"),
        "verification": {
            "pass": pass_flag,
            "reason": (
                "manual_sql_ok_roundtrip_ok"
                if pass_flag
                else ("roundtrip_sql_execute_failed" if not roundtrip_ok else "intent_verify_failed")
            ),
        },
        "runtime_source": "query_ui_manual",
        "review_status": "auto_pass" if pass_flag else "needs_manual",
        "review_method": "manual_api_sql",
        "source_stage": "runtime",
        "source_method": "manual_api_sql",
        "source_channel": "runtime_query_tab",
        "created_at": datetime.now().isoformat(),
    }

    if pass_flag:
        _, written = _save_result(output_dir, "runtime_valid.jsonl", rec)
        note = (
            "手工SQL可执行，round-trip通过；写入 runtime_valid.jsonl"
            if written
            else "手工SQL可执行，round-trip通过；跳过写入 runtime_valid.jsonl（重复SQL）"
        )
        return "通过（手工SQL+round-trip）", roundtrip_sql, json.dumps(rec, ensure_ascii=False, indent=2, default=_json_default), note

    _, written = _save_result(output_dir, "runtime_invalid.jsonl", rec)
    if not roundtrip_ok:
        reason_note = f"手工SQL可执行，但round-trip SQL执行失败：{roundtrip_result.get('error')}"
    else:
        reason_note = "手工SQL可执行，round-trip SQL可执行，但语义校验未通过"
    note = reason_note + ("；写入 runtime_invalid.jsonl" if written else "；跳过写入 runtime_invalid.jsonl（重复SQL）")
    return "失败（round-trip未通过）", roundtrip_sql, json.dumps(rec, ensure_ascii=False, indent=2, default=_json_default), note


def _expand_query_variants(base_query: str, h_count: int = 3, v_count: int = 3) -> tuple[list[str], list[str]]:
    q = (base_query or "").strip()
    if not q:
        return [], []

    horizontal_templates = [
        "{}",
        "请帮我查一下{}",
        "帮我统计{}",
        "想看一下{}",
        "{}，麻烦给下结果",
        "查询一下{}",
    ]
    vertical_templates = [
        "{}（仅在职）",
        "{}（按部门拆分）",
        "{}（最近30天）",
        "{}（按职级拆分）",
        "{}（只看正式员工）",
        "{}（只看离职员工）",
    ]

    horizontal = []
    for tmpl in horizontal_templates:
        variant = tmpl.format(q).strip()
        if variant not in horizontal:
            horizontal.append(variant)
        if len(horizontal) >= max(1, h_count):
            break

    vertical = []
    for tmpl in vertical_templates:
        variant = tmpl.format(q).strip()
        if variant not in vertical:
            vertical.append(variant)
        if len(vertical) >= max(1, v_count):
            break

    return horizontal, vertical


def expand_and_test_queries(
    query: str,
    valid_path: str,
    table_name: str,
    table_desc: str,
    output_dir: str,
    review_queue: str,
    schema_path: Optional[str],
    horizontal_count: int = 3,
    vertical_count: int = 3,
):
    try:
        if not query.strip():
            return "请输入query", "", "", ""

        # 确保参数类型正确
        horizontal_count = int(horizontal_count) if horizontal_count else 3
        vertical_count = int(vertical_count) if vertical_count else 3

        horizontal, vertical = _expand_query_variants(query, horizontal_count, vertical_count)
        all_queries = []
        for q in horizontal + vertical:
            if q not in all_queries:
                all_queries.append(q)

        summary = []
        details = []
        for idx, q in enumerate(all_queries, 1):
            status, sql, record_json, note = run_runtime_query(
                query=q,
                valid_path=valid_path,
                table_name=table_name,
                table_desc=table_desc,
                output_dir=output_dir,
                review_queue=review_queue,
                schema_path=schema_path,
            )
            summary.append(f"{idx}. {status} | {q}")
            details.append({
                "query": q,
                "status": status,
                "sql": sql,
                "note": note,
                "record": _parse_json_text(record_json, {"raw": record_json}),
            })

        merged_sql = "\n\n".join([f"-- {d['query']}\n{d['sql']}" for d in details if d.get("sql")])
        merged_record = json.dumps({"summary": summary, "details": details}, ensure_ascii=False, indent=2)
        return "扩展测试完成", merged_sql, merged_record, "\n".join(summary)
    except Exception as e:
        import traceback
        error_msg = f"扩展测试异常: {str(e)}\n{traceback.format_exc()}"
        return error_msg, "", json.dumps({"error": str(e)}, ensure_ascii=False), traceback.format_exc()[:500]


def import_final_to_valid(
    query: str,
    table_name: str,
    valid_path: str,
    api_name: str,
    api_desc: str,
    input_schema_text: str,
    sql: str,
):
    if not query.strip():
        return "请输入query"
    if not (sql or "").strip():
        return "请输入SQL"

    api_schema = _build_api_schema_from_inputs(table_name, query, api_name, api_desc, input_schema_text, sql)
    record = {
        "query": query,
        "api_schema": api_schema.model_dump(),
        "reviewed_at": datetime.now().isoformat(),
        "source": "runtime_manual_finalize",
        "from_runtime_correction": True,
        "source_stage": "runtime",
        "source_method": "manual_finalize",
        "source_channel": "runtime_query_tab",
    }
    # Use upsert: if same SQL/query exists, replace old record with new one
    save_jsonl_upsert_sql(valid_path, record)
    _log_valid_write(record, source="manual_finalize")
    # Force invalidate registry cache so next runtime query picks up the new record
    invalidate_registry_cache()
    return f"已导入到valid（已覆盖旧记录如有）: {valid_path}"


def _infer_api_desc_from_query(query: str, sql: str = "") -> str:
    query_text = (query or "").strip()
    if not query_text:
        return ""

    query_text = query_text.strip("。！？!?；;，,")
    prefixes = [
        "请帮我查一下", "请帮我查询", "帮我查一下", "帮我查询", "请帮我", "帮我",
        "请查询", "查询一下", "查一下", "查查", "看一下", "想看一下", "我想看一下", "请",
    ]
    for prefix in prefixes:
        if query_text.startswith(prefix):
            query_text = query_text[len(prefix):].strip()
            break

    sql_lower = (sql or "").lower()
    if any(token in query_text for token in ["多少", "几", "人数", "数量", "总数"]) or "count(" in sql_lower:
        query_text = query_text.replace("有多少", "")
        query_text = query_text.replace("多少", "")
        query_text = query_text.replace("几", "")
        query_text = query_text.replace("人数", "员工人数") if query_text == "人数" else query_text
        query_text = query_text.strip()
        if not query_text:
            return "统计相关数量"
        if query_text.endswith("员工"):
            return f"统计{query_text}数量"
        if query_text.endswith("人"):
            return f"统计{query_text}数"
        if query_text.endswith("数量") or query_text.endswith("总数"):
            return f"统计{query_text}"
        return f"统计{query_text}数量"

    if "order by" in sql_lower and "limit" in sql_lower:
        if "最新" in query_text or "最近" in query_text:
            return f"查询{query_text}"
        return f"查询最新{query_text}"

    if any(word in query_text for word in ["哪些", "哪几个", "哪几位", "谁", "名单", "列表"]):
        return f"查询{query_text}"

    if query_text.endswith(("信息", "情况", "记录", "列表", "明细", "结果")):
        return f"查询{query_text}"

    return f"查询{query_text}相关信息"


def fill_form_from_record(record_json: str, sql_fallback: str = ""):
    data = _parse_json_text(record_json, {})
    if not isinstance(data, dict):
        return "", "", "{}", sql_fallback

    api_schema = data.get("api_schema") or {}
    api_name = data.get("api_name") or api_schema.get("name") or ""
    sql = data.get("sql") or api_schema.get("bound_sql") or sql_fallback or ""
    query = data.get("query") or ""
    # 从SQL推断inputSchema（不使用存储的旧schema）
    input_schema_str = _infer_input_schema_from_sql_str(sql)

    api_desc = (
        api_schema.get("description")
        or data.get("api_desc")
        or data.get("description")
        or ""
    )
    if not api_desc:
        api_desc = _infer_api_desc_from_query(query, sql)
    if not api_desc:
        if sql:
            api_desc = "查询相关信息"
        elif api_name:
            api_desc = "查询相关能力"
        else:
            api_desc = "查询相关信息"

    return api_name, api_desc, input_schema_str, sql


def run_runtime_query(
    query: str,
    valid_path: str,
    table_name: str,
    table_desc: str,
    output_dir: str,
    review_queue: str,
    schema_path: Optional[str] = None,
):
    if not query.strip():
        return "请输入query", "", "", ""

    # 每次运行前确保数据库连接可用（连接失败时仍继续，交由执行结果判定）
    db_manager.connect()

    if not output_dir:
        output_dir = os.path.dirname(valid_path) or "."

    router = _get_router(valid_path, review_queue)
    intent_verifier = IntentVerifier()

    result = router.route(query)
    if result.status == "success":
        pass_flag = intent_verifier.verify(query, result.invoked_sql or "", result.exec_result or {})
        verification_info = _serialize_verification(getattr(result, "verification", None))
        if pass_flag:
            rec = {
                "source": "runtime",
                "query": query,
                "api_name": result.api_name,
                "sql": result.invoked_sql,
                "params": result.params,
                "status": result.status,
                "row_count": result.row_count,
                "columns": result.columns,
                "data": result.data,
                "verification": {
                    "pass": True,
                    "reason": verification_info,
                },
                "runtime_source": "query_ui",
                "review_status": "auto_pass",
                "review_method": "initial_verify",
                "source_stage": "runtime",
                "source_method": "initial_verify",
                "source_channel": "runtime_query_tab",
                "created_at": datetime.now().isoformat(),
            }
            _, written = _save_result(output_dir, "runtime_valid.jsonl", rec)
            note = "写入 runtime_valid.jsonl" if written else "跳过写入 runtime_valid.jsonl（重复SQL）"
            return "通过（主路由）", result.invoked_sql or "", json.dumps(rec, ensure_ascii=False, indent=2, default=_json_default), note

    # --- Layer 2: RAG generation from top-k candidates ---
    # Gather top-k candidate APIs from the primary route attempt for RAG context
    table_top_k, _ = _get_runtime_topk()
    candidate_tables = router.registry.get_candidate_tables(query, top_k=table_top_k)
    selected_table = candidate_tables[0] if candidate_tables else (table_name or "")
    candidates = router.recaller.recall(query, table_hint=selected_table)
    candidates = candidates[:5]  # top-5

    rag_result = _rag_generate_from_candidates(query, candidates, selected_table)
    if rag_result and rag_result.get("api_schema"):
        api_schema = rag_result["api_schema"]
        params = router.slot_filler.fill(query, api_schema) or {}
        try:
            exec_sql = fill_sql_with_values(api_schema.bound_sql, params)
        except Exception:
            exec_sql = rag_result.get("sql") or api_schema.bound_sql

        exec_result = execute_sql(None, exec_sql)
        pass_flag = exec_result.get("status") == "success" and intent_verifier.verify(query, exec_sql, exec_result)

        rec = {
            "source": "rag_generate",
            "query": query,
            "api_name": api_schema.name,
            "api_schema": api_schema.model_dump(),
            "sql": exec_sql,
            "params": params,
            "status": "success" if exec_result.get("status") == "success" else "error",
            "row_count": exec_result.get("row_count"),
            "columns": exec_result.get("columns"),
            "data": exec_result.get("data"),
            "verification": {
                "pass": pass_flag,
                "reason": rag_result.get("description") if pass_flag else exec_result.get("error") or rag_result.get("description"),
            },
            "runtime_source": "query_ui_rag",
            "review_status": "auto_pass" if pass_flag else "needs_manual",
            "review_method": "rag_generate",
            "source_stage": "runtime",
            "source_method": "rag_generate",
            "source_channel": "runtime_query_tab",
            "rag_candidate_count": len(candidates),
            "rag_candidate_names": [getattr(c, "name", "") for c in candidates],
            "created_at": datetime.now().isoformat(),
        }

        if pass_flag:
            _, written = _save_result(output_dir, "runtime_valid.jsonl", rec)
            note = "写入 runtime_valid.jsonl" if written else "跳过写入 runtime_valid.jsonl（重复SQL）"
            return "通过（RAG生成）", exec_sql, json.dumps(rec, ensure_ascii=False, indent=2, default=_json_default), note

        _, written = _save_result(output_dir, "runtime_invalid.jsonl", rec)
        note = "写入 runtime_invalid.jsonl" if written else "跳过写入 runtime_invalid.jsonl（重复SQL）"
        return "失败（RAG生成未通过校验）", exec_sql, json.dumps(rec, ensure_ascii=False, indent=2, default=_json_default), note

    # --- No candidates at all ---
    rec = {
        "source": "runtime",
        "query": query,
        "status": result.status,
        "error": result.error,
        "review_status": "needs_manual",
        "runtime_source": "query_ui",
        "created_at": datetime.now().isoformat(),
    }
    _, written = _save_result(output_dir, "runtime_invalid.jsonl", rec)
    note = "写入 runtime_invalid.jsonl" if written else "跳过写入 runtime_invalid.jsonl（重复SQL）"
    return "失败（未命中可用方案）", "", json.dumps(rec, ensure_ascii=False, indent=2, default=_json_default), note


def main():
    if gr is None:
        print("Gradio not available, please install gradio")
        return

    with gr.Blocks(title="Runtime Query UI") as demo:
        gr.Markdown("# Runtime Query UI\n输入 query，走 runtime 自调整 + 自检查流程")

        with gr.Row():
            query_input = gr.Textbox(label="Query", lines=2)
        with gr.Row():
            valid_path = gr.Textbox(label="valid.jsonl Path", value="output/base_staff/valid.jsonl")
            table_name = gr.Textbox(label="Table Name", value="base_staff")
        with gr.Row():
            table_desc = gr.Textbox(label="Table Description", value="员工信息表，包括姓名、城市、部门、状态、入职日期等，用于人力资源查询", lines=2)
        with gr.Row():
            output_dir = gr.Textbox(label="Output Dir", value="output/base_staff")
            review_queue = gr.Textbox(label="Review Queue", value="output/base_staff/review_queue.jsonl")
            schema_path = gr.Textbox(label="Schema Path (optional)", value="")

        run_btn = gr.Button("Run Runtime")

        status_out = gr.Textbox(label="Status")
        sql_out = gr.TextArea(label="SQL", lines=4)
        record_out = gr.TextArea(label="Record JSON", lines=12)
        note_out = gr.Textbox(label="Note")

        run_btn.click(
            run_runtime_query,
            inputs=[query_input, valid_path, table_name, table_desc, output_dir, review_queue, schema_path],
            outputs=[status_out, sql_out, record_out, note_out],
        )

    demo.launch(server_port=7861, share=False)


if __name__ == "__main__":
    main()
