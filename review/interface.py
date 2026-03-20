"""
审核Web界面（Gradio实现）

支持实时阅览invalid集合，审核运行时纠错任务，以及事后扩展任务
"""

import json
import os
import re
import sys
import threading
from datetime import datetime
from typing import Optional, Callable

# 添加父目录到sys.path，使得无论从哪个目录运行都能导入
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import execute_sql, db_manager
from core.capability_manager import CapabilityInstructManager
from core.config import db_config
from core.utils import save_jsonl_dedup_sql, fill_sql_with_values, load_jsonl, overwrite_jsonl
from core.versioning import VersionManager
from validation.intent_verify import IntentVerifier
from runtime.registry import APIRegistry
from runtime.router import RuntimeRouter
from review.submitter import ReviewSubmitter
from runtime.online_runtime import generate_queries_from_desc, _fallback_generate_api
from core.llm import call_llm_json
from schema.loader import SchemaLoader
from schema.models import DatabaseSchema, TableSchema, FieldInfo
from schema.auto_fix import auto_fix_all_fields, auto_fix_single_field_in_schema, auto_prune_useless_fields
from schema.db_schema_builder import build_schema_from_db
from runtime.runtime_api_bridge import start_runtime_api_bridge
from core.schema_feedback import (
    analyze_sql_modification,
    find_cascade_affected_apis,
    generate_cascade_updates,
    SchemaUpdateSuggestion,
    ModificationReason,
)
from review.i18n import t, t_list

try:
    import gradio as gr
except ImportError:
    gr = None
    print("[Warning] gradio not installed, review interface disabled")

try:
    from review.runtime_query_ui import (
        run_runtime_query,
        run_manual_api_sql,
        import_final_to_valid,
        fill_form_from_record,
        _build_api_schema_from_inputs,
        invalidate_registry_cache,
        set_version_manager as _set_rtui_version_manager,
    )
except Exception:
    try:
        from runtime_query_ui import (
            run_runtime_query,
            run_manual_api_sql,
            import_final_to_valid,
            fill_form_from_record,
            _build_api_schema_from_inputs,
            invalidate_registry_cache,
            set_version_manager as _set_rtui_version_manager,
        )
    except Exception:
        run_runtime_query = None
        run_manual_api_sql = None
        import_final_to_valid = None
        fill_form_from_record = None
        _build_api_schema_from_inputs = None
        invalidate_registry_cache = None
        _set_rtui_version_manager = None


class ReviewInterface:
    """审核Web界面"""
    
    def __init__(
        self,
        invalid_path: str = "./output/base_staff/invalid.jsonl",
        recorrect_path: str = "./output/base_staff/recorrect.jsonl",
        review_queue_path: str = "./output/base_staff/review_queue.jsonl",
        valid_path: str = "./output/base_staff/valid.jsonl",
        on_approve: Optional[Callable] = None,
        auth_users: Optional[list[str]] = None,
    ):
        self.invalid_path = invalid_path
        self.valid_path = valid_path
        self.recorrect_path = recorrect_path
        self.review_queue_path = review_queue_path
        self._auth_users = auth_users or []
        self._normalize_storage_paths()
        self.on_approve = on_approve
        
        # Version management (binlog-style)
        versions_dir = os.path.join(os.path.dirname(self.valid_path) or ".", ".versions")
        self._version_mgr = VersionManager(versions_dir)
        # Inject VersionManager into runtime_query_ui for binlog support
        if _set_rtui_version_manager is not None:
            _set_rtui_version_manager(self._version_mgr)
        # Take initial snapshots
        self._version_mgr.ensure_snapshot("valid", self.valid_path)
        self._version_mgr.ensure_snapshot("invalid", self.invalid_path)
        
        # Schema path (auto-detected)
        self._schema_path = self._detect_schema_path()
        
        # 缓存数据
        self._invalid_records: list[dict] = []
        self._review_tasks: list[dict] = []
        self._current_invalid_idx: int = 0
        self._current_task_idx: int = 0

        # Concurrency guards — prevent double-click side effects
        self._lock_prebuild = threading.Lock()
        self._lock_autofix = threading.Lock()
        self._lock_invalid_action = threading.Lock()
        self._lock_task_action = threading.Lock()
        self._lock_valid_write = threading.Lock()

        # Schema feedback: pending column desc update suggestions
        # List of SchemaUpdateSuggestion objects waiting for user confirmation
        self._pending_schema_suggestions: list = []
        # Cascade API updates waiting for user confirmation
        # List of {index, api_name, query, new_api_desc, reason}
        self._pending_cascade_updates: list = []

    def _detect_schema_path(self) -> str:
        """Auto-detect the initial schema JSON file path."""
        output_dir = os.path.dirname(self.valid_path) or "."
        root_output = os.path.dirname(output_dir) or "."
        # Try common patterns
        for candidate in [
            os.path.join(root_output, "schema_from_db__smart_data__all_tables.json"),
            os.path.join(root_output, "schema_from_db.json"),
        ]:
            if os.path.exists(candidate):
                return candidate
        # Try glob
        import glob
        matches = glob.glob(os.path.join(root_output, "schema_from_db*.json"))
        if matches:
            return matches[0]
        return os.path.join(root_output, "schema_from_db.json")

    def _analyze_and_suggest_schema_updates(self, query: str, old_sql: str, new_sql: str, old_query: str = "") -> dict:
        """
        Analyze SQL/query modification, generate schema update suggestions.
        Called after any API modification (Dataset edit, Validation approve, Task approve).
        Compares both SQL diff and query diff to identify column ambiguity.
        
        Returns:
            {
                "reason_type": str,
                "reason_detail": str,
                "suggestion_text": str,   # Formatted markdown for UI display
                "has_suggestions": bool,
            }
        """
        schema_path = self._schema_path
        if not os.path.exists(schema_path):
            return {"reason_type": "none", "reason_detail": "Schema not found", "suggestion_text": "", "has_suggestions": False}

        with open(schema_path, "r", encoding="utf8") as f:
            schema_json = json.load(f)

        table_name = self._infer_table_name()
        result = analyze_sql_modification(query, old_sql, new_sql, schema_json, table_name, old_query=old_query)

        suggestions = result.get("suggestions", [])
        reason_type = result.get("reason_type", "none")
        reason_detail = result.get("reason_detail", "")

        if not suggestions:
            self._pending_schema_suggestions = []
            self._pending_cascade_updates = []
            type_labels = {
                "column_ambiguity": t("feedback_reason_column_ambiguity"),
                "sql_logic_error": t("feedback_reason_sql_logic"),
                "value_mismatch": t("feedback_reason_value_mismatch"),
                "none": t("feedback_reason_none"),
            }
            label = type_labels.get(reason_type, reason_type)
            return {
                "reason_type": reason_type,
                "reason_detail": reason_detail,
                "suggestion_text": f"{t('feedback_modify_reason')} {label} — {reason_detail}\n\n{t('feedback_no_suggestion')}",
                "has_suggestions": False,
            }

        # Store pending suggestions
        self._pending_schema_suggestions = suggestions

        # Find cascade affected APIs
        changed_fields = [s.field_name for s in suggestions]
        affected_apis = find_cascade_affected_apis(self.valid_path, changed_fields, table_name)
        field_updates = {s.field_name: s.new_desc for s in suggestions}
        cascade = generate_cascade_updates(affected_apis, field_updates, table_name)
        self._pending_cascade_updates = cascade

        # Format display text
        lines = [
            f"{t('feedback_modify_reason')} {t('feedback_reason_column_ambiguity')} — {reason_detail}",
            "",
            t("feedback_suggested_fields"),
            t("feedback_field_header"),
            "|---|---|---|---|",
        ]
        for s in suggestions:
            lines.append(f"| `{s.field_name}` | {s.old_desc[:40]} | **{s.new_desc[:50]}** | {s.confidence:.0%} |")

        if cascade:
            lines.extend([
                "",
                t("feedback_cascade_title"),
                t("feedback_cascade_header"),
                "|---|---|---|---|---|---|",
            ])
            for c in cascade:
                dims = '、'.join(c.get('update_fields', ['description']))
                new_desc_preview = (c.get('new_api_desc', '') or '-')[:40]
                new_sql_preview = (c.get('new_bound_sql', '') or '-')[:40]
                lines.append(f"| `{c['api_name']}` | {c['query'][:30]} | {dims} | {new_desc_preview} | {new_sql_preview} | {c.get('reason', '')} |")

        lines.extend([
            "",
            t("feedback_apply_hint"),
        ])

        return {
            "reason_type": reason_type,
            "reason_detail": reason_detail,
            "suggestion_text": "\n".join(lines),
            "has_suggestions": True,
        }

    def _apply_schema_suggestions(self):
        """Apply pending schema column description updates + cascade API desc updates."""
        if not self._pending_schema_suggestions:
            return t("feedback_no_pending")

        schema_path = self._schema_path
        if not os.path.exists(schema_path):
            return t("msg_schema_not_found")

        with open(schema_path, "r", encoding="utf8") as f:
            schema_json = json.load(f)

        table_name = self._infer_table_name()
        tables = schema_json.get("tables", {})
        table = tables.get(table_name, {})
        fields = table.get("fields", {})

        old_schema_str = json.dumps(schema_json, ensure_ascii=False)
        applied_fields = []

        for s in self._pending_schema_suggestions:
            if s.field_name in fields:
                fields[s.field_name]["comment"] = s.new_desc
                applied_fields.append(s.field_name)

        if not applied_fields:
            return t("feedback_no_applicable")

        # Save schema with binlog
        self._version_mgr.log_operation(
            "schema", "update", schema_json, json.loads(old_schema_str),
            {"source": "schema_feedback", "updated_fields": applied_fields}
        )
        with open(schema_path, "w", encoding="utf8") as f:
            json.dump(schema_json, f, ensure_ascii=False, indent=2)

        # Apply cascade API updates (description, SQL, query)
        cascade_applied = 0
        cascade_details = []  # collect what was updated for each API
        if self._pending_cascade_updates:
            from core.utils import load_jsonl, overwrite_jsonl
            records = load_jsonl(self.valid_path) if os.path.exists(self.valid_path) else []
            for cu in self._pending_cascade_updates:
                idx = cu.get("index", -1)
                if idx < 0 or idx >= len(records):
                    continue
                # Double-check: skip user_edited records (safety net)
                if records[idx].get("user_edited"):
                    continue

                update_fields = cu.get("update_fields", [])
                if not update_fields:
                    continue

                old_rec = dict(records[idx])
                api = records[idx].get("api_schema") or {}
                updated_parts = []

                if "description" in update_fields and cu.get("new_api_desc"):
                    api["description"] = cu["new_api_desc"]
                    updated_parts.append("描述")
                if "bound_sql" in update_fields and cu.get("new_bound_sql"):
                    api["bound_sql"] = cu["new_bound_sql"]
                    updated_parts.append("SQL")
                if "query" in update_fields and cu.get("new_query"):
                    records[idx]["query"] = cu["new_query"]
                    updated_parts.append("查询")

                if updated_parts:
                    records[idx]["api_schema"] = api
                    self._version_mgr.log_operation(
                        "valid", "update", records[idx], old_rec,
                        {"source": "cascade_update", "field_updates": applied_fields,
                         "updated_parts": updated_parts}
                    )
                    cascade_applied += 1
                    cascade_details.append(f"`{cu.get('api_name', '?')}` ({'、'.join(updated_parts)})")

            if cascade_applied > 0:
                overwrite_jsonl(self.valid_path, records)
                if invalidate_registry_cache:
                    invalidate_registry_cache()

        # Clear pending
        self._pending_schema_suggestions = []
        self._pending_cascade_updates = []

        result = t("feedback_applied", count=len(applied_fields), fields=', '.join(applied_fields))
        if cascade_applied > 0:
            detail_text = '; '.join(cascade_details)
            result += f"\n{t('feedback_cascade_applied', count=cascade_applied, details=detail_text)}"
        return result

    def _dismiss_schema_suggestions(self):
        """Dismiss pending schema suggestions without applying."""
        count = len(self._pending_schema_suggestions)
        self._pending_schema_suggestions = []
        self._pending_cascade_updates = []
        if count > 0:
            return t("feedback_dismissed", count=count)
        return t("feedback_dismiss_empty")

    def _boundary_path(self) -> str:
        return os.path.join(os.path.dirname(self.valid_path) or ".", "boundary.jsonl")

    def _save_boundary(self, query: str, api_name: str, boundary_text: str, reviewer: str):
        reviewer = self._ensure_reviewer(reviewer)
        if not reviewer:
            return t("msg_reviewer_required")
        text = (boundary_text or "").strip()
        if not text:
            return t("msg_boundary_empty")

        parsed = None
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = {"raw_text": text}

        rec = {
            "query": (query or "").strip(),
            "api_name": (api_name or "").strip(),
            "boundary": parsed,
            "reviewer": reviewer,
            "created_at": datetime.now().isoformat(),
            "source": "manual_boundary_review",
        }
        path = self._boundary_path()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "a", encoding="utf8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        # Version log for boundary write
        self._version_mgr.log_operation("boundary", "insert", rec, meta={"source": "manual_boundary_review"})

        # 保存能力边界视为该invalid已处理，避免下次继续进入审核队列
        current = self._get_current_invalid()
        if current:
            self._resolve_invalid_record(current, reviewer, "skipped")
            self._refresh_invalid_records()

        return t("msg_boundary_saved", path=path)

    def _auto_review_sql_only(self, query: str, sql: str, table_name: str):
        q = (query or "").strip()
        manual_sql = (sql or "").strip()
        table = (table_name or "").strip() or "base_staff"
        if not q:
            return [
                gr.update(), gr.update(), gr.update(), gr.update(),
                gr.update(value=t("msg_query_empty") + ", " + t("msg_auto_review_fail"))
            ]
        if not manual_sql:
            return [
                gr.update(), gr.update(), gr.update(), gr.update(),
                gr.update(value=t("msg_sql_empty") + ", " + t("msg_auto_review_fail"))
            ]

        # Fill named placeholders (:param) with dummy values for initial syntax check
        _slots = re.findall(r':(\w+)', manual_sql)
        _test_sql = fill_sql_with_values(manual_sql, {s: 'test' for s in _slots}) if _slots else manual_sql
        exec_result = execute_sql(None, _test_sql)
        if exec_result.get("status") != "success":
            return [
                gr.update(), gr.update(), gr.update(), gr.update(),
                gr.update(value=f"❌ 手工SQL执行失败: {exec_result.get('error')}")
            ]

        if _build_api_schema_from_inputs is None:
            return [
                gr.update(), gr.update(), gr.update(), gr.update(value=manual_sql),
                gr.update(value=t("msg_build_api_unavailable"))
            ]

        input_schema = self._infer_input_schema_from_sql(manual_sql)
        api_schema = _build_api_schema_from_inputs(
            table_name=table,
            query=q,
            api_name="",
            api_desc="",
            input_schema_text=json.dumps(input_schema, ensure_ascii=False),
            sql=manual_sql,
        )

        router = self._build_router()
        params = router.slot_filler.fill(q, api_schema) or {}
        required = api_schema.inputSchema.get("required", []) if isinstance(api_schema.inputSchema, dict) else []
        missing = [k for k in required if k not in params or params.get(k) in [None, ""]]
        if missing:
            note = f"⚠️ 自动复审：缺少槽位参数 {missing}"
            return [
                gr.update(value=getattr(api_schema, "name", "")),
                gr.update(value=getattr(api_schema, "description", "")),
                gr.update(value=json.dumps(getattr(api_schema, "inputSchema", {}), ensure_ascii=False, indent=2)),
                gr.update(value=manual_sql),
                gr.update(value=note),
            ]

        bound_sql = getattr(api_schema, "bound_sql", manual_sql)
        roundtrip_sql = fill_sql_with_values(bound_sql, params)
        roundtrip_res = execute_sql(None, roundtrip_sql)
        passed = False
        if roundtrip_res.get("status") == "success":
            try:
                passed = IntentVerifier().verify(q, roundtrip_sql, roundtrip_res)
            except Exception:
                passed = False

        note = t("msg_auto_review_pass") if passed else t("msg_auto_review_fail")
        return [
            gr.update(value=getattr(api_schema, "name", "")),
            gr.update(value=getattr(api_schema, "description", "")),
            gr.update(value=json.dumps(getattr(api_schema, "inputSchema", {}), ensure_ascii=False, indent=2)),
            gr.update(value=manual_sql),
            gr.update(value=f"{note} | roundtrip_sql: {roundtrip_sql}"),
        ]

    def _extract_task_sql(self, task: dict) -> str:
        task = self._normalize_review_task(task or {})
        correct_api = task.get("correct_api") or {}
        wrong_api = task.get("wrong_api") or {}
        return (
            correct_api.get("bound_sql")
            or wrong_api.get("bound_sql")
            or task.get("sql")
            or ""
        )

    def _extract_task_table_name(self, task: dict) -> str:
        task = self._normalize_review_task(task or {})
        candidate_tables = task.get("candidate_tables") or []
        if candidate_tables:
            return candidate_tables[0]
        correct_api = task.get("correct_api") or {}
        wrong_api = task.get("wrong_api") or {}
        return correct_api.get("table") or wrong_api.get("table") or "base_staff"

    def _auto_review_task_sql_only(self, query: str, sql: str):
        task = self._get_current_task() or {}
        table_name = self._extract_task_table_name(task)
        return self._auto_review_sql_only(query, sql, table_name)

    def _approve_task_sql_only(self, query: str, sql: str, reviewer: str, comment: str = ""):
        original_task = self._get_current_task()
        if not original_task:
            return self._render_task_interface()

        task = self._normalize_review_task(original_task)
        reviewer = self._ensure_reviewer(reviewer)
        if not reviewer:
            return self._render_task_interface_with_status(t("msg_reviewer_required"))

        schema_obj = self._infer_input_schema_from_sql(sql)
        if _build_api_schema_from_inputs is not None:
            built = _build_api_schema_from_inputs(
                table_name=self._extract_task_table_name(task),
                query=query,
                api_name="",
                api_desc="",
                input_schema_text=json.dumps(schema_obj, ensure_ascii=False),
                sql=sql,
            )
            api_schema = {
                "name": getattr(built, "name", ""),
                "description": getattr(built, "description", "") or self._infer_api_desc_from_query(query),
                "inputSchema": getattr(built, "inputSchema", schema_obj),
                "bound_sql": getattr(built, "bound_sql", sql),
                "slot_mapping": list((getattr(built, "slot_mapping", {}) or {}).keys()),
                "source": "manual_review",
            }
        else:
            api_schema = {
                "name": "runtime_task_api",
                "description": self._infer_api_desc_from_query(query),
                "inputSchema": schema_obj,
                "bound_sql": sql,
                "slot_mapping": list(re.findall(r':(\w+)', sql)),
                "source": "manual_review",
            }

        self._update_task_status(original_task['task_id'], 'approved', comment, reviewer)
        self._save_to_valid(
            api_schema,
            query,
            from_runtime_correction=True,
            distinction_instruction=task.get('distinction_instruction'),
            reviewer=reviewer,
            review_method='manual_review',
        )
        self._current_task_idx += 1
        return self._render_task_interface()

    def _is_abstract_query(self, query: str) -> bool:
        q = (query or "").strip()
        if not q:
            return False
        patterns = [r"指定", r"某个?", r"某些", r"范围内", r"描述", r"条件"]
        return any(re.search(p, q) for p in patterns)

    def _collect_sample_value_hints(self, table_name: str, max_cols: int = 6) -> str:
        try:
            desc_res = execute_sql(None, f"DESC {table_name}")
            rows = desc_res.get("all_rows") or []
            if not rows:
                return t("msg_no_sample_values")

            preferred = []
            fallback = []
            keywords = ["name", "city", "dept", "company", "phone", "status", "address", "formal", "display", "title"]
            for row in rows:
                col = str(row[0])
                typ = str(row[1]).lower()
                if any(k in col.lower() for k in keywords):
                    preferred.append((col, typ))
                elif any(tp in typ for tp in ["char", "text", "varchar"]):
                    fallback.append((col, typ))

            candidates = (preferred + fallback)[:max_cols]
            hints = []
            for col, _ in candidates:
                sample_sql = (
                    f"SELECT `{col}` FROM `{table_name}` "
                    f"WHERE `{col}` IS NOT NULL AND CAST(`{col}` AS CHAR) <> '' "
                    f"GROUP BY `{col}` LIMIT 3"
                )
                sample_res = execute_sql(None, sample_sql)
                sample_rows = sample_res.get("all_rows") or []
                values = []
                for item in sample_rows:
                    if isinstance(item, (list, tuple)) and item:
                        values.append(str(item[0]))
                    elif isinstance(item, dict) and item:
                        values.append(str(next(iter(item.values()))))
                if values:
                    hints.append(f"- {col}: {values}")
            return "\n".join(hints) if hints else t("msg_no_sample_values")
        except Exception:
            return t("msg_no_sample_values")

    def _concretize_query(self, query: str, table_name: str, sql: str = ""):
        q = (query or "").strip()
        table = (table_name or "").strip() or "base_staff"
        if not q:
            return gr.update(value=q), gr.update(value=t("msg_query_empty"))
        if not self._is_abstract_query(q):
            return gr.update(value=q), gr.update(value=t("msg_concretize_skip"))

        sample_hints = self._collect_sample_value_hints(table)
        prompt = f"""
你是数据标注助手。请把抽象query改写成一个更具体、可执行的用户query。
要求：
1. 保持原始意图不变；
2. 尽量使用给定表中的样例值；
3. 输出一个具体query，不要解释；
4. 不要生成SQL。

表名: {table}
当前query: {q}
当前SQL(如有): {sql}
样例值:
{sample_hints}

仅输出JSON:
{{"query": "..."}}
""".strip()
        result = call_llm_json(prompt)
        concrete = q
        if isinstance(result, dict) and isinstance(result.get("query"), str) and result.get("query").strip():
            concrete = result.get("query").strip()
        note = t("msg_concretize_done") if concrete != q else t("msg_concretize_fail")
        return gr.update(value=concrete), gr.update(value=note)

    def _auto_generate_sql_fields(self, query: str, table_name: str, table_desc: str):
        q = (query or "").strip()
        table = (table_name or "").strip() or "base_staff"
        desc = (table_desc or "").strip()
        if not q:
            return [
                gr.update(value=""),
                gr.update(value=""),
                gr.update(value=json.dumps({"type": "object", "properties": {}, "required": []}, ensure_ascii=False, indent=2)),
                gr.update(value=""),
                gr.update(value=t("msg_query_empty")),
            ]
        api_schema, sql, ok = self._auto_generate_api_sql(q, table, desc, attempts=3)
        if api_schema is None:
            return [
                gr.update(value=""),
                gr.update(value=""),
                gr.update(value=json.dumps({"type": "object", "properties": {}, "required": []}, ensure_ascii=False, indent=2)),
                gr.update(value=""),
                gr.update(value=t("msg_auto_gen_fail")),
            ]
        status = t("msg_auto_gen_success") if ok else t("msg_auto_gen_warn")
        return [
            gr.update(value=getattr(api_schema, "name", "")),
            gr.update(value=getattr(api_schema, "description", "")),
            gr.update(value=json.dumps(getattr(api_schema, "inputSchema", {}), ensure_ascii=False, indent=2)),
            gr.update(value=sql),
            gr.update(value=status),
        ]

    def _auto_generate_task_sql_fields(self, query: str, table_desc: str):
        task = self._get_current_task() or {}
        return self._auto_generate_sql_fields(query, self._extract_task_table_name(task), table_desc)

    def _infer_table_output_dir(self) -> Optional[str]:
        candidates = []
        for p in [self.invalid_path, self.valid_path]:
            if not p:
                continue
            base = os.path.basename(p)
            if base in {"invalid.jsonl", "valid.jsonl", "runtime_invalid.jsonl", "runtime_valid.jsonl"}:
                candidates.append(os.path.dirname(p))

        for d in candidates:
            if d and os.path.basename(d) and os.path.basename(d) != "output":
                return d
        return None

    def _infer_table_name(self) -> str:
        """优先从 valid/invalid 路径推断表名，失败时回退到记录字段"""
        table_dir = self._infer_table_output_dir()
        if table_dir:
            table_name = os.path.basename(table_dir)
            if table_name and table_name != "output":
                return table_name

        record = self._get_current_invalid() or {}
        if record.get("table"):
            return record.get("table")

        return "base_staff"

    def _normalize_storage_paths(self):
        """将审核相关文件路径统一到 output/<table>/ 目录"""
        table_dir = self._infer_table_output_dir()
        if not table_dir:
            return

        # 根目录旧默认：./review_queue.jsonl -> output/<table>/review_queue.jsonl
        if self.review_queue_path in {"./review_queue.jsonl", "review_queue.jsonl"}:
            self.review_queue_path = os.path.join(table_dir, "review_queue.jsonl")

        # 旧默认：./output/dataset_recorrect.jsonl -> output/<table>/recorrect.jsonl
        if self.recorrect_path in {"./output/dataset_recorrect.jsonl", "output/dataset_recorrect.jsonl"}:
            self.recorrect_path = os.path.join(table_dir, "recorrect.jsonl")
    
    def _load_invalid(self) -> list[dict]:
        """加载无效记录（合并 invalid.jsonl + runtime_invalid.jsonl）"""
        if not os.path.exists(self.invalid_path):
            output_dir = os.path.dirname(self.invalid_path) or "./output"
            if os.path.basename(self.invalid_path).startswith("dataset_"):
                output_dir = "./output"

            candidates = []
            if os.path.exists(output_dir):
                for name in os.listdir(output_dir):
                    table_invalid = os.path.join(output_dir, name, "invalid.jsonl")
                    if os.path.isfile(table_invalid):
                        candidates.append(table_invalid)

            if len(candidates) == 1:
                self.invalid_path = candidates[0]
                self.valid_path = os.path.join(os.path.dirname(candidates[0]), "valid.jsonl")
            else:
                return []

        # 同目录下的 runtime_invalid.jsonl 一并读入
        invalid_dir = os.path.dirname(self.invalid_path) or "."
        paths_to_load = [self.invalid_path]
        runtime_invalid_path = os.path.join(invalid_dir, "runtime_invalid.jsonl")
        if (
            os.path.exists(runtime_invalid_path)
            and os.path.abspath(runtime_invalid_path) != os.path.abspath(self.invalid_path)
        ):
            paths_to_load.append(runtime_invalid_path)

        records = []
        for path in paths_to_load:
            if not os.path.exists(path):
                continue
            with open(path, "r", encoding="utf8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        review_status = record.get("review_status")
                        if review_status in {"approved", "rejected", "skipped"}:
                            continue
                        records.append(record)
                    except Exception:
                        continue
        return records
    
    def _load_review_tasks(self) -> list[dict]:
        """加载审核任务"""
        if not os.path.exists(self.review_queue_path):
            return []
        
        records = []
        with open(self.review_queue_path, "r", encoding="utf8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    # 只加载待审核的
                    if record.get("status") == "pending":
                        records.append(record)
                except:
                    continue
        return records

    def _ensure_reviewer(self, reviewer: str) -> Optional[str]:
        if reviewer and reviewer.strip():
            return reviewer.strip()
        return None

    def _render_invalid_interface_with_status(self, status_text: str):
        rendered = self._render_invalid_interface()
        if isinstance(rendered, list) and len(rendered) >= 11:
            rendered[10] = gr.update(value=status_text)
        return rendered

    def _keep_invalid_form_with_status(self, status_text: str):
        return [
            gr.update(),  # progress_invalid
            gr.update(),  # content_invalid
            gr.update(),  # query_input
            gr.update(),  # api_name_input
            gr.update(),  # api_desc_input
            gr.update(),  # input_schema_input
            gr.update(),  # sql_input
            gr.update(),  # approve_btn
            gr.update(),  # skip_btn
            gr.update(),  # reject_btn
            gr.update(value=status_text),  # auto_status_md
        ]

    def _render_task_interface_with_status(self, status_text: str):
        rendered = self._render_task_interface()
        if isinstance(rendered, list) and len(rendered) >= 15:
            rendered[14] = gr.update(value=status_text)
        return rendered

    def _build_router(self) -> RuntimeRouter:
        registry = APIRegistry(self.valid_path)
        submitter = ReviewSubmitter(self.review_queue_path)
        return RuntimeRouter(registry, submitter, enable_verify=True)
    
    def _generate_instruct_for_record(self, query: str, table_name: str, table_desc: str) -> str:
        """
        为当前invalid记录生成能力约束指令
        
        Args:
            query: 用户查询
            table_name: 表名（来自UI）
            table_desc: 表描述
        
        Returns:
            生成的instruct说明（JSON格式字符串）
        """
        # 从当前无效记录中获取真实表名（优先使用记录中的表名）
        record = self._get_current_invalid()
        if record and record.get('table'):
            table_name = record.get('table')
        
        instruct_mgr = CapabilityInstructManager(os.path.dirname(self.valid_path) or "./output")
        supported_fields = []
        if record and record.get("schema"):
            table_schema = record.get("schema", {}).get("tables", {}).get(table_name, {})
            fields = table_schema.get("fields", {}) if isinstance(table_schema, dict) else {}
            supported_fields = list(fields.keys())
        
        # 从valid.jsonl提取已有的API名字
        existing_apis = []
        if os.path.exists(self.valid_path):
            with open(self.valid_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        rec = json.loads(line.strip())
                        api_name = rec.get("api_schema", {}).get("name", "")
                        if api_name:
                            existing_apis.append(api_name)
                    except:
                        pass
        
        # 调用LLM生成instruct
        instruct = instruct_mgr.generate_instruct(
            query=query,
            table_name=table_name,
            table_desc=table_desc,
            available_fields=supported_fields,
            existing_api_names=existing_apis
        )
        
        return json.dumps(instruct, ensure_ascii=False, indent=2)

    def _build_schema_loader_from_db(self, table_name: str) -> Optional[SchemaLoader]:
        """通过 DESC 语句从数据库实时拉取表字段信息，构建 SchemaLoader。"""
        try:
            res = execute_sql(None, f"DESC `{table_name}`")
            rows = res.get("all_rows") or []
            if not rows:
                return None
            fields = []
            for row in rows:
                # DESC 返回 (Field, Type, Null, Key, Default, Extra)
                if isinstance(row, dict):
                    col = str(row.get("Field") or row.get("field") or next(iter(row.values()), ""))
                    typ = str(row.get("Type") or row.get("type") or "varchar")
                    key = str(row.get("Key") or row.get("key") or "")
                    comment = str(row.get("Comment") or row.get("comment") or "")
                else:
                    col = str(row[0]) if len(row) > 0 else ""
                    typ = str(row[1]) if len(row) > 1 else "varchar"
                    key = str(row[3]) if len(row) > 3 else ""
                    comment = ""
                if not col:
                    continue
                fields.append(FieldInfo(
                    name=col,
                    type=typ,
                    is_primary=(key == "PRI"),
                    comment=comment or None,
                ))
            if not fields:
                return None
            table_schema = TableSchema(name=table_name, fields=fields)
            db_schema = DatabaseSchema(database="smart_data", tables={table_name: table_schema})
            loader = SchemaLoader()
            loader._schema = db_schema
            return loader
        except Exception:
            return None

    def _sample_value_for_slot(self, table_name: str, slot: str):
        """按slot名称从表中取一个样例值，作为SQL具体化兜底值。"""
        col = (slot or "").strip()
        if not col:
            return "test"
        try:
            exists_sql = f"DESC `{table_name}` `{col}`"
            exists_res = execute_sql(None, exists_sql)
            if exists_res.get("status") != "success" or not (exists_res.get("all_rows") or []):
                return "test"

            sample_sql = (
                f"SELECT `{col}` FROM `{table_name}` "
                f"WHERE `{col}` IS NOT NULL AND CAST(`{col}` AS CHAR) <> '' LIMIT 1"
            )
            sample_res = execute_sql(None, sample_sql)
            rows = sample_res.get("all_rows") or []
            if not rows:
                return "test"
            row = rows[0]
            if isinstance(row, (list, tuple)) and row:
                return row[0]
            if isinstance(row, dict) and row:
                return next(iter(row.values()))
            return "test"
        except Exception:
            return "test"

    def _materialize_sql_for_display(self, query: str, table_name: str, api_schema, sql_template: str) -> str:
        """将槽位SQL具体化为可直接执行的SQL，用于UI展示。"""
        sql = (sql_template or "").strip()
        if not sql:
            return ""

        slots = re.findall(r':(\w+)', sql)
        if not slots:
            return sql

        params = {}
        try:
            router = self._build_router()
            params = router.slot_filler.fill(query, api_schema) or {}
        except Exception:
            params = {}

        for slot in slots:
            if slot not in params or params.get(slot) in [None, ""]:
                params[slot] = self._sample_value_for_slot(table_name, slot)

        return fill_sql_with_values(sql, params)

    def _auto_generate_api_sql(self, query: str, table_name: str, table_desc: str, attempts: int = 3):
        schema_loader = self._build_schema_loader_from_db(table_name)
        last_result = None
        for _ in range(attempts):
            last_result = _fallback_generate_api(query, table_name, table_desc, schema_loader)
            if not last_result or not last_result.get("api_schema"):
                continue

            api_schema = last_result.get("api_schema")
            # APISchema 是 Pydantic 模型，需要用属性访问
            bound_sql = getattr(api_schema, "bound_sql", "") or last_result.get("sql") or ""
            if not bound_sql:
                continue

            sql = self._materialize_sql_for_display(query, table_name, api_schema, bound_sql)
            if not sql:
                continue

            # Fill any :param placeholders with dummy values before syntax-check execution
            _slots = re.findall(r':(\w+)', sql)
            _test_sql = fill_sql_with_values(sql, {s: 'test' for s in _slots}) if _slots else sql
            exec_result = execute_sql(None, _test_sql)
            try:
                ok = IntentVerifier().verify(query, _test_sql, exec_result)
            except Exception:
                ok = False

            if ok:
                return api_schema, sql, True

        if last_result and last_result.get("api_schema"):
            api_schema = last_result.get("api_schema")
            bound_sql = getattr(api_schema, "bound_sql", "") or last_result.get("sql") or ""
            sql = self._materialize_sql_for_display(query, table_name, api_schema, bound_sql)
            return api_schema, sql, False

        return None, "", False

    def _auto_generate_for_invalid(self, query: str, table_desc: str):
        record = self._get_current_invalid() or {}
        table_name = record.get("table") or "base_staff"

        api_schema, sql, ok = self._auto_generate_api_sql(query, table_name, table_desc, attempts=3)
        
        if api_schema is None:
            status = t("msg_auto_gen_fail")
            return [
                gr.update(value=""),
                gr.update(value=""),
                gr.update(value=""),
                gr.update(value=""),
                gr.update(value=status),
            ]
        
        status = t("msg_auto_gen_success") if ok else t("msg_auto_gen_warn")

        return [
            gr.update(value=getattr(api_schema, "name", "")),
            gr.update(value=getattr(api_schema, "description", "")),
            gr.update(value=json.dumps(getattr(api_schema, "inputSchema", {}), ensure_ascii=False, indent=2)),
            gr.update(value=sql),
            gr.update(value=status),
        ]

    def _auto_generate_for_task(self, query: str, table_desc: str):
        task = self._normalize_review_task(self._get_current_task() or {})
        table_name = task.get("table")
        if not table_name:
            candidate_tables = task.get("candidate_tables") or []
            table_name = candidate_tables[0] if candidate_tables else "base_staff"

        api_schema, sql, ok = self._auto_generate_api_sql(query, table_name, table_desc, attempts=3)
        
        if api_schema is None:
            status = t("msg_auto_gen_fail")
            return [
                gr.update(value=""),
                gr.update(value=status),
            ]
        
        # 将 Pydantic 模型转为字典
        if hasattr(api_schema, "model_dump"):
            api_dict = api_schema.model_dump()
        elif hasattr(api_schema, "dict"):
            api_dict = api_schema.dict()
        else:
            api_dict = {}
        
        if sql:
            api_dict["bound_sql"] = sql

        status = t("msg_auto_gen_success") if ok else t("msg_auto_gen_warn")

        return [
            gr.update(value=json.dumps(api_dict, ensure_ascii=False, indent=2)),
            gr.update(value=status),
        ]

    def _expand_queries(self, base_query: str, reviewer: str, table_desc: Optional[str] = None, n: int = 5):
        desc = table_desc or ""
        if not desc:
            return 0
        router = self._build_router()
        intent_verifier = IntentVerifier()
        derived = generate_queries_from_desc(desc, n)
        saved = 0
        for dq in derived:
            if dq == base_query:
                continue
            res = router.route(dq)
            if res.status != "success" or not res.invoked_sql:
                continue
            try:
                ok = intent_verifier.verify(dq, res.invoked_sql, res.exec_result or {})
            except Exception:
                ok = False
            if not ok:
                continue
            self._save_to_valid(
                {
                    "name": res.api_name,
                    "description": getattr(res, "description", ""),
                    "inputSchema": {},
                    "bound_sql": res.invoked_sql,
                },
                dq,
                reviewer=reviewer,
                source="manual_expand",
                runtime_source="review_expand",
            )
            saved += 1
        return saved
    
    def _resolve_invalid_record(self, target_record: dict, reviewer: str, decision: str):
        """将 invalid 记录标记为已审核，同时覆盖 invalid.jsonl 与 runtime_invalid.jsonl。"""
        invalid_dir = os.path.dirname(self.invalid_path) or "."
        runtime_invalid_path = os.path.join(invalid_dir, "runtime_invalid.jsonl")

        for path in [self.invalid_path, runtime_invalid_path]:
            if not os.path.exists(path):
                continue

            with open(path, "r", encoding="utf8") as f:
                lines = [l for l in f if l.strip()]

            resolved = False
            rewritten = []
            for raw_line in lines:
                try:
                    rec = json.loads(raw_line)
                except Exception:
                    rewritten.append(raw_line if raw_line.endswith("\n") else raw_line + "\n")
                    continue

                if not resolved and self._same_invalid_record(rec, target_record):
                    old_rec = dict(rec)
                    rec["review_status"] = decision
                    rec["reviewed_at"] = datetime.now().isoformat()
                    rec["reviewer"] = reviewer
                    resolved = True
                    rewritten.append(json.dumps(rec, ensure_ascii=False) + "\n")
                    # Version log
                    self._version_mgr.log_operation("invalid", "update", rec, old_rec, {"decision": decision})
                else:
                    rewritten.append(raw_line if raw_line.endswith("\n") else raw_line + "\n")

            with open(path, "w", encoding="utf8") as f:
                f.writelines(rewritten)

            if resolved:
                break

    def _same_invalid_record(self, record_a: Optional[dict], record_b: Optional[dict]) -> bool:
        if not isinstance(record_a, dict) or not isinstance(record_b, dict):
            return False
        if record_a == record_b:
            return True

        key_fields = ["query", "sql", "reason", "error", "table", "status", "query_type"]
        return all((record_a.get(key) or "") == (record_b.get(key) or "") for key in key_fields)

    def _refresh_invalid_records(self):
        self._invalid_records = self._load_invalid()
        if self._current_invalid_idx >= len(self._invalid_records):
            self._current_invalid_idx = max(0, len(self._invalid_records) - 1)

    def _save_to_recorrect(
        self,
        record: dict,
        modified: bool = False,
        reviewer: Optional[str] = None,
        corrected_query: Optional[str] = None,
        corrected_api_schema: Optional[dict] = None,
        corrected_sql: Optional[str] = None,
    ):
        """保存到recorrect集合"""
        os.makedirs(os.path.dirname(self.recorrect_path) or ".", exist_ok=True)
        
        recorrect_record = {
            **record,
            "reviewed_at": __import__('datetime').datetime.now().isoformat(),
            "modified": modified,
            "source": "invalid_recovery",
            "reviewer": reviewer,
        }

        if corrected_query is not None:
            recorrect_record["query"] = corrected_query
        if corrected_api_schema is not None:
            recorrect_record["api_schema"] = corrected_api_schema
        if corrected_sql is not None:
            recorrect_record["sql"] = corrected_sql
        recorrect_record["source_stage"] = "manual_review"
        recorrect_record["source_method"] = "invalid_recovery"
        recorrect_record["source_channel"] = "review_interface"
        
        with open(self.recorrect_path, "a", encoding="utf8") as f:
            f.write(json.dumps(recorrect_record, ensure_ascii=False) + "\n")

        # Version log for recorrect write
        self._version_mgr.log_operation("recorrect", "insert", recorrect_record, meta={"source": "invalid_recovery", "modified": modified})

    def _upsert_valid_record_by_sql(self, valid_record: dict):
        sql = ((valid_record.get("api_schema") or {}).get("bound_sql") or valid_record.get("sql") or "").strip()
        if not sql:
            save_jsonl_dedup_sql(self.valid_path, valid_record)
            return

        normalized_sql = re.sub(r"\s+", "", sql).strip().lower()
        records = []
        matched = False

        if os.path.exists(self.valid_path):
            with open(self.valid_path, "r", encoding="utf8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue

                    rec_sql = ((rec.get("api_schema") or {}).get("bound_sql") or rec.get("sql") or "").strip()
                    rec_norm_sql = re.sub(r"\s+", "", rec_sql).strip().lower()
                    if not matched and rec_norm_sql and rec_norm_sql == normalized_sql:
                        merged = dict(rec)
                        merged_api = dict(rec.get("api_schema") or {})
                        merged_api.update(valid_record.get("api_schema") or {})
                        merged.update(valid_record)
                        merged["api_schema"] = merged_api
                        records.append(merged)
                        matched = True
                    else:
                        records.append(rec)

        if not matched:
            records.append(valid_record)

        with open(self.valid_path, "w", encoding="utf8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    
    def _save_to_valid(self, api_schema: dict, query: str, **extra):
        """保存到valid集合"""
        os.makedirs(os.path.dirname(self.valid_path) or ".", exist_ok=True)

        source_stage = extra.pop("source_stage", "manual_review")
        source_method = extra.pop("source_method", "manual_review")
        source_channel = extra.pop("source_channel", "review_interface")
        
        valid_record = {
            "query": query,
            "api_schema": api_schema,
            "reviewed_at": __import__('datetime').datetime.now().isoformat(),
            "source": "manual_review",
            "source_stage": source_stage,
            "source_method": source_method,
            "source_channel": source_channel,
            **extra
        }

        self._upsert_valid_record_by_sql(valid_record)

        # Version log
        self._version_mgr.log_operation("valid", "insert", valid_record, meta={"source": source_method})

        # Invalidate runtime registry cache so next query picks up changes
        if invalidate_registry_cache is not None:
            invalidate_registry_cache()
    
    def _update_task_status(self, task_id: str, status: str, comment: str = "", reviewer: Optional[str] = None):
        """更新审核任务状态"""
        if not os.path.exists(self.review_queue_path):
            return
        
        # 读取所有任务
        all_tasks = []
        with open(self.review_queue_path, "r", encoding="utf8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    all_tasks.append(json.loads(line))
                except:
                    continue
        
        # 更新状态
        for task in all_tasks:
            if task.get("task_id") == task_id:
                old_task = dict(task)
                task["status"] = status
                task["review_comment"] = comment
                task["reviewed_at"] = __import__('datetime').datetime.now().isoformat()
                task["reviewer"] = reviewer
                # Version log for review_queue update
                self._version_mgr.log_operation("review_queue", "update", task, old_task,
                                                 {"source": "task_review", "decision": status})
                break
        
        # 写回
        with open(self.review_queue_path, "w", encoding="utf8") as f:
            for task in all_tasks:
                f.write(json.dumps(task, ensure_ascii=False) + "\n")
    
    def _get_current_invalid(self) -> Optional[dict]:
        """获取当前无效记录"""
        if not self._invalid_records:
            self._invalid_records = self._load_invalid()
        
        if self._current_invalid_idx < len(self._invalid_records):
            return self._invalid_records[self._current_invalid_idx]
        return None
    
    def _generate_table_desc_from_db(self, table_name: str) -> str:
        """从数据库表结构动态生成表描述"""
        try:
            # 查询表结构
            sql = f"DESC {table_name}"
            result = execute_sql(None, sql)
            
            if result.get("status") != "success":
                return t("msg_table_info_fail")
            
            # 提取字段信息
            fields_info = []
            for row in result.get("all_rows", [])[:15]:  # 前15个字段
                field_name = row[0]
                field_type = row[1]
                fields_info.append(f"{field_name} ({field_type})")
            
            fields_str = ", ".join(fields_info)
            
            # 用LLM生成自然语言描述
            prompt = f"""
根据以下数据库表字段信息，生成一个简洁的中文表业务描述（2-3句）。

表名: {table_name}
字段: {fields_str}

要求: 简洁、清晰、突出主要用途

输出JSON: {{"description": "..."}}
"""
            result = call_llm_json(prompt)
            if result and "description" in result:
                return result["description"]
            
            # 降级：用字段列表作为描述
            return f"{table_name} table, fields: {', '.join([f.split('(')[0] for f in fields_info[:5]])} etc."
        except Exception as e:
            print(f"Failed to generate table desc: {e}")
            return f"{table_name}"
    
    def _get_current_task(self) -> Optional[dict]:
        """获取当前审核任务"""
        if not self._review_tasks:
            self._review_tasks = self._load_review_tasks()
        
        if self._current_task_idx < len(self._review_tasks):
            return self._review_tasks[self._current_task_idx]
        return None

    def _task_api_to_dict(self, api_obj) -> dict:
        if isinstance(api_obj, dict):
            return api_obj
        if hasattr(api_obj, "model_dump"):
            return api_obj.model_dump()
        if hasattr(api_obj, "dict"):
            return api_obj.dict()
        return {}

    def _normalize_review_task(self, task: dict) -> dict:
        """兼容不同版本的review_queue任务字段"""
        if not task:
            return {}

        normalized = dict(task)
        normalized["query"] = task.get("query") or task.get("source_query") or ""

        wrong_api = task.get("wrong_api") or task.get("current_api")
        wrong_api = self._task_api_to_dict(wrong_api)

        correct_api = task.get("correct_api")
        if not correct_api:
            candidate_apis = task.get("candidate_apis") or []
            if candidate_apis:
                correct_api = candidate_apis[0]
        correct_api = self._task_api_to_dict(correct_api)

        candidate_tables = task.get("candidate_tables") or []
        if not candidate_tables:
            table_name = wrong_api.get("table") or correct_api.get("table")
            if table_name:
                candidate_tables = [table_name]

        normalized["wrong_api"] = wrong_api
        normalized["correct_api"] = correct_api
        normalized["candidate_tables"] = candidate_tables
        normalized["distinction_instruction"] = (
            task.get("distinction_instruction")
            or task.get("generated_query")
            or ""
        )
        return normalized

    def _infer_input_schema_from_sql(self, sql: str) -> dict:
        slots = list(dict.fromkeys(re.findall(r':(\w+)', sql or "")))
        properties = {}
        required = []

        for slot in slots:
            slot_lower = slot.lower()
            slot_type = "string"
            if slot_lower.endswith("_id") or slot_lower in {"id", "age", "year", "month", "day", "count", "num", "number"}:
                slot_type = "integer"
            elif any(k in slot_lower for k in ["amount", "price", "salary", "score", "rate", "ratio", "percent"]):
                slot_type = "number"

            properties[slot] = {
                "type": slot_type,
                "description": f"筛选参数：{slot}"
            }
            required.append(slot)

        return {
            "type": "object",
            "properties": properties,
            "required": required,
        }

    def _infer_api_desc_from_query(self, query: str) -> str:
        q = (query or "").strip().strip("。！？!?；;，,")
        if not q:
            return "query related information"

        for prefix in ["请帮我查一下", "请帮我查询", "帮我查一下", "帮我查询", "请帮我", "帮我", "请查询", "查询一下", "查一下", "看一下", "请"]:
            if q.startswith(prefix):
                q = q[len(prefix):].strip()
                break

        if any(k in q for k in ["多少", "几", "数量", "总数", "人数"]):
            q2 = q.replace("有多少", "").replace("多少", "").replace("几", "").strip()
            return f"count {q2 or q}"

        if q.endswith(("信息", "情况", "记录", "列表", "明细", "结果")):
            return f"query {q}"
        return f"query {q}"
    
    def _render_invalid_interface(self):
        """渲染无效记录审核界面"""
        record = self._get_current_invalid()

        if record is None:
            return [
            gr.update(value=t("msg_all_invalid_done")),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
            ]

        # 构建显示内容
        current_pos = min(self._current_invalid_idx + 1, len(self._invalid_records)) if self._invalid_records else 0
        progress = t("msg_record_progress", current=current_pos, total=len(self._invalid_records))
        err = record.get("error") or record.get("verification") or record.get("reason") or "未知"
        reason_type = "unknown"
        if isinstance(err, str) and "没有找到匹配" in err:
            reason_type = "api_not_found"
        elif isinstance(err, str) and "缺少必填参数" in err:
            reason_type = "slot_missing"
        elif record.get("verification") and isinstance(record.get("verification"), dict):
            if record.get("verification", {}).get("pass") is False:
                reason_type = "intent_failed"
        elif isinstance(record.get("error"), dict):
            reason_type = "sql_error"
        api_data = record.get("api") or record.get("api_schema") or {}
        sql = record.get("sql") or record.get("invoked_sql") or api_data.get("bound_sql") or "N/A"
        exec_sql = record.get("exec_sql") or record.get("invoked_sql") or api_data.get("bound_sql") or "N/A"

        content = f"""
    {t('review_suggestion_title')}
    {t('review_suggestion_status')}
    {t('review_suggestion_error_type')} `{reason_type}`
    {t('review_suggestion_failure')} {err}

    {t('original_sql_title')}
    ```sql
    {sql}
    ```
    """

        # 尝试恢复api_schema
        api_schema = api_data
        query = record.get('query', '')
        derived_schema = self._infer_input_schema_from_sql(sql if sql != "N/A" else "")
        desc = api_schema.get('description') or self._infer_api_desc_from_query(query)

        return [
            gr.update(value=progress),
            gr.update(value=content, visible=True),
            gr.update(value=query, visible=True),
            gr.update(value=record.get('api_name', api_schema.get('name', '')), visible=True),
            gr.update(value=desc, visible=True),
            gr.update(value=json.dumps(derived_schema, ensure_ascii=False, indent=2), visible=True),
            gr.update(value=sql if sql != "N/A" else "", visible=True),
            gr.update(visible=True),  # approve_btn
            gr.update(visible=True),  # skip_btn
            gr.update(visible=True),  # reject_btn
            gr.update(value=t("msg_auto_review_start")),  # auto_status_md
        ]

def _render_task_interface(self):
    """渲染审核任务界面"""
    task = self._get_current_task()
    
    if task is None:
        return [
        gr.update(value=t("msg_all_tasks_done")),
            gr.update(visible=False),
            gr.update(visible=False),
            gr.update(visible=False),
            gr.update(visible=False),  # task_invoked_sql
            gr.update(visible=False),
            gr.update(visible=False),
            gr.update(visible=False),
            gr.update(visible=False),
            gr.update(visible=False),
            gr.update(visible=False),
            gr.update(visible=False),
            gr.update(visible=False),
            gr.update(visible=False),
            gr.update(visible=False),
        ]
    
    task = self._normalize_review_task(task)
    task_type = task.get('task_type', 'unknown')
    progress = t("msg_task_progress", current=self._current_task_idx + 1, total=len(self._review_tasks), type=task_type)
    
    # 根据任务类型渲染不同内容
    if task_type == 'runtime_correction':
        content = self._render_runtime_correction_task(task)
    elif task_type == 'schema_expansion':
        content = self._render_schema_expansion_task(task)
    else:
        content = f"Unknown task type: {task_type}\n\n{json.dumps(task, indent=2, ensure_ascii=False)}"
    
    task_sql = self._extract_task_sql(task)
    invoked_sql = task.get('invoked_sql', '') or ''
    return [
        gr.update(value=progress),
        gr.update(value=content, visible=True),
        gr.update(value=task.get('query', ''), visible=True),
        gr.update(value=task_sql, visible=True),
        gr.update(value=invoked_sql, visible=True),  # task_invoked_sql
        gr.update(value="", visible=True),
        gr.update(value="", visible=True),
        gr.update(value=json.dumps(self._infer_input_schema_from_sql(task_sql), ensure_ascii=False, indent=2), visible=True),
        gr.update(value=task.get('distinction_instruction', ''), visible=True),
        gr.update(visible=True),  # approve_task_btn
        gr.update(visible=True),  # modify_task_btn
        gr.update(visible=True),  # reject_task_btn
        gr.update(visible=True),  # comment_input
        gr.update(visible=True),  # next_task_btn
        gr.update(value=t("msg_auto_review_start")),  # auto_task_status_md
    ]

def _render_runtime_correction_task(self, task: dict) -> str:
    """渲染运行时纠错任务"""
    task = self._normalize_review_task(task)
    parts = [
        t("task_runtime_correction"),
        "",
        f"{t('task_user_query')} {task.get('query', 'N/A')}",
        "",
        t("task_wrong_api"),
        f"```json\n{json.dumps(task.get('wrong_api', {}), indent=2, ensure_ascii=False)}\n```",
        "",
        t("task_candidate_tables"),
        f"{', '.join(task.get('candidate_tables', []))}",
        "",
        t("task_correct_api"),
        f"```json\n{json.dumps(task.get('correct_api', {}), indent=2, ensure_ascii=False)}\n```",
        "",
        t("task_distinction"),
        f"{task.get('distinction_instruction', 'N/A')}",
        "",
        t("task_auto_analysis"),
        f"{task.get('auto_verify_result', {}).get('reason', 'N/A') if task.get('auto_verify_result') else 'N/A'}",
    ]
    return '\n'.join(parts)

def _render_schema_expansion_task(self, task: dict) -> str:
    """渲染Schema扩展任务"""
    schemas = task.get('generated_schemas', [])
    parts = [
        t("task_schema_expansion"),
        "",
        f"{t('task_original_query')} {task.get('original_query', 'N/A')}",
        "",
        t("task_expanded_queries"),
    ]
    for i, q in enumerate(task.get('expanded_queries', []), 1):
        parts.append(f"{i}. {q}")
    
    parts.extend([
        "",
        t("task_base_api"),
        f"```json\n{json.dumps(task.get('base_api', {}), indent=2, ensure_ascii=False)}\n```",
        "",
        f"{t('task_schema_candidates')} ({len(schemas)})",
    ])
    
    for i, schema in enumerate(schemas, 1):
        parts.extend([
            f"",
        f"{t('task_candidate')} {i}",
            f"```json\n{json.dumps(schema, indent=2, ensure_ascii=False)}\n```",
        ])
    
    return '\n'.join(parts)

def _approve_invalid(self, query: str, api_name: str, api_desc: str, 
                   input_schema: str, sql: str, reviewer: str, table_desc: str):
    """批准无效记录（修正后）"""
    if not self._lock_invalid_action.acquire(blocking=False):
        return self._keep_invalid_form_with_status(t("msg_action_in_progress"))
    try:
        return self._approve_invalid_inner(query, api_name, api_desc, input_schema, sql, reviewer, table_desc)
    finally:
        self._lock_invalid_action.release()

def _approve_invalid_inner(self, query: str, api_name: str, api_desc: str,
                   input_schema: str, sql: str, reviewer: str, table_desc: str):
    record = self._get_current_invalid()
    if not record:
        return self._render_invalid_interface()

    reviewer = self._ensure_reviewer(reviewer)
    if not reviewer:
        return self._render_invalid_interface_with_status(t("msg_reviewer_required"))
    
    if _build_api_schema_from_inputs is not None:
        schema_obj = self._infer_input_schema_from_sql(sql)
        built = _build_api_schema_from_inputs(
            table_name=self._infer_table_name(),
            query=query,
            api_name=api_name,
            api_desc=api_desc,
            input_schema_text=json.dumps(schema_obj, ensure_ascii=False),
            sql=sql,
        )
        api_schema = {
            "name": getattr(built, "name", api_name),
            "description": getattr(built, "description", api_desc) or self._infer_api_desc_from_query(query),
            "inputSchema": getattr(built, "inputSchema", schema_obj),
            "bound_sql": getattr(built, "bound_sql", sql),
            "slot_mapping": list((getattr(built, "slot_mapping", {}) or {}).keys()),
            "source": "manual_review",
        }
    else:
        schema_obj = self._infer_input_schema_from_sql(sql)
        api_schema = {
            "name": api_name,
            "description": (api_desc or "").strip() or self._infer_api_desc_from_query(query),
            "inputSchema": schema_obj,
            "bound_sql": sql,
            "slot_mapping": list(re.findall(r':(\w+)', sql)),
            "source": "manual_review"
        }

    # 保存到recorrect和valid
    self._save_to_recorrect(
        record,
        modified=True,
        reviewer=reviewer,
        corrected_query=query,
        corrected_api_schema=api_schema,
        corrected_sql=api_schema.get("bound_sql") if isinstance(api_schema, dict) else sql,
    )
    self._save_to_valid(api_schema, query, 
                      original_error=record.get('reason'),
                      corrected_from_invalid=True,
                      reviewer=reviewer,
                      review_method="manual_review")

    # 回调
    if self.on_approve:
        self.on_approve(api_schema, query)
    
    self._resolve_invalid_record(record, reviewer, "approved")
    self._refresh_invalid_records()
    return self._render_invalid_interface()

def _skip_invalid(self):
    """跳过当前无效记录"""
    if not self._lock_invalid_action.acquire(blocking=False):
        return self._keep_invalid_form_with_status(t("msg_action_in_progress"))
    try:
        self._current_invalid_idx += 1
        return self._render_invalid_interface()
    finally:
        self._lock_invalid_action.release()

def _reject_invalid(self, reviewer: str):
    """拒绝当前无效记录"""
    if not self._lock_invalid_action.acquire(blocking=False):
        return self._keep_invalid_form_with_status(t("msg_action_in_progress"))
    try:
        # 标记为已审但不采纳
        record = self._get_current_invalid()
        reviewer = self._ensure_reviewer(reviewer)
        if not reviewer:
            return self._keep_invalid_form_with_status(t("msg_reviewer_required"))
        if record:
            self._save_to_recorrect(record, modified=False, reviewer=reviewer)
        
        self._resolve_invalid_record(record, reviewer, "rejected")
        self._refresh_invalid_records()
        return self._render_invalid_interface()
    finally:
        self._lock_invalid_action.release()

def _approve_task(self, comment: str, reviewer: str, table_desc: str):
    """批准审核任务"""
    if not self._lock_task_action.acquire(blocking=False):
        return self._render_task_interface_with_status(t("msg_action_in_progress"))
    try:
        original_task = self._get_current_task()
        if not original_task:
            return self._render_task_interface()

        task = self._normalize_review_task(original_task)

        reviewer = self._ensure_reviewer(reviewer)
        if not reviewer:
            return self._render_task_interface_with_status(t("msg_reviewer_required"))
        
        # 人工通过仍需LLM校验（如果有SQL）
        correct_api = task.get('correct_api')
        sql = correct_api.get('bound_sql') if isinstance(correct_api, dict) else None
        ok = True
        if sql:
            exec_result = execute_sql(None, sql)
            try:
                ok = IntentVerifier().verify(task.get('query', ''), sql, exec_result)
            except Exception:
                ok = False

        if not ok:
            self._update_task_status(original_task['task_id'], 'rejected', "llm_verify_failed", reviewer)
            return self._render_task_interface()

        self._update_task_status(original_task['task_id'], 'approved', comment, reviewer)
        
        # 如果有correct_api，加入valid
        if correct_api and task.get('task_type') == 'runtime_correction':
            correct_api = dict(correct_api)
            sql = correct_api.get('bound_sql') or ""
            correct_api['inputSchema'] = self._infer_input_schema_from_sql(sql)
            if sql:
                correct_api['slot_mapping'] = list(re.findall(r':(\w+)', sql))
            if not (correct_api.get('description') or '').strip():
                correct_api['description'] = self._infer_api_desc_from_query(task.get('query', ''))

            self._save_to_valid(correct_api, task.get('query', ''),
                      from_runtime_correction=True,
                              distinction_instruction=task.get('distinction_instruction'),
                              reviewer=reviewer,
                              review_method="manual_review")
        
        self._current_task_idx += 1
        return self._render_task_interface()
    finally:
        self._lock_task_action.release()

def _modify_task(self, comment: str, reviewer: str, table_desc: str):
    """修改后批准"""
    if not self._lock_task_action.acquire(blocking=False):
        return self._render_task_interface_with_status(t("msg_action_in_progress"))
    try:
        task = self._get_current_task()
        if not task:
            return self._render_task_interface()

        reviewer = self._ensure_reviewer(reviewer)
        if not reviewer:
            return self._render_task_interface_with_status(t("msg_reviewer_required"))
        
        self._update_task_status(task['task_id'], 'modified', comment, reviewer)
        self._current_task_idx += 1
        return self._render_task_interface()
    finally:
        self._lock_task_action.release()

def _reject_task(self, comment: str, reviewer: str, table_desc: str):
    """拒绝任务"""
    if not self._lock_task_action.acquire(blocking=False):
        return self._render_task_interface_with_status(t("msg_action_in_progress"))
    try:
        task = self._get_current_task()
        if task:
            reviewer = self._ensure_reviewer(reviewer)
            if not reviewer:
                return self._render_task_interface_with_status(t("msg_reviewer_required"))
            self._update_task_status(task['task_id'], 'rejected', comment, reviewer)
        
        self._current_task_idx += 1
        return self._render_task_interface()
    finally:
        self._lock_task_action.release()

def _next_task(self):
    """下一个任务"""
    if not self._lock_task_action.acquire(blocking=False):
        return self._render_task_interface_with_status(t("msg_action_in_progress"))
    try:
        self._current_task_idx += 1
        return self._render_task_interface()
    finally:
        self._lock_task_action.release()

def create_interface(self):
    """创建Gradio界面"""
    # 初始化数据库连接
    db_manager.connect()
    
    if gr is None:
        print("Gradio not available, cannot create interface")
        return None
    
    # Custom CSS — lightweight, airy design
    _custom_css = """
    /* ── Global layout ── */
    .gradio-container {
        max-width: 1280px !important;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Inter', 'Helvetica Neue', Arial, sans-serif !important;
    }

    /* ── Buttons: pill-shaped, flat, subtle shadow ── */
    .gr-button {
        border-radius: 20px !important;
        font-weight: 500 !important;
        font-size: 0.85rem !important;
        padding: 6px 18px !important;
        letter-spacing: 0.02em !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06) !important;
        transition: all 0.2s ease !important;
        border: 1px solid rgba(0,0,0,0.06) !important;
    }
    .gr-button:hover { box-shadow: 0 2px 8px rgba(0,0,0,0.10) !important; transform: translateY(-1px) !important; }
    .gr-button.primary {
        background: #5b6abf !important;
        color: #fff !important;
        border: none !important;
    }
    .gr-button.primary:hover { background: #4a59b0 !important; }
    .gr-button.stop {
        background: #fff !important;
        color: #e74c5e !important;
        border: 1px solid #e74c5e !important;
    }
    .gr-button.stop:hover { background: #fef2f2 !important; }

    /* ── Panels & cards ── */
    .gr-panel, .gr-box, .gr-form {
        border-radius: 14px !important;
        border: 1px solid #eef0f4 !important;
        box-shadow: 0 1px 4px rgba(0,0,0,0.03) !important;
    }

    /* ── Inputs: slimmer, rounder ── */
    .gr-input, .gr-text-input, textarea, input[type="text"] {
        border-radius: 10px !important;
        border: 1px solid #e2e5eb !important;
        font-size: 0.88rem !important;
        padding: 8px 12px !important;
    }

    /* ── Tabs: minimal underline style ── */
    .tabs > .tab-nav > button {
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        padding: 8px 16px !important;
        border-radius: 8px 8px 0 0 !important;
        letter-spacing: 0.01em !important;
    }

    /* ── Accordion: lighter headers ── */
    .gr-accordion > .label-wrap {
        font-size: 0.88rem !important;
        font-weight: 500 !important;
        color: #4a5568 !important;
    }

    /* ── Tables: compact rows ── */
    table { font-size: 0.84rem !important; }
    table th { font-weight: 600 !important; color: #4a5568 !important; }
    table td { padding: 6px 10px !important; }

    /* ── Markdown inside UI ── */
    .prose h2 { font-size: 1.15rem !important; font-weight: 600 !important; color: #2d3748 !important; }
    .prose h3 { font-size: 1rem !important; font-weight: 600 !important; color: #4a5568 !important; }
    .prose h4 { font-size: 0.92rem !important; font-weight: 600 !important; color: #718096 !important; }

    /* ── Subtitle ── */
    .subtitle { opacity: 0.65; font-size: 0.85rem !important; margin-top: -8px !important; }

    /* ── Hide footer ── */
    footer { display: none !important; }

    /* ── Spacing tweaks ── */
    .gr-padded { padding: 12px !important; }
    .gap { gap: 10px !important; }
    """

    with gr.Blocks(title="NL2AutoAPI Workbench", css=_custom_css, theme=gr.themes.Soft()) as demo:
        gr.Markdown(t("app_title"))
        gr.Markdown(t("app_subtitle"), elem_classes=["subtitle"])

        with gr.Row():
            reviewer_input = gr.Textbox(label=t("reviewer"), placeholder=t("reviewer_placeholder"), value="", scale=1)
            
            # Infer table name from first invalid record
            inferred_table = "base_staff"
            invalid_records = self._load_invalid()
            if invalid_records and invalid_records[0].get('table'):
                inferred_table = invalid_records[0].get('table')
            
            # Auto-generated table description (read-only)
            auto_desc = self._generate_table_desc_from_db(inferred_table)
            table_desc_input = gr.Textbox(label=t("table_desc_label"), value=auto_desc, interactive=False, scale=3)
        
        with gr.Tab(t("tab_schema")):
            gr.Markdown(t("schema_title"))
            gr.Markdown(t("schema_desc"))
            
            def _load_schema_json():
                path = self._schema_path
                if not os.path.exists(path):
                    return "{}", f"{t('msg_schema_not_found')}: {path}", gr.update(), gr.update()
                with open(path, "r", encoding="utf8") as f:
                    data = json.load(f)
                fs_upd, fd_upd = _refresh_field_dropdowns(data)
                return json.dumps(data, ensure_ascii=False, indent=2), t("msg_schema_loaded", path=path), fs_upd, fd_upd
            
            def _save_schema_json(schema_text):
                path = self._schema_path
                try:
                    data = json.loads(schema_text)
                except json.JSONDecodeError as e:
                    return f"{t('msg_json_error')}: {e}", gr.update(), gr.update()
                # Version log the old schema
                if os.path.exists(path):
                    with open(path, "r", encoding="utf8") as f:
                        old_data = json.load(f)
                    self._version_mgr.log_operation("schema", "update", data, old_data, {"source": "ui_edit"})
                else:
                    self._version_mgr.log_operation("schema", "insert", data, meta={"source": "ui_create"})
                
                with open(path, "w", encoding="utf8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                fs_upd, fd_upd = _refresh_field_dropdowns(data)
                return t("msg_schema_saved", path=path), fs_upd, fd_upd
            
            def _get_field_list():
                path = self._schema_path
                if not os.path.exists(path):
                    return []
                with open(path, "r", encoding="utf8") as f:
                    data = json.load(f)
                table_name = self._infer_table_name()
                table = data.get("tables", {}).get(table_name, {})
                fields = table.get("fields", {})
                return list(fields.keys())
            
            def _refresh_field_dropdowns(schema_data=None):
                """Return gr.update for both field dropdowns based on current schema."""
                if schema_data is None:
                    fields = _get_field_list()
                else:
                    table_name = self._infer_table_name()
                    table = schema_data.get("tables", {}).get(table_name, {})
                    fields = list(table.get("fields", {}).keys())
                return (
                    gr.update(choices=fields, value=fields[0] if fields else None),
                    gr.update(choices=fields, value=None),
                )

            def _run_global_autofix(rounds_str):
                if not self._lock_autofix.acquire(blocking=False):
                    yield gr.update(), t("msg_autofix_running"), "", gr.update(), gr.update()
                    return
                try:
                    rounds = max(1, min(10, int(rounds_str or 1)))
                    path = self._schema_path
                    if not os.path.exists(path):
                        yield "{}", t("msg_schema_not_found"), "", gr.update(), gr.update()
                        return

                    with open(path, "r", encoding="utf8") as f:
                        data = json.load(f)

                    table_name = self._infer_table_name()
                    old_data_str = json.dumps(data, ensure_ascii=False)

                    # Use threading + queue for real-time log streaming
                    import threading, queue
                    log_queue = queue.Queue()
                    live_lines = []
                    result_holder = [None, None]  # [data, progress_log]

                    def _worker():
                        d, plog = auto_fix_all_fields(
                            table_name, data, rounds=rounds,
                            log_callback=lambda msg: log_queue.put(msg),
                        )
                        result_holder[0] = d
                        result_holder[1] = plog
                        log_queue.put(None)  # sentinel

                    worker = threading.Thread(target=_worker, daemon=True)
                    worker.start()

                    # Stream logs to UI in real-time
                    while True:
                        try:
                            msg = log_queue.get(timeout=0.5)
                        except queue.Empty:
                            # Yield current state to keep UI responsive
                            if live_lines:
                                yield gr.update(), t("msg_autofix_running"), "\n".join(live_lines), gr.update(), gr.update()
                            continue
                        if msg is None:
                            break
                        live_lines.append(msg)
                        yield gr.update(), t("msg_autofix_running"), "\n".join(live_lines), gr.update(), gr.update()

                    worker.join(timeout=5)

                    # Final result
                    final_data = result_holder[0] if result_holder[0] is not None else data
                    progress_log = result_holder[1] or live_lines

                    # Save
                    self._version_mgr.log_operation("schema", "update", final_data, json.loads(old_data_str), {"source": "global_autofix", "rounds": rounds})
                    with open(path, "w", encoding="utf8") as f:
                        json.dump(final_data, f, ensure_ascii=False, indent=2)

                    log_text = "\n".join(progress_log) if progress_log else t("msg_no_log")
                    fs_upd, fd_upd = _refresh_field_dropdowns(final_data)
                    yield json.dumps(final_data, ensure_ascii=False, indent=2), t("msg_global_autofix_done", rounds=rounds), log_text, fs_upd, fd_upd
                finally:
                    self._lock_autofix.release()

            def _run_field_autofix(field_name, rounds_str):
                if not self._lock_autofix.acquire(blocking=False):
                    yield gr.update(), t("msg_autofix_running"), "", gr.update(), gr.update()
                    return
                try:
                    rounds = max(1, min(10, int(rounds_str or 3)))
                    field_name = (field_name or "").strip()
                    if not field_name:
                        yield "{}", t("msg_select_field"), "", gr.update(), gr.update()
                        return

                    path = self._schema_path
                    if not os.path.exists(path):
                        yield "{}", t("msg_schema_not_found"), "", gr.update(), gr.update()
                        return

                    with open(path, "r", encoding="utf8") as f:
                        data = json.load(f)

                    table_name = self._infer_table_name()
                    old_data_str = json.dumps(data, ensure_ascii=False)

                    # Use threading + queue for real-time log streaming
                    import threading, queue
                    log_queue = queue.Queue()
                    live_lines = []
                    result_holder = [None, None, None]  # [new_desc, data, progress_log]

                    def _worker():
                        nd, d, plog = auto_fix_single_field_in_schema(
                            table_name, field_name, data, rounds=rounds,
                            log_callback=lambda msg: log_queue.put(msg),
                        )
                        result_holder[0] = nd
                        result_holder[1] = d
                        result_holder[2] = plog
                        log_queue.put(None)  # sentinel

                    worker = threading.Thread(target=_worker, daemon=True)
                    worker.start()

                    # Stream logs to UI in real-time
                    while True:
                        try:
                            msg = log_queue.get(timeout=0.5)
                        except queue.Empty:
                            if live_lines:
                                yield gr.update(), t("msg_autofix_running"), "\n".join(live_lines), gr.update(), gr.update()
                            continue
                        if msg is None:
                            break
                        live_lines.append(msg)
                        yield gr.update(), t("msg_autofix_running"), "\n".join(live_lines), gr.update(), gr.update()

                    worker.join(timeout=5)

                    # Final result
                    new_desc = result_holder[0] or ""
                    final_data = result_holder[1] if result_holder[1] is not None else data
                    progress_log = result_holder[2] or live_lines

                    self._version_mgr.log_operation("schema", "update", final_data, json.loads(old_data_str),
                                                     {"source": "field_autofix", "field": field_name, "rounds": rounds})
                    with open(path, "w", encoding="utf8") as f:
                        json.dump(final_data, f, ensure_ascii=False, indent=2)

                    log_text = "\n".join(progress_log) if progress_log else t("msg_no_log")
                    fs_upd, fd_upd = _refresh_field_dropdowns(final_data)
                    yield json.dumps(final_data, ensure_ascii=False, indent=2), t("msg_field_autofix_done", field=field_name, desc=new_desc), log_text, fs_upd, fd_upd
                finally:
                    self._lock_autofix.release()

            def _generate_schema_from_db():
                """从数据库生成 Schema JSON"""
                table_name = self._infer_table_name()
                try:
                    db_conn = db_manager.connect()
                    if db_conn is None:
                        return "{}", t("msg_db_connect_fail"), gr.update(), gr.update()
                    from core.config import db_config as _db_config
                    schema_data = build_schema_from_db(db_conn, _db_config.database, table_name)
                    path = self._schema_path
                    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                    with open(path, "w", encoding="utf8") as f:
                        json.dump(schema_data, f, ensure_ascii=False, indent=2)
                    self._version_mgr.log_operation("schema", "insert", schema_data, meta={"source": "db_generate"})
                    field_count = sum(len(t.get("fields", {})) for t in schema_data.get("tables", {}).values())
                    table_count = len(schema_data.get("tables", {}))
                    fs_upd, fd_upd = _refresh_field_dropdowns(schema_data)
                    return json.dumps(schema_data, ensure_ascii=False, indent=2), t("msg_schema_gen_done", tables=table_count, fields=field_count, path=path), fs_upd, fd_upd
                except Exception as e:
                    return "{}", t("msg_schema_gen_fail", error=str(e)), gr.update(), gr.update()
            
            def _run_prebuild():
                """运行 Prebuild 流程 (generator, yields log lines)"""
                if not self._lock_prebuild.acquire(blocking=False):
                    yield t("msg_prebuild_running")
                    return
                try:
                    yield from _run_prebuild_inner()
                finally:
                    self._lock_prebuild.release()

            def _run_prebuild_inner():
                path = self._schema_path
                if not os.path.exists(path):
                    yield t("msg_schema_not_found")
                    return
                
                table_name = self._infer_table_name()
                output_dir = os.path.dirname(self.valid_path) or "./output"
                root_output = os.path.dirname(output_dir) or "./output"
                
                import subprocess, threading, queue as _queue
                venv_python = sys.executable
                cmd = [
                    venv_python, "-u", "pre_build.py",
                    "--schema", path,
                    "--tables", table_name,
                    "--output-dir", root_output,
                ]
                cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                log_lines: list[str] = []
                yield t("msg_prebuild_starting")
                
                try:
                    proc = subprocess.Popen(
                        cmd, cwd=cwd,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        text=True, bufsize=1,
                        env={**os.environ, "PYTHONUNBUFFERED": "1"},
                    )
                    
                    # Read stdout line by line and yield accumulated log
                    for line in iter(proc.stdout.readline, ""):
                        stripped = line.rstrip("\n")
                        if stripped:
                            log_lines.append(stripped)
                            # Yield the full accumulated log so far
                            yield "\n".join(log_lines)
                    
                    proc.stdout.close()
                    retcode = proc.wait(timeout=600)
                    
                    if retcode == 0:
                        log_lines.append(t("msg_prebuild_done"))
                    else:
                        log_lines.append(f"{t('msg_prebuild_failed')} (returncode={retcode})")
                    yield "\n".join(log_lines)
                    
                except subprocess.TimeoutExpired:
                    proc.kill()
                    log_lines.append(t("msg_prebuild_timeout"))
                    yield "\n".join(log_lines)
                except Exception as e:
                    log_lines.append(f"{t('msg_prebuild_failed')}: {e}")
                    yield "\n".join(log_lines)
            
            def _prebuild_with_check():
                """Prebuild with auto-fix check (generator)"""
                path = self._schema_path
                if not os.path.exists(path):
                    yield t("msg_schema_not_found")
                    return
                
                # Block full prebuild if valid.jsonl already has data (especially user-edited records)
                from core.utils import load_jsonl
                existing_records = load_jsonl(self.valid_path) if os.path.exists(self.valid_path) else []
                user_edited_count = sum(1 for r in existing_records if r.get("user_edited"))
                total_count = len(existing_records)

                if total_count > 0:
                    yield t("msg_prebuild_blocked", total=total_count, edited=user_edited_count)
                    return

                # Check if binlog has auto-fix records
                entries = self._version_mgr.read_binlog("schema")
                has_autofix = any(
                    (e.get("meta") or {}).get("source") in ("global_autofix", "field_autofix", "auto_prune")
                    for e in entries
                )
                if not has_autofix:
                    yield t("msg_prebuild_no_autofix")
                    return
                
                yield from _run_prebuild()
            
            def _delete_schema_field(field_name):
                field_name = (field_name or "").strip()
                if not field_name:
                    return "{}", t("msg_select_field"), gr.update(), gr.update()
                
                path = self._schema_path
                if not os.path.exists(path):
                    return "{}", t("msg_schema_not_found"), gr.update(), gr.update()
                
                with open(path, "r", encoding="utf8") as f:
                    data = json.load(f)
                
                table_name = self._infer_table_name()
                table = data.get("tables", {}).get(table_name, {})
                fields = table.get("fields", {})
                if field_name not in fields:
                    fs_upd, fd_upd = _refresh_field_dropdowns(data)
                    return json.dumps(data, ensure_ascii=False, indent=2), t("msg_field_not_found", field=field_name), fs_upd, fd_upd
                
                old_data_str = json.dumps(data, ensure_ascii=False)
                del fields[field_name]
                
                self._version_mgr.log_operation("schema", "update", data, json.loads(old_data_str),
                                                 {"source": "field_delete", "field": field_name})
                with open(path, "w", encoding="utf8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                fs_upd, fd_upd = _refresh_field_dropdowns(data)
                return json.dumps(data, ensure_ascii=False, indent=2), t("msg_field_deleted", field=field_name), fs_upd, fd_upd
            
            def _auto_prune_fields():
                path = self._schema_path
                if not os.path.exists(path):
                    return "{}", t("msg_schema_not_found"), gr.update(), gr.update()
                
                with open(path, "r", encoding="utf8") as f:
                    data = json.load(f)
                
                table_name = self._infer_table_name()
                old_data_str = json.dumps(data, ensure_ascii=False)
                
                pruned, data = auto_prune_useless_fields(table_name, data)
                
                if pruned:
                    self._version_mgr.log_operation("schema", "update", data, json.loads(old_data_str),
                                                     {"source": "auto_prune", "pruned_fields": pruned})
                    with open(path, "w", encoding="utf8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                
                fs_upd, fd_upd = _refresh_field_dropdowns(data)
                if pruned:
                    return json.dumps(data, ensure_ascii=False, indent=2), t("msg_prune_done", count=len(pruned), fields=', '.join(pruned)), fs_upd, fd_upd
                else:
                    return json.dumps(data, ensure_ascii=False, indent=2), t("msg_prune_none"), fs_upd, fd_upd
            
            schema_status_md = gr.Markdown("")
            
            with gr.Accordion(t("schema_gen_accordion"), open=True):
                gr.Markdown(t("schema_gen_desc"))
                gen_schema_btn = gr.Button(t("btn_gen_schema"), variant="primary")
            
            with gr.Accordion(t("schema_editor_accordion"), open=True):
                schema_editor = gr.TextArea(label=t("schema_editor_label"), lines=28, max_lines=60)
                with gr.Row():
                    schema_load_btn = gr.Button(t("btn_reload"))
                    schema_save_btn = gr.Button(t("btn_save"), variant="primary")
            
            with gr.Accordion(t("field_mgmt_accordion"), open=True):
                gr.Markdown(t("field_ops_title"))
                with gr.Row():
                    with gr.Column(scale=1):
                        field_choices = _get_field_list()
                        field_del_select = gr.Dropdown(label=t("field_del_label"), choices=field_choices, value=None)
                        field_del_btn = gr.Button(t("btn_delete_field"), variant="stop")
                    with gr.Column(scale=1):
                        gr.Markdown(t("smart_prune_desc"))
                        auto_prune_btn = gr.Button(t("btn_smart_prune"), variant="primary")
                
                gr.Markdown("---")
                gr.Markdown(t("autofix_desc_title"))
                gr.Markdown(t("autofix_desc_hint"))
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown(t("global_autofix_title"))
                        global_fix_rounds = gr.Number(label=t("rounds_label"), value=1, minimum=1, maximum=10, precision=0)
                        global_fix_btn = gr.Button(t("btn_global_autofix"), variant="primary")
                    with gr.Column(scale=1):
                        gr.Markdown(t("single_field_autofix_title"))
                        field_select = gr.Dropdown(label=t("field_select_label"), choices=field_choices, value=field_choices[0] if field_choices else None)
                        field_fix_rounds = gr.Number(label=t("rounds_label"), value=3, minimum=1, maximum=10, precision=0)
                        field_fix_btn = gr.Button(t("btn_field_autofix"))
                
                gr.Markdown(t("autofix_log_title"))
                autofix_log_output = gr.TextArea(label=t("autofix_log_label"), lines=12, max_lines=30, interactive=False)
            
            with gr.Accordion(t("prebuild_accordion"), open=True):
                gr.Markdown(t("prebuild_desc"))
                prebuild_btn = gr.Button(t("btn_prebuild"), variant="primary")
                prebuild_status = gr.TextArea(label=t("prebuild_log_label"), lines=16, max_lines=40, interactive=False)
            
            gen_schema_btn.click(_generate_schema_from_db, outputs=[schema_editor, schema_status_md, field_select, field_del_select])
            prebuild_btn.click(_prebuild_with_check, outputs=[prebuild_status])
            schema_load_btn.click(_load_schema_json, outputs=[schema_editor, schema_status_md, field_select, field_del_select])
            schema_save_btn.click(_save_schema_json, inputs=[schema_editor], outputs=[schema_status_md, field_select, field_del_select])
            field_del_btn.click(_delete_schema_field, inputs=[field_del_select], outputs=[schema_editor, schema_status_md, field_select, field_del_select])
            auto_prune_btn.click(_auto_prune_fields, outputs=[schema_editor, schema_status_md, field_select, field_del_select])
            global_fix_btn.click(_run_global_autofix, inputs=[global_fix_rounds], outputs=[schema_editor, schema_status_md, autofix_log_output, field_select, field_del_select])
            field_fix_btn.click(_run_field_autofix, inputs=[field_select, field_fix_rounds], outputs=[schema_editor, schema_status_md, autofix_log_output, field_select, field_del_select])
            demo.load(_load_schema_json, outputs=[schema_editor, schema_status_md, field_select, field_del_select])

        with gr.Tab(t("tab_dataset")):
            gr.Markdown(t("dataset_title"))
            gr.Markdown(t("dataset_desc"))
            
            valid_page_state = gr.State(value=0)
            VALID_PAGE_SIZE = 20
            
            def _load_valid_page(page_idx):
                page_idx = int(page_idx or 0)
                records = load_jsonl(self.valid_path) if os.path.exists(self.valid_path) else []
                total = len(records)
                total_pages = max(1, (total + VALID_PAGE_SIZE - 1) // VALID_PAGE_SIZE)
                page_idx = max(0, min(page_idx, total_pages - 1))
                start = page_idx * VALID_PAGE_SIZE
                page_records = records[start:start + VALID_PAGE_SIZE]
                
                rows = []
                for idx, rec in enumerate(page_records):
                    api = rec.get("api_schema") or {}
                    rows.append([
                        start + idx,
                        api.get("name", ""),
                        (rec.get("query") or "")[:80],
                        (api.get("bound_sql") or rec.get("sql") or "")[:100],
                        api.get("description", "")[:60],
                        rec.get("source", ""),
                    ])
                
                info = t("msg_page_info", total=total, page=page_idx + 1, pages=total_pages)
                return rows, info, page_idx
            
            def _valid_prev(page_idx):
                return _load_valid_page(max(0, int(page_idx or 0) - 1))
            
            def _valid_next(page_idx):
                return _load_valid_page(int(page_idx or 0) + 1)
            
            def _load_valid_detail(idx_str):
                try:
                    idx = int(idx_str)
                except (TypeError, ValueError):
                    return "", "", "{}", "", ""
                records = load_jsonl(self.valid_path) if os.path.exists(self.valid_path) else []
                if idx < 0 or idx >= len(records):
                    return "", "", "{}", "", ""
                rec = records[idx]
                api = rec.get("api_schema") or {}
                return (
                    rec.get("query", ""),
                    api.get("name", ""),
                    json.dumps(api, ensure_ascii=False, indent=2),
                    api.get("bound_sql") or rec.get("sql", ""),
                    api.get("description", ""),
                )
            
            def _save_valid_edit(idx_str, query, api_name, api_json_str, sql, desc):
                if not self._lock_valid_write.acquire(blocking=False):
                    return t("msg_write_in_progress"), ""
                try:
                    return _save_valid_edit_inner(idx_str, query, api_name, api_json_str, sql, desc)
                finally:
                    self._lock_valid_write.release()

            def _save_valid_edit_inner(idx_str, query, api_name, api_json_str, sql, desc):
                try:
                    idx = int(idx_str)
                except (TypeError, ValueError):
                    return t("msg_invalid_index"), ""
                records = load_jsonl(self.valid_path) if os.path.exists(self.valid_path) else []
                if idx < 0 or idx >= len(records):
                    return t("msg_index_out_of_range"), ""
                
                old_record = records[idx]
                try:
                    new_api = json.loads(api_json_str)
                except json.JSONDecodeError:
                    return t("msg_json_error"), ""
                
                # Detect SQL and query changes for schema feedback
                old_sql = (old_record.get("api_schema") or {}).get("bound_sql") or old_record.get("sql") or ""
                new_sql = sql or ""
                old_query = old_record.get("query") or ""
                
                new_api["name"] = api_name
                new_api["bound_sql"] = sql
                new_api["description"] = desc
                records[idx] = {**old_record, "query": query, "api_schema": new_api, "user_edited": True}
                
                overwrite_jsonl(self.valid_path, records)
                self._version_mgr.log_operation("valid", "update", records[idx], old_record, {"source": "ui_edit"})
                if invalidate_registry_cache:
                    invalidate_registry_cache()
                
                # Analyze SQL/query modification for schema feedback
                suggestion_text = ""
                sql_changed = old_sql.strip() and new_sql.strip() and old_sql.strip() != new_sql.strip()
                query_changed = old_query.strip() and query.strip() and old_query.strip() != query.strip()
                if sql_changed or query_changed:
                    analysis = self._analyze_and_suggest_schema_updates(query, old_sql, new_sql, old_query=old_query)
                    suggestion_text = analysis.get("suggestion_text", "")
                
                return t("msg_record_saved", idx=idx), suggestion_text
            
            def _delete_valid_record(idx_str):
                if not self._lock_valid_write.acquire(blocking=False):
                    return "⚠️ Another write operation is in progress. Please wait."
                try:
                    return _delete_valid_record_inner(idx_str)
                finally:
                    self._lock_valid_write.release()

            def _delete_valid_record_inner(idx_str):
                try:
                    idx = int(idx_str)
                except (TypeError, ValueError):
                    return t("msg_invalid_index")
                records = load_jsonl(self.valid_path) if os.path.exists(self.valid_path) else []
                if idx < 0 or idx >= len(records):
                    return t("msg_index_out_of_range")
                
                deleted = records.pop(idx)
                overwrite_jsonl(self.valid_path, records)
                self._version_mgr.log_operation("valid", "delete", None, deleted, {"source": "ui_delete"})
                if invalidate_registry_cache:
                    invalidate_registry_cache()
                return t("msg_record_deleted", idx=idx)
            
            valid_info_md = gr.Markdown("Loading...")
            valid_table = gr.Dataframe(
                headers=t_list("table_headers"),
                datatype=["number", "str", "str", "str", "str", "str"],
                interactive=False,
            )
            with gr.Row():
                valid_prev_btn = gr.Button(t("btn_prev_page"))
                valid_next_btn = gr.Button(t("btn_next_page"))
                valid_refresh_btn = gr.Button(t("btn_refresh"))
            
            gr.Markdown(t("edit_delete_title"))
            valid_edit_idx = gr.Textbox(label=t("record_idx_label"), value="")
            valid_load_btn = gr.Button(t("btn_load_record"))
            with gr.Row():
                with gr.Column():
                    valid_edit_query = gr.Textbox(label=t("query_label"), lines=2)
                    valid_edit_api_name = gr.Textbox(label=t("api_name_label"))
                    valid_edit_desc = gr.Textbox(label=t("api_desc_label"), lines=2)
                    valid_edit_sql = gr.TextArea(label=t("sql_label"), lines=4)
                    valid_edit_api_json = gr.TextArea(label=t("api_schema_json_label"), lines=10)
                with gr.Column():
                    valid_save_btn = gr.Button(t("btn_save_edit"), variant="primary")
                    valid_delete_btn = gr.Button(t("btn_delete_record"), variant="stop")
                    valid_edit_status = gr.Markdown("")
            
            with gr.Accordion(t("schema_feedback_accordion_edit"), open=True):
                valid_schema_feedback = gr.Markdown(t("schema_feedback_default_edit"))
                with gr.Row():
                    valid_apply_suggestion_btn = gr.Button(t("btn_apply_suggestion"), variant="primary", visible=False)
                    valid_dismiss_suggestion_btn = gr.Button(t("btn_dismiss_suggestion"), visible=False)
            
            valid_prev_btn.click(_valid_prev, inputs=[valid_page_state], outputs=[valid_table, valid_info_md, valid_page_state])
            valid_next_btn.click(_valid_next, inputs=[valid_page_state], outputs=[valid_table, valid_info_md, valid_page_state])
            valid_refresh_btn.click(_load_valid_page, inputs=[valid_page_state], outputs=[valid_table, valid_info_md, valid_page_state])
            valid_load_btn.click(_load_valid_detail, inputs=[valid_edit_idx],
                                 outputs=[valid_edit_query, valid_edit_api_name, valid_edit_api_json, valid_edit_sql, valid_edit_desc])
            def _on_valid_save(idx_str, query, api_name, api_json_str, sql, desc):
                status, suggestion = _save_valid_edit(idx_str, query, api_name, api_json_str, sql, desc)
                has_suggestions = bool(suggestion and t("feedback_suggested_fields") in suggestion)
                return (
                    status,
                    suggestion or t("schema_feedback_default_edit"),
                    gr.update(visible=has_suggestions),
                    gr.update(visible=has_suggestions),
                )

            valid_save_btn.click(_on_valid_save,
                                 inputs=[valid_edit_idx, valid_edit_query, valid_edit_api_name, valid_edit_api_json, valid_edit_sql, valid_edit_desc],
                                 outputs=[valid_edit_status, valid_schema_feedback, valid_apply_suggestion_btn, valid_dismiss_suggestion_btn])
            valid_delete_btn.click(_delete_valid_record, inputs=[valid_edit_idx], outputs=[valid_edit_status])
            
            def _on_apply_suggestion():
                result = self._apply_schema_suggestions()
                return result, gr.update(visible=False), gr.update(visible=False)
            
            def _on_dismiss_suggestion():
                result = self._dismiss_schema_suggestions()
                return result, gr.update(visible=False), gr.update(visible=False)
            
            valid_apply_suggestion_btn.click(_on_apply_suggestion,
                                              outputs=[valid_schema_feedback, valid_apply_suggestion_btn, valid_dismiss_suggestion_btn])
            valid_dismiss_suggestion_btn.click(_on_dismiss_suggestion,
                                               outputs=[valid_schema_feedback, valid_apply_suggestion_btn, valid_dismiss_suggestion_btn])
            demo.load(lambda: _load_valid_page(0), outputs=[valid_table, valid_info_md, valid_page_state])

        with gr.Tab(t("tab_validation")):
            gr.Markdown(t("validation_title"))
            gr.Markdown(t("validation_desc"))
            
            progress_invalid = gr.Markdown("Loading...")
            content_invalid = gr.Markdown()
            
            with gr.Row():
                with gr.Column(scale=3):
                    query_input = gr.Textbox(label=t("query_label"), lines=2)
                    api_name_input = gr.Textbox(label=t("api_name_auto_label"))
                    api_desc_input = gr.Textbox(label=t("api_desc_auto_label"), lines=2)
                    input_schema_input = gr.TextArea(label=t("input_schema_label"), lines=5, interactive=False)
                    sql_input = gr.TextArea(label=t("sql_editable_label"), lines=4)
                
                with gr.Column(scale=2):
                    gr.Markdown(t("ops_title"))
                    concretize_query_btn = gr.Button(t("btn_concretize"))
                    auto_sql_btn = gr.Button(t("btn_auto_sql"))
                    auto_generate_btn = gr.Button(t("btn_auto_review"))
                    approve_btn = gr.Button(t("btn_approve"), variant="primary")
                    skip_btn = gr.Button(t("btn_skip"))
                    reject_btn = gr.Button(t("btn_reject"))
                    auto_status_md = gr.Markdown(t("validation_hint"))
            
            with gr.Accordion(t("schema_feedback_accordion_approve"), open=True):
                invalid_schema_feedback = gr.Markdown(t("schema_feedback_default_approve"))
                with gr.Row():
                    invalid_apply_suggestion_btn = gr.Button(t("btn_apply_suggestion"), variant="primary", visible=False)
                    invalid_dismiss_suggestion_btn = gr.Button(t("btn_dismiss_suggestion"), visible=False)
            
            # Hidden table_name for concretize_query usage
            table_name_input = gr.Textbox(value="base_staff", visible=False)

            concretize_query_btn.click(
                self._concretize_query,
                inputs=[query_input, table_name_input, sql_input],
                outputs=[query_input, auto_status_md]
            )

            auto_sql_btn.click(
                self._auto_generate_sql_fields,
                inputs=[query_input, table_name_input, table_desc_input],
                outputs=[api_name_input, api_desc_input, input_schema_input, sql_input, auto_status_md]
            )
            
            # Approve with schema feedback analysis
            def _approve_with_feedback(query, api_name, api_desc, input_schema, sql, reviewer, table_desc):
                # Remember old SQL and query before approve
                record = self._get_current_invalid()
                old_sql = ""
                old_query = ""
                if record:
                    api_data = record.get("api") or record.get("api_schema") or {}
                    old_sql = record.get("sql") or record.get("invoked_sql") or api_data.get("bound_sql") or ""
                    old_query = record.get("query") or ""
                
                result = self._approve_invalid(query, api_name, api_desc, input_schema, sql, reviewer, table_desc)
                
                # Analyze SQL/query modification
                new_sql = sql or ""
                suggestion_text = t("schema_feedback_default_approve")
                has_suggestions = False
                sql_changed = old_sql.strip() and new_sql.strip() and old_sql.strip() != new_sql.strip()
                query_changed = old_query.strip() and query.strip() and old_query.strip() != query.strip()
                if sql_changed or query_changed:
                    analysis = self._analyze_and_suggest_schema_updates(query, old_sql, new_sql, old_query=old_query)
                    if analysis.get("suggestion_text"):
                        suggestion_text = analysis["suggestion_text"]
                        has_suggestions = analysis.get("has_suggestions", False)
                
                if isinstance(result, list):
                    result.extend([
                        gr.update(value=suggestion_text),
                        gr.update(visible=has_suggestions),
                        gr.update(visible=has_suggestions),
                    ])
                return result

            approve_btn.click(
                _approve_with_feedback,
                inputs=[query_input, api_name_input, api_desc_input, 
                       input_schema_input, sql_input, reviewer_input, table_desc_input],
                outputs=[progress_invalid, content_invalid, query_input,
                        api_name_input, api_desc_input, input_schema_input,
                        sql_input, approve_btn, skip_btn, reject_btn, auto_status_md,
                        invalid_schema_feedback, invalid_apply_suggestion_btn, invalid_dismiss_suggestion_btn]
            )
            skip_btn.click(
                self._skip_invalid,
                outputs=[progress_invalid, content_invalid, query_input,
                        api_name_input, api_desc_input, input_schema_input,
                        sql_input, approve_btn, skip_btn, reject_btn, auto_status_md]
            )
            reject_btn.click(
                self._reject_invalid,
                inputs=[reviewer_input],
                outputs=[progress_invalid, content_invalid, query_input,
                        api_name_input, api_desc_input, input_schema_input,
                        sql_input, approve_btn, skip_btn, reject_btn, auto_status_md]
            )
            auto_generate_btn.click(
                self._auto_review_sql_only,
                inputs=[query_input, sql_input, table_name_input],
                outputs=[api_name_input, api_desc_input, input_schema_input, sql_input, auto_status_md]
            )
            
            def _on_invalid_apply():
                result = self._apply_schema_suggestions()
                return result, gr.update(visible=False), gr.update(visible=False)
            
            def _on_invalid_dismiss():
                result = self._dismiss_schema_suggestions()
                return result, gr.update(visible=False), gr.update(visible=False)
            
            invalid_apply_suggestion_btn.click(_on_invalid_apply,
                                               outputs=[invalid_schema_feedback, invalid_apply_suggestion_btn, invalid_dismiss_suggestion_btn])
            invalid_dismiss_suggestion_btn.click(_on_invalid_dismiss,
                                                 outputs=[invalid_schema_feedback, invalid_apply_suggestion_btn, invalid_dismiss_suggestion_btn])
            
            # 初始化
            demo.load(
                self._render_invalid_interface,
                outputs=[progress_invalid, content_invalid, query_input,
                        api_name_input, api_desc_input, input_schema_input,
                        sql_input, approve_btn, skip_btn, reject_btn, auto_status_md]
            )
        
        with gr.Tab(t("tab_runtime")):
            gr.Markdown(t("runtime_title"))
            gr.Markdown(t("runtime_desc"))

            if run_runtime_query is None:
                gr.Markdown(t("runtime_unavailable"))
            else:
                # 推断表名和相关路径
                inferred_table = self._infer_table_name()
                
                # 自动生成table_desc
                auto_table_desc = self._generate_table_desc_from_db(inferred_table)
                
                query_input_rt = gr.Textbox(label=t("query_label"), lines=2)
                
                with gr.Accordion(t("advanced_settings"), open=False):
                    valid_path_rt = gr.Textbox(label="valid.jsonl Path", value=self.valid_path, interactive=False)
                    table_name_rt = gr.Textbox(label="Table Name", value=inferred_table, interactive=False)
                    output_dir_rt = gr.Textbox(label="Output Dir", value=os.path.dirname(self.valid_path) or "./output", interactive=False)
                    review_queue_rt = gr.Textbox(label="Review Queue", value=self.review_queue_path, interactive=False)
                    schema_path_rt = gr.Textbox(label="Schema Path (optional)", value="", interactive=False)
                
                table_desc_rt = gr.Textbox(label=t("table_desc_label"), value=auto_table_desc, interactive=False, lines=2)

                run_btn_rt = gr.Button(t("btn_run"), variant="primary")
                status_out_rt = gr.Textbox(label=t("status_label"))
                sql_out_rt = gr.TextArea(label=t("generated_sql_label"), lines=4)
                record_out_rt = gr.TextArea(label=t("record_json_label"), lines=12)
                note_out_rt = gr.Textbox(label=t("note_label"))

                gr.Markdown(t("step1_title"))
                api_name_rt = gr.Textbox(label=t("api_name_auto_label"), interactive=False)
                api_desc_rt = gr.Textbox(label=t("api_desc_auto_label"), lines=2, interactive=False)
                input_schema_rt = gr.TextArea(label=t("input_schema_inferred_label"), lines=6, interactive=False)
                manual_sql_rt = gr.TextArea(label=t("manual_sql_label"), lines=4)

                with gr.Row():
                    fill_btn_rt = gr.Button(t("btn_fill_from_record"))
                    concretize_query_btn_rt = gr.Button(t("btn_concretize"))
                    auto_sql_btn_rt = gr.Button(t("btn_auto_sql"))
                    auto_review_btn_rt = gr.Button(t("btn_auto_review_short"))
                    run_manual_btn_rt = gr.Button(t("btn_test_query_sql"))

                gr.Markdown(t("step2_title"))
                import_valid_btn_rt = gr.Button(t("btn_import_valid"), variant="primary")
                import_note_rt = gr.Textbox(label=t("import_result_label"), interactive=False)

                run_btn_rt.click(
                    run_runtime_query,
                    inputs=[query_input_rt, valid_path_rt, table_name_rt, table_desc_rt,
                            output_dir_rt, review_queue_rt, schema_path_rt],
                    outputs=[status_out_rt, sql_out_rt, record_out_rt, note_out_rt],
                )

                fill_btn_rt.click(
                    fill_form_from_record,
                    inputs=[record_out_rt, sql_out_rt],
                    outputs=[api_name_rt, api_desc_rt, input_schema_rt, manual_sql_rt],
                )

                concretize_query_btn_rt.click(
                    self._concretize_query,
                    inputs=[query_input_rt, table_name_rt, manual_sql_rt],
                    outputs=[query_input_rt, note_out_rt],
                )

                auto_sql_btn_rt.click(
                    self._auto_generate_sql_fields,
                    inputs=[query_input_rt, table_name_rt, table_desc_rt],
                    outputs=[api_name_rt, api_desc_rt, input_schema_rt, manual_sql_rt, note_out_rt],
                )

                auto_review_btn_rt.click(
                    self._auto_review_sql_only,
                    inputs=[query_input_rt, manual_sql_rt, table_name_rt],
                    outputs=[api_name_rt, api_desc_rt, input_schema_rt, manual_sql_rt, note_out_rt],
                )

                manual_sql_rt.change(
                    fn=lambda sql: __import__('review.runtime_query_ui', fromlist=['_infer_input_schema_from_sql_str'])._infer_input_schema_from_sql_str(sql),
                    inputs=[manual_sql_rt],
                    outputs=[input_schema_rt],
                )

                run_manual_btn_rt.click(
                    run_manual_api_sql,
                    inputs=[query_input_rt, valid_path_rt, table_name_rt, output_dir_rt, review_queue_rt,
                            api_name_rt, api_desc_rt, input_schema_rt, manual_sql_rt],
                    outputs=[status_out_rt, sql_out_rt, record_out_rt, note_out_rt],
                )

                import_valid_btn_rt.click(
                    import_final_to_valid,
                    inputs=[query_input_rt, table_name_rt, valid_path_rt, api_name_rt, api_desc_rt, input_schema_rt, manual_sql_rt],
                    outputs=[import_note_rt],
                )
    
        with gr.Tab(t("tab_review_queue")):
            gr.Markdown(t("review_queue_title"))
            gr.Markdown(t("review_queue_desc"))
            
            progress_task = gr.Markdown("Loading...")
            content_task = gr.Markdown()
            
            with gr.Row():
                with gr.Column(scale=3):
                    task_query = gr.Textbox(label=t("query_label"), lines=2)
                    task_sql = gr.TextArea(label=t("sql_template_label"), lines=5)
                    task_invoked_sql = gr.TextArea(label=t("invoked_sql_label"), lines=3, interactive=False)
                    task_api_name = gr.Textbox(label=t("api_name_auto_label"), interactive=False)
                    task_api_desc = gr.Textbox(label=t("api_desc_auto_label"), lines=2, interactive=False)
                    task_input_schema = gr.TextArea(label=t("input_schema_label"), lines=5, interactive=False)
                    task_instruction = gr.TextArea(label=t("task_instruction_label"), lines=4)
                
                with gr.Column(scale=2):
                    gr.Markdown(t("review_ops_title"))
                    comment_input = gr.TextArea(label=t("comment_label"), lines=3)
                    concretize_task_btn = gr.Button(t("btn_concretize"))
                    auto_sql_task_btn = gr.Button(t("btn_auto_sql"))
                    modify_task_btn = gr.Button(t("btn_auto_review"))
                    approve_task_btn = gr.Button(t("btn_approve"), variant="primary")
                    reject_task_btn = gr.Button(t("btn_reject"))
                    next_task_btn = gr.Button(t("btn_next"))
                    auto_task_status_md = gr.Markdown(t("review_queue_hint"))

            with gr.Accordion(t("schema_feedback_accordion_approve"), open=True):
                task_schema_feedback = gr.Markdown(t("schema_feedback_default_approve"))
                with gr.Row():
                    task_apply_suggestion_btn = gr.Button(t("btn_apply_suggestion"), variant="primary", visible=False)
                    task_dismiss_suggestion_btn = gr.Button(t("btn_dismiss_suggestion"), visible=False)
            concretize_task_btn.click(
                fn=lambda q, sql: self._concretize_query(q, self._extract_task_table_name(self._get_current_task() or {}), sql),
                inputs=[task_query, task_sql],
                outputs=[task_query, auto_task_status_md]
            )
            auto_sql_task_btn.click(
                self._auto_generate_task_sql_fields,
                inputs=[task_query, table_desc_input],
                outputs=[task_api_name, task_api_desc, task_input_schema, task_sql, auto_task_status_md]
            )
            
            def _approve_task_with_feedback(query, sql, reviewer, comment):
                # Get old SQL and query before approve — use invoked_sql (filled) for schema feedback
                task = self._get_current_task()
                old_invoked_sql = ""
                old_query = ""
                if task:
                    task_norm = self._normalize_review_task(task)
                    old_invoked_sql = task_norm.get("invoked_sql") or self._extract_task_sql(task)
                    old_query = task_norm.get("query") or ""
                
                result = self._approve_task_sql_only(query, sql, reviewer, comment)
                
                # Analyze SQL/query modification using invoked_sql (filled with actual values)
                suggestion_text = t("schema_feedback_default_approve")
                has_suggestions = False
                new_sql = sql or ""
                sql_changed = old_invoked_sql.strip() and new_sql.strip() and old_invoked_sql.strip() != new_sql.strip()
                query_changed = old_query.strip() and query.strip() and old_query.strip() != query.strip()
                if sql_changed or query_changed:
                    analysis = self._analyze_and_suggest_schema_updates(query, old_invoked_sql, new_sql, old_query=old_query)
                    if analysis.get("suggestion_text"):
                        suggestion_text = analysis["suggestion_text"]
                        has_suggestions = analysis.get("has_suggestions", False)
                
                if isinstance(result, list):
                    result.extend([
                        gr.update(value=suggestion_text),
                        gr.update(visible=has_suggestions),
                        gr.update(visible=has_suggestions),
                    ])
                return result

            approve_task_btn.click(
                _approve_task_with_feedback,
                inputs=[task_query, task_sql, reviewer_input, comment_input],
                outputs=[progress_task, content_task, task_query, task_sql,
                    task_invoked_sql, task_api_name, task_api_desc, task_input_schema,
                    task_instruction, approve_task_btn, modify_task_btn,
                    reject_task_btn, comment_input, next_task_btn, auto_task_status_md,
                    task_schema_feedback, task_apply_suggestion_btn, task_dismiss_suggestion_btn]
            )
            modify_task_btn.click(
                self._auto_review_task_sql_only,
                inputs=[task_query, task_sql],
                outputs=[task_api_name, task_api_desc, task_input_schema, task_sql, auto_task_status_md]
            )
            reject_task_btn.click(
                self._reject_task,
                inputs=[comment_input, reviewer_input, table_desc_input],
                outputs=[progress_task, content_task, task_query, task_sql,
                    task_invoked_sql, task_api_name, task_api_desc, task_input_schema,
                    task_instruction, approve_task_btn, modify_task_btn,
                    reject_task_btn, comment_input, next_task_btn, auto_task_status_md]
            )
            next_task_btn.click(
                self._next_task,
                outputs=[progress_task, content_task, task_query, task_sql,
                    task_invoked_sql, task_api_name, task_api_desc, task_input_schema,
                    task_instruction, approve_task_btn, modify_task_btn,
                    reject_task_btn, comment_input, next_task_btn, auto_task_status_md]
            )
            
            def _on_task_apply():
                result = self._apply_schema_suggestions()
                return result, gr.update(visible=False), gr.update(visible=False)
            
            def _on_task_dismiss():
                result = self._dismiss_schema_suggestions()
                return result, gr.update(visible=False), gr.update(visible=False)
            
            task_apply_suggestion_btn.click(_on_task_apply,
                                            outputs=[task_schema_feedback, task_apply_suggestion_btn, task_dismiss_suggestion_btn])
            task_dismiss_suggestion_btn.click(_on_task_dismiss,
                                              outputs=[task_schema_feedback, task_apply_suggestion_btn, task_dismiss_suggestion_btn])
            
            # 初始化
            demo.load(
                self._render_task_interface,
                outputs=[progress_task, content_task, task_query, task_sql,
                    task_invoked_sql, task_api_name, task_api_desc, task_input_schema,
                    task_instruction, approve_task_btn, modify_task_btn,
                    reject_task_btn, comment_input, next_task_btn, auto_task_status_md]
            )
        
        with gr.Tab(t("tab_version")):
            gr.Markdown(t("version_title"))
            gr.Markdown(t("version_desc"))
            
            def _get_version_summary():
                summaries = []
                for ds_name in ["valid", "invalid", "schema", "boundary", "recorrect", "review_queue"]:
                    s = self._version_mgr.get_summary(ds_name)
                    if s['total_ops'] == 0:
                        continue
                    summaries.append(f"### {ds_name}\n- 总操作数: {s['total_ops']}\n- 首次操作: {s.get('first_ts', 'N/A')}\n- 最近操作: {s.get('last_ts', 'N/A')}\n- 分布: {s.get('ops', {})}\n")
                return "\n".join(summaries) if summaries else t("msg_no_version_records")
            
            def _get_binlog_detail(dataset_name):
                entries = self._version_mgr.read_binlog(dataset_name)
                if not entries:
                    return t("msg_no_ops")
                lines = []
                for i, e in enumerate(entries[-50:]):  # show latest 50
                    ts = e.get("ts", "")
                    op = e.get("op", "")
                    meta = e.get("meta", {})
                    rec = e.get("record") or {}
                    query = rec.get("query", "")[:50]
                    api_name = (rec.get("api_schema") or {}).get("name", "")[:30]
                    lines.append(f"| {ts} | {op} | {api_name} | {query} | {json.dumps(meta, ensure_ascii=False)[:60]} |")
                header = t("binlog_table_header") + "\n|---|---|---|---|---|\n"
                return header + "\n".join(lines)
            
            def _load_restore_timestamps(dataset_name):
                """Load available timestamps for the restore dropdown."""
                ds = (dataset_name or "").strip()
                if not ds:
                    return gr.update(choices=[], value=None)
                ts_list = self._version_mgr.get_timestamps(ds)
                if not ts_list:
                    return gr.update(choices=[t("msg_no_ops")], value=None)
                return gr.update(choices=ts_list, value=ts_list[0] if ts_list else None)

            def _restore_dataset(dataset_name, target_ts):
                ds = (dataset_name or "").strip()
                raw_ts = (target_ts or "").strip()
                # Extract pure timestamp (strip "[op] (source)" label suffix)
                ts = raw_ts.split("  [")[0].strip() if raw_ts else ""
                if not ds or not ts:
                    return t("msg_restore_input_required")
                
                if ds == "valid":
                    dest = self.valid_path
                elif ds == "invalid":
                    dest = self.invalid_path
                elif ds == "schema":
                    dest = self._schema_path
                else:
                    return t("msg_restore_unsupported", ds=ds)
                
                if ds == "schema":
                    # Schema restore is JSON, not JSONL
                    records = self._version_mgr.restore_to_timestamp(ds, ts)
                    if records:
                        with open(dest, "w", encoding="utf8") as f:
                            json.dump(records[-1] if records else {}, f, ensure_ascii=False, indent=2)
                        count = 1
                    else:
                        return t("msg_restore_empty")
                else:
                    count = self._version_mgr.write_restored(ds, ts, dest)
                
                if invalidate_registry_cache:
                    invalidate_registry_cache()
                return t("msg_restored", ds=ds, ts=ts, count=count, dest=dest)
            
            version_summary = gr.Markdown("Loading...")
            version_refresh_btn = gr.Button(t("btn_refresh"))
            
            gr.Markdown(t("binlog_detail_title"))
            with gr.Row():
                binlog_dataset = gr.Dropdown(label=t("dataset_name_label"), choices=["valid", "invalid", "schema", "boundary", "recorrect", "review_queue"], value="valid")
                binlog_load_btn = gr.Button(t("btn_view_log"))
            binlog_detail = gr.Markdown("")
            
            gr.Markdown(t("restore_title"))
            with gr.Row():
                restore_dataset = gr.Dropdown(label=t("dataset_name_label"), choices=["valid", "invalid", "schema"], value="valid")
                restore_ts = gr.Dropdown(label=t("restore_ts_label"), choices=[], value=None, allow_custom_value=True)
                restore_btn = gr.Button(t("btn_restore"), variant="primary")
            restore_status = gr.Markdown("")
            
            # When restore dataset changes, refresh the timestamp dropdown
            restore_dataset.change(_load_restore_timestamps, inputs=[restore_dataset], outputs=[restore_ts])
            
            # When restore dataset changes, refresh the timestamp dropdown
            restore_dataset.change(_load_restore_timestamps, inputs=[restore_dataset], outputs=[restore_ts])
            
            version_refresh_btn.click(_get_version_summary, outputs=[version_summary])
            binlog_load_btn.click(_get_binlog_detail, inputs=[binlog_dataset], outputs=[binlog_detail])
            restore_btn.click(_restore_dataset, inputs=[restore_dataset, restore_ts], outputs=[restore_status])
            demo.load(_get_version_summary, outputs=[version_summary])
            # Auto-load timestamps for the default dataset on page load
            demo.load(_load_restore_timestamps, inputs=[restore_dataset], outputs=[restore_ts])
            # Auto-load timestamps for the default dataset on page load
            demo.load(_load_restore_timestamps, inputs=[restore_dataset], outputs=[restore_ts])

        with gr.Tab(t("tab_stats")):
            def get_stats():
                invalid_count = len(self._load_invalid())
                task_count = len(self._load_review_tasks())
                
                type_counts = {}
                for tk in self._load_review_tasks():
                    tt = tk.get('task_type', 'unknown')
                    type_counts[tt] = type_counts.get(tt, 0) + 1
                
                stats_md = t("stats_overview", invalid=invalid_count, tasks=task_count) + "\n"
                for tt, count in sorted(type_counts.items()): stats_md += f"- `{tt}`: {count}\n"
                return stats_md
            
            stats_output = gr.Markdown()
            refresh_btn = gr.Button(t("btn_refresh"))
            refresh_btn.click(get_stats, outputs=[stats_output])
            demo.load(get_stats, outputs=[stats_output])

    return demo

def launch(self, **kwargs):
    """启动界面"""
    # 启动伴生 runtime HTTP API（供外部系统调用）。
    try:
        start_runtime_api_bridge(
            host="127.0.0.1",
            port=int(os.getenv("RUNTIME_API_PORT", "7862")),
            valid_path=self.valid_path,
            review_queue=self.review_queue_path,
            output_dir=os.path.dirname(self.valid_path) or ".",
            table_name=self._infer_table_name(),
            table_desc=self._generate_table_desc_from_db(self._infer_table_name()),
            recorrect_path=self.recorrect_path,
            top_k=int(os.getenv("RUNTIME_TOPK", "5")),
        )
    except Exception as e:
        print(f"[WARN] runtime api bridge启动失败: {e}")

    demo = self.create_interface()
    if demo:
        # Whitelist auth: if auth_users configured, require login
        if self._auth_users:
            def _auth_fn(username, password):
                """Simple whitelist auth: username must be in auth_users, password can be anything."""
                return username in self._auth_users
            kwargs.setdefault("auth", _auth_fn)
            kwargs.setdefault("auth_message", t("auth_message"))
            print(f"✓ 白名单校验已启用，允许用户: {self._auth_users}")
        else:
            print("ℹ️  未配置 auth_users，跳过白名单校验")
        demo.launch(**kwargs)
    else:
        print("无法创建界面，请检查gradio安装")


# 将模块级函数绑定为类方法，修复启动/回调找不到方法的问题
ReviewInterface._render_task_interface = _render_task_interface
ReviewInterface._render_runtime_correction_task = _render_runtime_correction_task
ReviewInterface._render_schema_expansion_task = _render_schema_expansion_task
ReviewInterface._approve_invalid = _approve_invalid
ReviewInterface._approve_invalid_inner = _approve_invalid_inner
ReviewInterface._skip_invalid = _skip_invalid
ReviewInterface._reject_invalid = _reject_invalid
ReviewInterface._approve_task = _approve_task
ReviewInterface._modify_task = _modify_task
ReviewInterface._reject_task = _reject_task
ReviewInterface._next_task = _next_task
ReviewInterface.create_interface = create_interface
ReviewInterface.launch = launch


def main(): 
    """独立运行审核界面""" 
    import argparse
    parser = argparse.ArgumentParser(description="NL2AutoAPI Review Interface")
    parser.add_argument("--invalid-path", default="./output/base_staff/invalid.jsonl")
    parser.add_argument("--recorrect-path", default="./output/base_staff/recorrect.jsonl")
    parser.add_argument("--review-queue", default="./output/base_staff/review_queue.jsonl")
    parser.add_argument("--valid-path", default="./output/base_staff/valid.jsonl")
    parser.add_argument("--port", type=int, default=7860)
    parser.add_argument("--share", action="store_true")

    args = parser.parse_args()

    interface = ReviewInterface(
        invalid_path=args.invalid_path,
        recorrect_path=args.recorrect_path,
        review_queue_path=args.review_queue,
        valid_path=args.valid_path,
    )

    interface.launch(server_port=args.port, share=args.share)


if __name__ == "__main__":
    main()

