"""
Schema Feedback Module — Analyze SQL modifications and suggest schema updates.

When a user modifies an API's SQL (in Dataset, Validation Review, or Review Queue),
this module:
  1. Compares old_sql vs new_sql to identify what changed.
  2. Asks LLM to classify the modification reason:
     - column_ambiguity: field description was misleading → update schema desc
     - sql_logic_error: SQL logic/conditions were wrong (WHERE, JOIN, aggregation)
     - value_mismatch: enum values or data format assumptions were wrong
  3. For column_ambiguity: generates improved column descriptions.
  4. Finds cascade-affected APIs that use the same columns.
"""

import json
import os
import re
from typing import Optional

from core.llm import call_llm_json
from core.database import execute_sql
from core.logger import get_logger

logger = get_logger()


# ---------------------------------------------------------------------------
# Modification reason classification
# ---------------------------------------------------------------------------

class ModificationReason:
    COLUMN_AMBIGUITY = "column_ambiguity"
    SQL_LOGIC_ERROR = "sql_logic_error"
    VALUE_MISMATCH = "value_mismatch"


class SchemaUpdateSuggestion:
    """Represents a pending schema column description update."""

    def __init__(
        self,
        field_name: str,
        old_desc: str,
        new_desc: str,
        reason: str,
        confidence: float = 0.0,
        source_query: str = "",
        source_old_query: str = "",
        source_old_sql: str = "",
        source_new_sql: str = "",
    ):
        self.field_name = field_name
        self.old_desc = old_desc
        self.new_desc = new_desc
        self.reason = reason
        self.confidence = confidence
        self.source_query = source_query
        self.source_old_query = source_old_query
        self.source_old_sql = source_old_sql
        self.source_new_sql = source_new_sql

    def to_dict(self) -> dict:
        return {
            "field_name": self.field_name,
            "old_desc": self.old_desc,
            "new_desc": self.new_desc,
            "reason": self.reason,
            "confidence": self.confidence,
            "source_query": self.source_query,
            "source_old_query": self.source_old_query,
            "source_old_sql": self.source_old_sql,
            "source_new_sql": self.source_new_sql,
        }

    @staticmethod
    def from_dict(d: dict) -> "SchemaUpdateSuggestion":
        return SchemaUpdateSuggestion(
            field_name=d.get("field_name", ""),
            old_desc=d.get("old_desc", ""),
            new_desc=d.get("new_desc", ""),
            reason=d.get("reason", ""),
            confidence=d.get("confidence", 0.0),
            source_query=d.get("source_query", ""),
            source_old_query=d.get("source_old_query", ""),
            source_old_sql=d.get("source_old_sql", ""),
            source_new_sql=d.get("source_new_sql", ""),
        )


def analyze_sql_modification(
    query: str,
    old_sql: str,
    new_sql: str,
    schema_json: dict,
    table_name: str,
    old_query: str = "",
) -> dict:
    """
    Analyze the difference between old_sql/old_query and new_sql/new_query,
    classify the modification reason, and suggest schema updates if applicable.

    Both SQL and query changes are considered — sometimes only the query is
    modified (e.g. user rephrases the question), which can also reveal
    column ambiguity or value misunderstanding.

    Returns:
        {
            "reason_type": "column_ambiguity" | "sql_logic_error" | "value_mismatch" | "none",
            "reason_detail": "...",
            "suggestions": [SchemaUpdateSuggestion, ...],
        }
    """
    old_sql = (old_sql or "").strip()
    new_sql = (new_sql or "").strip()
    query = (query or "").strip()
    old_query = (old_query or "").strip()

    # Normalize for comparison
    norm_old_sql = re.sub(r"\s+", " ", old_sql).strip().lower()
    norm_new_sql = re.sub(r"\s+", " ", new_sql).strip().lower()
    sql_changed = (norm_old_sql != norm_new_sql) and bool(old_sql) and bool(new_sql)

    norm_old_query = re.sub(r"\s+", " ", old_query).strip().lower()
    norm_new_query = re.sub(r"\s+", " ", query).strip().lower()
    query_changed = (norm_old_query != norm_new_query) and bool(old_query) and bool(query)

    # At least one dimension must have changed
    if not sql_changed and not query_changed:
        return {"reason_type": "none", "reason_detail": "SQL and query unchanged", "suggestions": []}

    # Get current schema field descriptions
    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    fields = table.get("fields", {})
    field_descs = {fname: finfo.get("comment", "") for fname, finfo in fields.items()}

    # Build diff description
    diff_sections = []
    if query_changed:
        diff_sections.append(f"""## 修改前 Query（用户查询）
{old_query}

## 修改后 Query（用户查询）
{query}""")
    else:
        diff_sections.append(f"""## 用户查询（未修改）
{query}""")

    if sql_changed:
        diff_sections.append(f"""## 修改前 SQL
```sql
{old_sql}
```

## 修改后 SQL
```sql
{new_sql}
```""")
    else:
        diff_sections.append(f"""## SQL（未修改）
```sql
{new_sql or old_sql}
```""")

    diff_text = "\n\n".join(diff_sections)

    # Ask LLM to classify
    prompt = f"""你是一个数据质量分析专家。用户修改了一条 API 的 SQL 和/或查询语句（Query），请分析修改原因。

## 背景信息
表名: {table_name}
当前字段描述:
{json.dumps(field_descs, ensure_ascii=False, indent=2)}

{diff_text}

## 任务
1. 对比修改前后的差异（SQL 和 Query 都要看）
2. 判断修改原因属于以下哪一类（严格选择一个）：
   - "column_ambiguity": 因字段描述模糊/歧义导致选错了字段或误用了字段。例如表中有 name_formal 和 name_display 两个字段，描述不够清晰导致混淆；或者字段含义不明确导致在 WHERE/SELECT 中用错了列。Query 改写如果是为了消除字段歧义（如从「姓名」改为「正式姓名」），也属于此类。
   - "sql_logic_error": SQL 语法或逻辑有误，如 WHERE 条件、JOIN 方式、聚合逻辑、排序、LIMIT 等错误，但涉及的字段本身没有选错。Query 改写如果是为了纠正逻辑条件（如从「最新一条」改为「最新3条」），也属于此类。
   - "value_mismatch": 字段值的格式或枚举含义理解错误。例如 status 字段实际用 '在职'/'离职' 文本，但 SQL 中用了 1/0 数值。Query 改写如果是为了纠正值的表述（如从「在职」改为「status=1」），也属于此类。
   - "none": 修改很小或无法明确归因到以上类别，不需要更新 schema。

3. 如果是 "column_ambiguity"：
   - 指出哪些字段的描述需要更新
   - 为每个需更新的字段给出改进后的描述（中文，1-2句话，要比原描述更精确地区分该字段与易混淆字段）
   - 新描述不要与原描述内容重复，要补充新的区分信息
   - 给出修改置信度（0-1），0.7以上才值得更新

4. 如果不是 "column_ambiguity" 或没有需要更新的字段，suggestions 返回空数组。

## 输出JSON格式
{{
  "reason_type": "column_ambiguity" | "sql_logic_error" | "value_mismatch" | "none",
  "reason_detail": "一句话说明修改原因",
  "suggestions": [
    {{
      "field_name": "字段名",
      "new_desc": "改进后的描述",
      "confidence": 0.85
    }}
  ]
}}
"""

    try:
        result = call_llm_json(prompt, retry=2)
    except Exception as e:
        logger.warning("[SchemaFeedback] LLM call failed: %s", e)
        return {"reason_type": "none", "reason_detail": f"LLM error: {e}", "suggestions": []}

    if not isinstance(result, dict):
        return {"reason_type": "none", "reason_detail": "LLM returned invalid JSON", "suggestions": []}

    reason_type = result.get("reason_type", "none")
    reason_detail = result.get("reason_detail", "")
    raw_suggestions = result.get("suggestions", [])

    # Validate reason_type
    valid_types = {
        ModificationReason.COLUMN_AMBIGUITY,
        ModificationReason.SQL_LOGIC_ERROR,
        ModificationReason.VALUE_MISMATCH,
        "none",
    }
    if reason_type not in valid_types:
        reason_type = "none"

    # Build SchemaUpdateSuggestion objects (only for column_ambiguity)
    suggestions = []
    if reason_type == ModificationReason.COLUMN_AMBIGUITY and isinstance(raw_suggestions, list):
        for s in raw_suggestions:
            if not isinstance(s, dict):
                continue
            field_name = s.get("field_name", "")
            new_desc = s.get("new_desc", "")
            confidence = float(s.get("confidence", 0.0))

            # Validate: field must exist in schema
            if field_name not in fields:
                continue
            # Validate: new_desc must be non-empty and different from old
            old_desc = field_descs.get(field_name, "")
            if not new_desc or new_desc.strip() == old_desc.strip():
                continue
            # Skip low-confidence suggestions
            if confidence < 0.5:
                continue

            suggestions.append(SchemaUpdateSuggestion(
                field_name=field_name,
                old_desc=old_desc,
                new_desc=new_desc.strip(),
                reason=reason_detail,
                confidence=confidence,
                source_query=query,
                source_old_query=old_query,
                source_old_sql=old_sql,
                source_new_sql=new_sql,
            ))

    return {
        "reason_type": reason_type,
        "reason_detail": reason_detail,
        "suggestions": suggestions,
    }


# ---------------------------------------------------------------------------
# Cascade detection: find APIs affected by schema column desc changes
# ---------------------------------------------------------------------------

def find_cascade_affected_apis(
    valid_path: str,
    changed_fields: list[str],
    table_name: str,
) -> list[dict]:
    """
    Find all API records in valid.jsonl whose bound_sql references any of
    the changed_fields. These APIs might need their descriptions or SQL
    updated when the schema column descriptions change.

    **Skips records with `user_edited=True`** — user-manually-edited records
    are protected and must only be changed by the user themselves.

    Returns list of {index, query, api_name, bound_sql, affected_fields, user_edited}.
    """
    if not os.path.exists(valid_path) or not changed_fields:
        return []

    affected = []
    changed_set = set(f.lower() for f in changed_fields)

    with open(valid_path, "r", encoding="utf8") as f:
        for idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            # Skip user-manually-edited records — they are protected
            if rec.get("user_edited"):
                continue

            api = rec.get("api_schema") or {}
            bound_sql = (api.get("bound_sql") or rec.get("sql") or "").lower()
            if not bound_sql:
                continue

            # Check which changed fields appear in this SQL
            matched_fields = [f for f in changed_set if re.search(rf"\b{re.escape(f)}\b", bound_sql)]
            if matched_fields:
                affected.append({
                    "index": idx,
                    "query": rec.get("query", ""),
                    "api_name": api.get("name", ""),
                    "bound_sql": api.get("bound_sql") or rec.get("sql", ""),
                    "api_desc": api.get("description", ""),
                    "affected_fields": matched_fields,
                })

    return affected


def generate_cascade_updates(
    affected_apis: list[dict],
    field_updates: dict,  # {field_name: new_desc}
    table_name: str,
) -> list[dict]:
    """
    For each affected API, ask LLM whether its description, SQL, or query
    needs updating given the new field descriptions.

    This replaces the old approach of "just update description, leave SQL alone".
    Now the LLM will judge all three dimensions:
      - description: does the API description reflect the new field meaning?
      - bound_sql: does the SQL use the correct field given the clarified desc?
      - query: does the query text need adjusting to match the corrected field?

    Returns list of {index, api_name, needs_update, update_fields,
                     new_api_desc, new_bound_sql, new_query, reason}.
    """
    if not affected_apis or not field_updates:
        return []

    results = []
    for api_info in affected_apis:
        prompt = f"""你是一个 API 数据质量审核助手。某些数据库字段的描述刚被更新（通常是为了消除歧义），请判断这个 API 的描述、SQL、查询是否需要同步更新。

## 字段描述更新（旧 → 新）
{json.dumps(field_updates, ensure_ascii=False, indent=2)}

## 当前 API 信息
- API 名称: {api_info['api_name']}
- 用户查询: {api_info['query']}
- API 描述: {api_info['api_desc']}
- SQL: {api_info['bound_sql']}
- 涉及的已更新字段: {api_info['affected_fields']}

## 任务
请判断以下三个维度是否需要更新（每个维度独立判断）：

1. **description（API 描述）**: 描述中是否包含了旧的字段含义？如果是，需要更新为更精确的表述。
2. **bound_sql（SQL 语句）**: 根据字段描述的澄清，SQL 中使用的字段是否正确？
   - 如果字段描述澄清后发现 SQL 中应该用另一个字段才对，则需要修改 SQL
   - 如果 SQL 中的字段使用没问题，则**不要修改**
   - 注意保持 `:slot_xxx` 参数化格式不变
3. **query（用户查询）**: 查询文本是否因字段含义变化需要调整措辞？
   - 通常不需要修改，除非查询中直接引用了被混淆的字段名

⚠️ 重要：只有确实需要更新时才设为 true。如果某个维度不需要改，对应值设为空字符串即可。

输出JSON:
{{
  "needs_update": true/false,
  "update_desc": true/false,
  "update_sql": true/false,
  "update_query": true/false,
  "new_api_desc": "更新后的描述（不需要更新则为空）",
  "new_bound_sql": "更新后的SQL（不需要更新则为空，保持:slot参数化）",
  "new_query": "更新后的查询（不需要更新则为空）",
  "reason": "一句话说明更新原因"
}}
"""
        try:
            result = call_llm_json(prompt, retry=1)
            if isinstance(result, dict):
                # Collect which fields will actually be updated
                update_fields = []
                if result.get("update_desc") and result.get("new_api_desc", "").strip():
                    update_fields.append("description")
                if result.get("update_sql") and result.get("new_bound_sql", "").strip():
                    update_fields.append("bound_sql")
                if result.get("update_query") and result.get("new_query", "").strip():
                    update_fields.append("query")

                results.append({
                    "index": api_info["index"],
                    "api_name": api_info["api_name"],
                    "query": api_info["query"],
                    "needs_update": result.get("needs_update", False) and len(update_fields) > 0,
                    "update_fields": update_fields,
                    "new_api_desc": result.get("new_api_desc", "").strip(),
                    "new_bound_sql": result.get("new_bound_sql", "").strip(),
                    "new_query": result.get("new_query", "").strip(),
                    "reason": result.get("reason", ""),
                })
        except Exception as e:
            logger.warning("[CascadeUpdate] LLM failed for API %s: %s", api_info["api_name"], e)

    return [r for r in results if r.get("needs_update")]
