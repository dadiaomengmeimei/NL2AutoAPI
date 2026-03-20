"""
Auto-fix schema field descriptions via exploration loop.

Inspired by auto_fix_schema_loop.py, this module uses an
"explore → generate SQL → execute → validate → targeted fix" cycle.

Only column descriptions are fixed; table descriptions are NOT modified.

Two public entry points:
  1) auto_fix_all_fields   — global exploration loop over N rounds
  2) auto_fix_single_field_in_schema — focus on ONE field for N rounds
"""

import json
import random
import re
from typing import Optional, Callable

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function, Parenthesis, Where
from sqlparse.tokens import Name

from core.llm import call_llm_json, call_llm
from core.database import execute_sql
from core.logger import get_logger

logger = get_logger()

# ---------------------------------------------------------------------------
# SQL field extraction (ported from reference code)
# ---------------------------------------------------------------------------

_SQL_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT", "AND", "OR",
    "AS", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CASE", "WHEN",
    "THEN", "END", "IS", "NULL", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE",
    "DISTINCT", "UNION", "ALL", "ANY", "HAVING", "COUNT", "SUM", "AVG",
    "MIN", "MAX", "ASC", "DESC",
}


def extract_fields_from_sql(sql: str) -> list[str]:
    """Extract physical column names from SQL (SELECT / WHERE / GROUP BY …)."""
    parsed = sqlparse.parse(sql)
    fields: set[str] = set()

    def _recurse(token_list):
        for token in token_list:
            if isinstance(token, Identifier):
                real = token.get_real_name()
                if real:
                    fields.add(real)
                if token.is_group:
                    _recurse(token.tokens)
            elif isinstance(token, IdentifierList):
                for ident in token.get_identifiers():
                    _recurse([ident])
            elif isinstance(token, (Function, Parenthesis)):
                _recurse(token.tokens)
            elif isinstance(token, Where):
                _recurse(token.tokens)
            elif token.is_group:
                _recurse(token.tokens)
            elif token.ttype in Name:
                fields.add(token.value)

    for stmt in parsed:
        _recurse(stmt.tokens)

    return [f for f in fields if f.upper() not in _SQL_KEYWORDS]


# ---------------------------------------------------------------------------
# Sampling helpers
# ---------------------------------------------------------------------------

def _sample_field_values(table_name: str, field_name: str, limit: int = 10) -> list:
    """Sample distinct non-null values via random ordering."""
    try:
        sql = (
            f"SELECT DISTINCT `{field_name}` FROM `{table_name}` "
            f"WHERE `{field_name}` IS NOT NULL "
            f"ORDER BY RAND() LIMIT {limit}"
        )
        res = execute_sql(None, sql)
        rows = res.get("all_rows") or []
        values = []
        for r in rows:
            if isinstance(r, (list, tuple)) and r:
                values.append(str(r[0]))
            elif isinstance(r, dict):
                values.append(str(next(iter(r.values()))))
        return values
    except Exception:
        # Fallback: plain LIMIT without random
        try:
            sql2 = f"SELECT DISTINCT `{field_name}` FROM `{table_name}` WHERE `{field_name}` IS NOT NULL LIMIT {limit}"
            res = execute_sql(None, sql2)
            rows = res.get("all_rows") or []
            return [str(r[0]) if isinstance(r, (list, tuple)) else str(next(iter(r.values()))) for r in rows if r]
        except Exception:
            return []


def _get_neighbor_fields(schema_json: dict, table_name: str, field_name: str, k: int = 5) -> list[dict]:
    """Return up to k neighboring fields with their descriptions."""
    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    fields = table.get("fields", {})
    neighbors = []
    for fname, finfo in fields.items():
        if fname == field_name:
            continue
        neighbors.append({
            "name": fname,
            "type": finfo.get("type", ""),
            "comment": finfo.get("comment", ""),
        })
    return neighbors[:k]


def _get_all_field_descs(schema_json: dict, table_name: str) -> dict[str, str]:
    """Return {field_name: comment} for all fields in a table."""
    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    fields = table.get("fields", {})
    return {fname: finfo.get("comment", "") for fname, finfo in fields.items()}


# ---------------------------------------------------------------------------
# Query generation (from field descriptions, with usage balancing)
# ---------------------------------------------------------------------------

def _generate_queries_from_fields(
    table_name: str,
    schema_json: dict,
    field_usage_count: dict[str, int],
    num_queries: int = 8,
) -> list[str]:
    """Generate NL queries biased towards high-usage (important) fields."""
    all_descs = _get_all_field_descs(schema_json, table_name)
    if not all_descs:
        return []

    # Sort fields by usage count (descending) → pick high-freq fields first.
    # For fields with 0 usage (cold start), assign a small random score so
    # they still get a chance, but high-freq fields are strongly preferred.
    sorted_fields = sorted(
        all_descs.keys(),
        key=lambda f: field_usage_count.get(f, 0) + random.random() * 0.5,
        reverse=True,
    )
    queries: list[str] = []

    for _ in range(num_queries):
        # Top-third pool for high-freq fields
        pool_size = max(1, len(sorted_fields) // 3)
        selected = random.sample(sorted_fields[:pool_size], min(2, pool_size))
        descs = {f: all_descs[f] for f in selected}
        prompt = f"""你是一个数据分析专家。根据以下字段描述，为表 {table_name} 生成一个自然语言查询。
字段描述：
{json.dumps(descs, ensure_ascii=False)}
要求：
- 查询要包含这些字段，并且要在 WHERE 条件或 GROUP BY 中使用它们
- 查询要有明确的统计或筛选目的
- 查询要简单组合，不涉及深度关联和复杂计算
- 要求是带具体值的查询（如具体的名称、类别），而不是模糊或笼统描述
- 不要使用具体的数字ID或编号，而是使用有业务含义的筛选值（如部门名称、状态描述等）
- 故意使用字段描述中的近义词或口语化表达，测试字段描述的辨识度
- 直接输出一个自然语言查询字符串，不要输出其他内容
"""
        try:
            text = call_llm(prompt).strip().strip('"').strip("'")
            if text:
                queries.append(text)
                # Update usage count for selected fields
                for f in selected:
                    field_usage_count[f] = field_usage_count.get(f, 0) + 1
        except Exception:
            pass

    return queries


# ---------------------------------------------------------------------------
# SQL generation from schema (simple LLM-based)
# ---------------------------------------------------------------------------

def _generate_sql_from_query(
    query: str,
    table_name: str,
    schema_json: dict,
    error_info: Optional[dict] = None,
) -> Optional[str]:
    """Ask LLM to produce a SQL for the given NL query using schema info."""
    all_descs = _get_all_field_descs(schema_json, table_name)
    fields_text = "\n".join(f"- {fname}: {desc}" for fname, desc in all_descs.items())

    error_hint = ""
    if error_info:
        error_hint = f"""
上一次尝试信息：
- SQL: {error_info.get('sql', '')}
- 错误原因: {error_info.get('reason', '')}
请避免上述问题，重新生成正确的SQL。
"""

    prompt = f"""你是一个SQL专家。根据以下表和字段信息，将用户的自然语言查询转换为SQL。

表名: {table_name}
字段列表:
{fields_text}

用户查询: {query}
{error_hint}
要求:
- 只使用上述字段列表中存在的字段
- 输出JSON格式: {{"sql": "SELECT ..."}}
- 不要输出其他内容
"""
    result = call_llm_json(prompt, retry=2)
    if result and result.get("sql"):
        return result["sql"]
    return None


# ---------------------------------------------------------------------------
# LLM validation (ported from reference: validate_with_llm)
# ---------------------------------------------------------------------------

def _validate_with_llm(
    user_query: str,
    sql: str,
    schema_json: dict,
    table_name: str,
) -> tuple[str, str]:
    """
    Validate SQL correctness with LLM.
    Returns (type, reason) where type is "CORRECT" | "PARTIAL" | "INCORRECT".
    """
    all_descs = _get_all_field_descs(schema_json, table_name)
    # Truncate long descriptions for prompt
    short_descs = {f: d[:50] for f, d in all_descs.items()}

    prompt = f"""你是一个严格的数据验证专家。请仔细验证 SQL 是否正确实现了用户查询意图。

用户查询: {user_query}
生成的 SQL: {sql}
当前表字段描述: {json.dumps(short_descs, ensure_ascii=False)}

请严格检查以下方面：
1. SQL中使用的字段是否正确对应了用户查询意图中的语义概念？
   - 例如：用户问"部门"，SQL是否用了正确的部门字段（而不是误用了其他字段）？
2. WHERE条件中的值是否合理？是否可能因为字段描述不清导致用了错误的筛选值？
3. SQL的SELECT、WHERE、GROUP BY等子句是否完整覆盖了用户意图？
4. 字段描述是否有歧义或不够精确，可能导致误用？

诊断类型（严格判定）：
- "CORRECT": SQL 完全正确且选用的字段无歧义
- "PARTIAL": SQL 大致正确但字段选择可能有歧义（如存在多个相似字段但不确定选了对的）
- "INCORRECT": 字段选择明显错误、逻辑错误、或查询意图超出表能力

输出 JSON 格式:
{{
    "reason": "原因，特别指出哪个字段描述可能有问题",
    "type": "以上英文枚举值"
}}
"""
    for _ in range(3):
        result = call_llm_json(prompt, retry=1)
        if result and result.get("type"):
            return result["type"].upper(), result.get("reason", "")
    return "INCORRECT", "LLM validation parse failed"


# ---------------------------------------------------------------------------
# Targeted field description refinement (with error context + sampling)
# ---------------------------------------------------------------------------

def _refine_field_desc(
    table_name: str,
    field_name: str,
    schema_json: dict,
    error_contexts: Optional[list[dict]] = None,
) -> str:
    """
    Refine a single field's description using:
      - random sampled values from DB
      - neighboring field context
      - error contexts from failed SQL attempts
    """
    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    fields = table.get("fields", {})
    finfo = fields.get(field_name, {})
    current_desc = finfo.get("comment", "")

    sample_values = _sample_field_values(table_name, field_name)
    neighbors = _get_neighbor_fields(schema_json, table_name, field_name, k=4)

    # Cross-reference samples for neighbors
    cross_samples = {}
    for nb in neighbors[:2]:
        cross_samples[nb["name"]] = _sample_field_values(table_name, nb["name"], limit=5)

    # Format error context (keep latest only)
    error_text = "【无】"
    if error_contexts:
        recent = error_contexts[-1:]
        error_text = "\n".join(
            f"* 意图: {c.get('intent')} | 报错原因: {c.get('reason')}"
            for c in recent
        )

    prompt = f"""### 任务：修正字段 [{field_name}] 的业务描述。
原描述: {current_desc or '无'}
字段类型: {finfo.get('type', '未知')}
随机采样数据: {sample_values}

邻居字段上下文:
{json.dumps(neighbors, ensure_ascii=False, indent=2)}

邻居字段采样:
{json.dumps(cross_samples, ensure_ascii=False, indent=2)}

报错上下文: {error_text}

### 要求：
1. 如果数据呈现枚举特征（如 0, 1, 2），描述应说明这是状态位或枚举值的含义。
2. 如果数据呈现数值特征且有特定模式（如 1672342342），请判断是否为时间戳。
3. 结合报错上下文修正描述，防止模型再次误用该字段。
4. 参考邻居字段理解业务语境。
5. 需要尽量简洁，1-2句话，用中文。

输出 JSON: {{"description": "修正后的内容"}}
"""
    try:
        result = call_llm_json(prompt, retry=2)
        if isinstance(result, dict) and result.get("description"):
            return result["description"]
    except Exception:
        pass
    return current_desc


def _fix_fields_from_sql(
    sql: str,
    table_name: str,
    schema_json: dict,
    error_contexts: list[dict],
) -> tuple[dict, list[dict]]:
    """
    Parse fields out of a SQL, then refine the description for each
    field that exists in the schema.
    Returns (mutated schema_json, list of change dicts).
    Each change dict: {"field": name, "old_desc": ..., "new_desc": ...}
    """
    fields_to_fix = extract_fields_from_sql(sql)
    logger.info("[Auto-Fix] SQL extracted fields: %s", fields_to_fix)

    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    field_defs = table.get("fields", {})

    changes: list[dict] = []
    for f in fields_to_fix:
        # Skip table name itself
        if f.lower() == table_name.lower():
            continue
        if f not in field_defs:
            continue

        logger.info("[Auto-Fix] Refining field: %s", f)
        old_desc = field_defs[f].get("comment", "")
        new_desc = _refine_field_desc(table_name, f, schema_json, error_contexts)
        if new_desc and new_desc != old_desc:
            field_defs[f]["comment"] = new_desc
            changes.append({"field": f, "old_desc": old_desc, "new_desc": new_desc})
            logger.info("[Auto-Fix] Field [%s]: '%s' => '%s'", f, old_desc[:40], new_desc[:40])
        else:
            logger.info("[Auto-Fix] Field [%s] unchanged", f)

    return schema_json, changes


# ---------------------------------------------------------------------------
# On-policy query regeneration (after fixing descriptions)
# ---------------------------------------------------------------------------

def _regenerate_query_on_policy(
    table_name: str,
    schema_json: dict,
    field_usage_count: dict[str, int],
    old_query: str,
    error_info: dict,
) -> str:
    """
    After fixing field descriptions, regenerate a NEW query using updated
    descriptions (on-policy). This avoids repeatedly testing the same
    broken query that uses non-existent filter values.
    """
    all_descs = _get_all_field_descs(schema_json, table_name)
    # Pick fields from the failed SQL to stay focused
    failed_fields = extract_fields_from_sql(error_info.get("sql", ""))
    target_descs = {f: all_descs[f] for f in failed_fields if f in all_descs}
    if not target_descs:
        # Fallback: pick random fields
        keys = list(all_descs.keys())
        chosen = random.sample(keys, min(2, len(keys)))
        target_descs = {f: all_descs[f] for f in chosen}

    prompt = f"""你是一个数据分析专家。根据以下最新的字段描述，为表 {table_name} 生成一个新的自然语言查询。

最新字段描述：
{json.dumps(target_descs, ensure_ascii=False)}

上一轮失败的查询（请不要重复它）: {old_query}
失败原因: {error_info.get('reason', '')[:100]}

要求：
- 生成一个全新的查询，不要复用上一轮的筛选值
- 查询要使用真实可能存在的筛选值（根据字段描述推断）
- 查询要有明确的统计或筛选目的
- 不要使用具体的数字ID或编号
- 直接输出一个自然语言查询字符串，不要输出其他内容
"""
    try:
        text = call_llm(prompt).strip().strip('"').strip("'")
        if text:
            return text
    except Exception:
        pass
    return old_query  # Fallback: return old query if generation fails


def _regenerate_single_field_query(
    table_name: str,
    field_name: str,
    schema_json: dict,
    old_query: str,
    error_info: dict,
) -> str:
    """
    On-policy query regeneration for single-field auto-fix.
    Uses updated field description to generate a new query.
    """
    all_descs = _get_all_field_descs(schema_json, table_name)
    other_fields = [f for f in all_descs if f != field_name]
    partner = random.choice(other_fields) if other_fields else None
    target_descs = {field_name: all_descs.get(field_name, "")}
    if partner:
        target_descs[partner] = all_descs.get(partner, "")

    prompt = f"""你是一个数据分析专家。根据以下最新的字段描述，为表 {table_name} 生成一个新的自然语言查询。

最新字段描述：
{json.dumps(target_descs, ensure_ascii=False)}

上一轮失败的查询（请不要重复它）: {old_query}
失败原因: {error_info.get('reason', '')[:100]}

要求：
- 查询必须包含字段 {field_name}
- 生成一个全新的查询，不要复用上一轮的筛选值
- 查询要使用真实可能存在的筛选值（根据字段描述推断）
- 查询要有明确的统计或筛选目的
- 不要使用具体的数字ID或编号
- 直接输出一个自然语言查询字符串，不要输出其他内容
"""
    try:
        text = call_llm(prompt).strip().strip('"').strip("'")
        if text:
            return text
    except Exception:
        pass
    return old_query


# ---------------------------------------------------------------------------
# Exploration loop (core cycle)
# ---------------------------------------------------------------------------

def _exploration_loop(
    table_name: str,
    schema_json: dict,
    field_usage_count: dict[str, int],
    num_queries: int = 8,
    progress_callback: Optional[Callable] = None,
    log_callback: Optional[Callable[[str], None]] = None,
) -> tuple[dict, list[str]]:
    """
    One round of the exploration loop:
      1. Generate NL queries (biased towards high-usage fields)
      2. For each query: generate SQL → execute → validate
      3. If INCORRECT/PARTIAL: extract SQL fields → targeted desc fix
    Returns (mutated schema_json, progress_log_lines).
    """
    def _log(msg: str):
        progress_log.append(msg)
        if log_callback:
            log_callback(msg)

    progress_log: list[str] = []
    queries = _generate_queries_from_fields(
        table_name, schema_json, field_usage_count, num_queries=num_queries,
    )
    logger.info("[Auto-Fix] Generated %d exploration queries", len(queries))
    _log(f"生成了 {len(queries)} 条探索查询")

    total = len(queries)
    for q_idx, query in enumerate(queries):
        prefix = f"[{q_idx+1}/{total}]"
        if progress_callback:
            progress_callback(q_idx, total, query[:40])

        error_contexts: list[dict] = []
        error_info: Optional[dict] = None
        final_status = "SKIP"
        current_query = query  # on-policy: may regenerate after fix

        try:
            for attempt in range(2):
                # Generate SQL (using latest schema descriptions)
                sql = _generate_sql_from_query(current_query, table_name, schema_json, error_info)
                if not sql:
                    logger.warning("[Auto-Fix] SQL generation returned None for: %s", current_query[:50])
                    _log(f"{prefix} ❌ SQL生成失败: {current_query[:60]}")
                    final_status = "GEN_FAIL"
                    break

                # Execute SQL
                exec_result = execute_sql(None, sql)
                if exec_result.get("status") != "success":
                    err_msg = exec_result.get("error", "SQL execution failed")
                    error_info = {
                        "intent": current_query,
                        "sql": sql,
                        "reason": err_msg,
                        "type": "EXEC_FAIL",
                    }
                    error_contexts.append(error_info)
                    _log(f"{prefix} ⚠️ SQL执行失败(attempt {attempt+1}): {err_msg[:80]}")

                    # Distinguish hallucinated column names vs pure syntax errors.
                    # If error mentions unknown column / field, it's likely caused
                    # by ambiguous descriptions → fix descriptions.
                    # Otherwise it's a pure SQL syntax problem → just retry SQL.
                    err_lower = err_msg.lower()
                    is_column_error = any(kw in err_lower for kw in [
                        "unknown column", "no such column", "doesn't exist",
                        "field list", "unknown field", "column not found",
                        "no column", "不存在", "未知列",
                    ])
                    if is_column_error:
                        _log(f"{prefix} 🔧 幻觉字段名，修正相关字段描述")
                        schema_json, changes = _fix_fields_from_sql(sql, table_name, schema_json, error_contexts)
                        for ch in changes:
                            _log(f"{prefix}   📝 [{ch['field']}]: \"{ch['old_desc'][:40]}\" → \"{ch['new_desc'][:40]}\"")
                        current_query = _regenerate_query_on_policy(
                            table_name, schema_json, field_usage_count, current_query, error_info
                        )
                        _log(f"{prefix} 🔄 重新生成查询: {current_query[:60]}")
                    else:
                        _log(f"{prefix} 🔄 SQL语法问题，将错误信息传递给下一次SQL生成")
                    final_status = "EXEC_FAIL"
                    continue

                # Additional check: if SQL returns 0 rows, likely the query
                # used invalid filter values (not a description problem).
                row_count = exec_result.get("row_count", 0)
                if row_count == 0:
                    logger.info("[Auto-Fix] SQL returned 0 rows for: %s", current_query[:50])
                    error_info = {
                        "intent": current_query,
                        "sql": sql,
                        "reason": "SQL executed successfully but returned 0 rows — the query may use non-existent filter values, or field descriptions have wrong enum values",
                        "type": "PARTIAL",
                    }
                    error_contexts.append(error_info)
                    _log(f"{prefix} ⚠️ 0行结果(attempt {attempt+1}): {current_query[:50]}")
                    # Fix descriptions AND regenerate query (on-policy)
                    schema_json, changes = _fix_fields_from_sql(sql, table_name, schema_json, error_contexts)
                    for ch in changes:
                        _log(f"{prefix}   📝 [{ch['field']}]: \"{ch['old_desc'][:40]}\" → \"{ch['new_desc'][:40]}\"")
                    current_query = _regenerate_query_on_policy(
                        table_name, schema_json, field_usage_count, current_query, error_info
                    )
                    _log(f"{prefix} 🔄 重新生成查询: {current_query[:60]}")
                    final_status = "ZERO_ROWS"
                    continue

                # Validate with LLM
                v_type, v_reason = _validate_with_llm(current_query, sql, schema_json, table_name)
                error_info = {"intent": current_query, "sql": sql, "reason": v_reason, "type": v_type}
                error_contexts.append(error_info)

                if v_type == "CORRECT":
                    logger.info("[Auto-Fix] Query OK: %s", current_query[:50])
                    _log(f"{prefix} ✅ CORRECT: {current_query[:60]}")
                    final_status = "CORRECT"
                    break
                else:
                    logger.info("[Auto-Fix] %s for: %s — fixing fields", v_type, current_query[:50])
                    fixed_fields = extract_fields_from_sql(sql)
                    _log(f"{prefix} 🔧 {v_type}(attempt {attempt+1}): {current_query[:50]} → 修正字段: {fixed_fields}")
                    schema_json, changes = _fix_fields_from_sql(sql, table_name, schema_json, error_contexts)
                    for ch in changes:
                        _log(f"{prefix}   📝 [{ch['field']}]: \"{ch['old_desc'][:40]}\" → \"{ch['new_desc'][:40]}\"")
                    # On-policy: regenerate query with updated descriptions
                    current_query = _regenerate_query_on_policy(
                        table_name, schema_json, field_usage_count, current_query, error_info
                    )
                    _log(f"{prefix} 🔄 重新生成查询: {current_query[:60]}")
                    final_status = v_type
        except Exception as e:
            logger.warning("[Auto-Fix] Query processing failed (skipping): %s — %s", current_query[:50], str(e)[:100])
            _log(f"{prefix} ⏭️ 跳过(异常): {str(e)[:80]}")

    return schema_json, progress_log


# ---------------------------------------------------------------------------
# Auto-prune useless fields
# ---------------------------------------------------------------------------

_COMMON_INTERNAL_PATTERNS = [
    r"^(created?|updated?|modified|deleted)[_]?(at|time|date|on|by)$",
    r"^(gmt_|gmt)(create|modified|updated)",
    r"^(is_deleted|is_removed|is_archived)$",
    r"^(row_id|_id|__id)$",
    r"^(etl_|dwid_|dw_)",
]


def _is_likely_internal_field(field_name: str) -> bool:
    """Quick heuristic check for common internal/meta fields."""
    fn = field_name.strip().lower()
    for pat in _COMMON_INTERNAL_PATTERNS:
        if re.match(pat, fn):
            return True
    return False


def auto_prune_useless_fields(
    table_name: str,
    schema_json: dict,
    dry_run: bool = False,
) -> tuple[list[str], dict]:
    """
    Ask LLM to identify fields that would never appear in user queries
    (e.g. internal timestamps, ETL markers, row IDs) and remove them.

    Args:
        table_name: target table
        schema_json: full schema dict
        dry_run: if True, return the list but don't mutate schema_json

    Returns:
        (list_of_pruned_field_names, updated_schema_json)
    """
    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    fields = table.get("fields", {})
    if not fields:
        return [], schema_json

    field_summaries = []
    for fname, finfo in fields.items():
        field_summaries.append({
            "name": fname,
            "type": finfo.get("type", ""),
            "comment": finfo.get("comment", ""),
            "heuristic_internal": _is_likely_internal_field(fname),
        })

    prompt = f"""你是一个数据建模专家。以下是表 [{table_name}] 的所有字段，请判断哪些字段是"内部管理字段"，即用户在自然语言查询中永远不会涉及的字段。

常见的内部管理字段包括：
- 表记录的创建时间、更新时间、删除标记（如 created_at, updated_at, is_deleted）
- ETL 流程字段（如 etl_time, dwid_xxx）
- 内部行ID（如 row_id, _id）
- 数据仓库标记字段

注意：以下类型的字段应该保留，不要删除：
- 有业务含义的时间字段（如"入职日期"、"生日"、"合同到期日"）
- 有业务含义的状态字段（如"在职状态"、"审批状态"）
- 任何用户可能在查询中提到的字段

字段列表：
{json.dumps(field_summaries, ensure_ascii=False, indent=2)}

请输出 JSON 格式：
{{"fields_to_remove": ["field_name_1", "field_name_2", ...], "reason": "简要说明"}}

如果没有需要删除的字段，返回：{{"fields_to_remove": [], "reason": "所有字段都有业务价值"}}
"""
    try:
        result = call_llm_json(prompt, retry=2)
        if not isinstance(result, dict):
            return [], schema_json
        to_remove = result.get("fields_to_remove", [])
        reason = result.get("reason", "")
        if not to_remove:
            logger.info("[Auto-Prune] No useless fields detected: %s", reason)
            return [], schema_json

        # Validate: only remove fields that actually exist
        to_remove = [f for f in to_remove if f in fields]
        if not to_remove:
            return [], schema_json

        logger.info("[Auto-Prune] Fields to remove: %s (reason: %s)", to_remove, reason)

        if not dry_run:
            for f in to_remove:
                del fields[f]
                logger.info("[Auto-Prune] Removed field: %s", f)

        return to_remove, schema_json
    except Exception as e:
        logger.warning("[Auto-Prune] LLM call failed: %s", e)
        return [], schema_json


# ===========================================================================
# Public API
# ===========================================================================

def auto_fix_all_fields(
    table_name: str,
    schema_json: dict,
    rounds: int = 1,
    queries_per_round: int = 8,
    progress_callback: Optional[Callable] = None,
    log_callback: Optional[Callable[[str], None]] = None,
) -> dict:
    """
    Global auto-fix: run exploration loop for *rounds* iterations.

    Each round generates queries, executes SQL, validates, and fixes
    the field descriptions that caused errors.

    Args:
        table_name: target table
        schema_json: full schema dict (mutated in place)
        rounds: number of exploration iterations
        queries_per_round: how many queries to generate per round
        progress_callback: optional (current, total, label)
        log_callback: optional callback(msg) called on every new log line

    Returns:
        The updated schema_json (also mutated in place).
    """
    all_progress: list[str] = []

    def _log(msg: str):
        all_progress.append(msg)
        if log_callback:
            log_callback(msg)

    # Phase 0: auto-prune obviously useless fields before exploration
    pruned, schema_json = auto_prune_useless_fields(table_name, schema_json)
    if pruned:
        logger.info("[Auto-Fix] Auto-pruned %d useless fields: %s", len(pruned), pruned)
        _log(f"🗑️ 自动裁剪了 {len(pruned)} 个无用字段: {', '.join(pruned)}")
    else:
        _log("✅ 无需裁剪字段")

    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    fields = table.get("fields", {})

    # Initialize field usage counter
    field_usage_count: dict[str, int] = {f: 0 for f in fields}

    for r in range(rounds):
        logger.info("[Auto-Fix] === Round %d/%d ===", r + 1, rounds)
        _log(f"\n===== 第 {r+1}/{rounds} 轮 =====")
        schema_json, round_log = _exploration_loop(
            table_name,
            schema_json,
            field_usage_count,
            num_queries=queries_per_round,
            progress_callback=progress_callback,
            log_callback=log_callback,
        )
        all_progress.extend(round_log)

    return schema_json, all_progress


def auto_fix_single_field_in_schema(
    table_name: str,
    field_name: str,
    schema_json: dict,
    rounds: int = 3,
    log_callback: Optional[Callable[[str], None]] = None,
) -> tuple[str, dict, list[str]]:
    """
    Focus on a single field: generate queries that involve it, run
    the explore-validate-fix cycle for N rounds.

    Args:
        log_callback: optional callback(msg) called on every new log line

    Returns (new_description, updated_schema_json, progress_log).
    """
    progress_log: list[str] = []

    def _log(msg: str):
        progress_log.append(msg)
        if log_callback:
            log_callback(msg)
    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    fields = table.get("fields", {})
    finfo = fields.get(field_name, {})
    current_desc = finfo.get("comment", "")

    all_descs = _get_all_field_descs(schema_json, table_name)

    for round_idx in range(rounds):
        logger.info("[Auto-Fix] Single-field round %d/%d for [%s]", round_idx + 1, rounds, field_name)
        _log(f"\n--- 第 {round_idx+1}/{rounds} 轮 (字段: {field_name}) ---")

        # Pick 1-2 random neighbors to form queries with this field
        other_fields = [f for f in all_descs if f != field_name]
        partner = random.choice(other_fields) if other_fields else None
        target_descs = {field_name: all_descs.get(field_name, "")}
        if partner:
            target_descs[partner] = all_descs.get(partner, "")

        # Generate one targeted query
        prompt = f"""你是一个数据分析专家。根据以下字段描述，为表 {table_name} 生成一个自然语言查询。
字段描述：
{json.dumps(target_descs, ensure_ascii=False)}
要求：
- 查询必须包含字段 {field_name}
- 查询要有明确的统计或筛选目的
- 要带具体值，不要模糊描述
- 直接输出一个自然语言查询字符串
"""
        try:
            query = call_llm(prompt).strip().strip('"').strip("'")
        except Exception:
            _log("❌ 查询生成失败")
            continue
        if not query:
            _log("❌ 查询生成返回空")
            continue
        _log(f"🔍 生成查询: {query[:80]}")

        error_contexts: list[dict] = []
        error_info: Optional[dict] = None
        current_query = query  # on-policy: may regenerate after fix

        try:
            for attempt in range(2):
                sql = _generate_sql_from_query(current_query, table_name, schema_json, error_info)
                if not sql:
                    _log(f"❌ SQL生成失败: {current_query[:60]}")
                    break

                exec_result = execute_sql(None, sql)
                if exec_result.get("status") != "success":
                    err_msg = exec_result.get("error", "SQL execution failed")
                    error_info = {
                        "intent": current_query,
                        "sql": sql,
                        "reason": err_msg,
                        "type": "EXEC_FAIL",
                    }
                    error_contexts.append(error_info)
                    _log(f"⚠️ SQL执行失败(attempt {attempt+1}): {err_msg[:80]}")

                    # Distinguish hallucinated column names vs pure syntax errors
                    err_lower = err_msg.lower()
                    is_column_error = any(kw in err_lower for kw in [
                        "unknown column", "no such column", "doesn't exist",
                        "field list", "unknown field", "column not found",
                        "no column", "不存在", "未知列",
                    ])
                    if is_column_error:
                        _log(f"🔧 幻觉字段名，修正字段描述")
                        old_desc = finfo.get("comment", "")
                        new_desc = _refine_field_desc(table_name, field_name, schema_json, error_contexts)
                        if new_desc and new_desc != old_desc:
                            finfo["comment"] = new_desc
                            _log(f"  📝 [{field_name}]: \"{old_desc[:40]}\" → \"{new_desc[:40]}\"")
                        current_query = _regenerate_single_field_query(
                            table_name, field_name, schema_json, current_query, error_info
                        )
                        _log(f"🔄 重新生成查询: {current_query[:60]}")
                    else:
                        _log(f"🔄 SQL语法问题，将错误信息传递给下一次SQL生成")
                    continue

                # Check for 0-row result — likely the query used non-existent values
                row_count = exec_result.get("row_count", 0)
                if row_count == 0:
                    error_info = {
                        "intent": current_query,
                        "sql": sql,
                        "reason": "SQL executed OK but returned 0 rows — query may use non-existent filter values or field description has wrong examples",
                        "type": "PARTIAL",
                    }
                    error_contexts.append(error_info)
                    _log(f"⚠️ 0行结果: {current_query[:50]} → 可能使用了不存在的筛选值")
                    old_desc = finfo.get("comment", "")
                    new_desc = _refine_field_desc(table_name, field_name, schema_json, error_contexts)
                    if new_desc and new_desc != old_desc:
                        finfo["comment"] = new_desc
                        _log(f"🔧 修正描述:")
                        _log(f"  📝 [{field_name}]: \"{old_desc[:40]}\" → \"{new_desc[:40]}\"")
                    else:
                        _log(f"🔧 描述未变化，跳过")
                    # On-policy: regenerate query with updated description
                    current_query = _regenerate_single_field_query(
                        table_name, field_name, schema_json, current_query, error_info
                    )
                    _log(f"🔄 重新生成查询: {current_query[:60]}")
                    continue

                v_type, v_reason = _validate_with_llm(current_query, sql, schema_json, table_name)
                error_info = {"intent": current_query, "sql": sql, "reason": v_reason, "type": v_type}
                error_contexts.append(error_info)

                if v_type == "CORRECT":
                    logger.info("[Auto-Fix] Single-field query OK: %s", current_query[:50])
                    _log(f"✅ CORRECT: {current_query[:60]}")
                    break
                else:
                    old_desc = finfo.get("comment", "")
                    new_desc = _refine_field_desc(table_name, field_name, schema_json, error_contexts)
                    if new_desc and new_desc != old_desc:
                        finfo["comment"] = new_desc
                        _log(f"🔧 {v_type}: 修正描述:")
                        _log(f"  📝 [{field_name}]: \"{old_desc[:40]}\" → \"{new_desc[:40]}\"")
                    else:
                        _log(f"🔧 {v_type}: 描述未变化，跳过")
                    # On-policy: regenerate query with updated description
                    current_query = _regenerate_single_field_query(
                        table_name, field_name, schema_json, current_query, error_info
                    )
                    _log(f"🔄 重新生成查询: {current_query[:60]}")
        except Exception as e:
            logger.warning("[Auto-Fix] Single-field query failed (skipping): %s — %s", current_query[:50], str(e)[:100])
            _log(f"⏭️ 跳过(异常): {str(e)[:80]}")

    return finfo.get("comment", current_desc), schema_json, progress_log
