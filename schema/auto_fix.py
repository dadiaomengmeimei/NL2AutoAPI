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
- 查询要包含这些字段
- 查询要有明确的统计或筛选目的
- 查询要简单组合，不涉及深度关联和复杂计算
- 要求是带具体值的查询，而不是模糊或笼统描述
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

    prompt = f"""你是一个数据验证专家。
用户查询: {user_query}
生成的 SQL: {sql}
当前表字段描述: {json.dumps(short_descs, ensure_ascii=False)}

请诊断类型，仅从以下枚举中选择，并简要说明原因。
    - "CORRECT": 完全正确，SQL 正确实现了用户查询意图
    - "PARTIAL": 部分正确（如字段匹配但逻辑不完整）
    - "INCORRECT": 用户查询涉及的内容超出当前表的能力范围、使用了错误的字段或者SQL和用户意图不关联

输出 JSON 格式:
{{
    "reason": "原因",
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
) -> dict:
    """
    Parse fields out of a SQL, then refine the description for each
    field that exists in the schema.
    Returns the mutated schema_json.
    """
    fields_to_fix = extract_fields_from_sql(sql)
    logger.info("[Auto-Fix] SQL extracted fields: %s", fields_to_fix)

    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    field_defs = table.get("fields", {})

    for f in fields_to_fix:
        # Skip table name itself
        if f.lower() == table_name.lower():
            continue
        if f not in field_defs:
            continue

        logger.info("[Auto-Fix] Refining field: %s", f)
        new_desc = _refine_field_desc(table_name, f, schema_json, error_contexts)
        field_defs[f]["comment"] = new_desc
        logger.info("[Auto-Fix] Field [%s] => %s", f, new_desc)

    return schema_json


# ---------------------------------------------------------------------------
# Exploration loop (core cycle)
# ---------------------------------------------------------------------------

def _exploration_loop(
    table_name: str,
    schema_json: dict,
    field_usage_count: dict[str, int],
    num_queries: int = 8,
    progress_callback: Optional[Callable] = None,
) -> tuple[dict, list[str]]:
    """
    One round of the exploration loop:
      1. Generate NL queries (biased towards high-usage fields)
      2. For each query: generate SQL → execute → validate
      3. If INCORRECT/PARTIAL: extract SQL fields → targeted desc fix
    Returns (mutated schema_json, progress_log_lines).
    """
    progress_log: list[str] = []
    queries = _generate_queries_from_fields(
        table_name, schema_json, field_usage_count, num_queries=num_queries,
    )
    logger.info("[Auto-Fix] Generated %d exploration queries", len(queries))
    progress_log.append(f"生成了 {len(queries)} 条探索查询")

    total = len(queries)
    for q_idx, query in enumerate(queries):
        prefix = f"[{q_idx+1}/{total}]"
        if progress_callback:
            progress_callback(q_idx, total, query[:40])

        error_contexts: list[dict] = []
        error_info: Optional[dict] = None
        final_status = "SKIP"

        for attempt in range(2):
            # Generate SQL
            sql = _generate_sql_from_query(query, table_name, schema_json, error_info)
            if not sql:
                logger.warning("[Auto-Fix] SQL generation returned None for: %s", query[:50])
                progress_log.append(f"{prefix} ❌ SQL生成失败: {query[:60]}")
                final_status = "GEN_FAIL"
                break

            # Execute SQL
            exec_result = execute_sql(None, sql)
            if exec_result.get("status") != "success":
                err_msg = exec_result.get("error", "SQL execution failed")
                error_info = {
                    "intent": query,
                    "sql": sql,
                    "reason": err_msg,
                    "type": "INCORRECT",
                }
                error_contexts.append(error_info)
                progress_log.append(f"{prefix} ⚠️ SQL执行失败(attempt {attempt+1}): {err_msg[:80]}")
                schema_json = _fix_fields_from_sql(sql, table_name, schema_json, error_contexts)
                final_status = "EXEC_FAIL"
                continue

            # Validate with LLM
            v_type, v_reason = _validate_with_llm(query, sql, schema_json, table_name)
            error_info = {"intent": query, "sql": sql, "reason": v_reason, "type": v_type}
            error_contexts.append(error_info)

            if v_type == "CORRECT":
                logger.info("[Auto-Fix] Query OK: %s", query[:50])
                progress_log.append(f"{prefix} ✅ CORRECT: {query[:60]}")
                final_status = "CORRECT"
                break
            else:
                logger.info("[Auto-Fix] %s for: %s — fixing fields", v_type, query[:50])
                fixed_fields = extract_fields_from_sql(sql)
                progress_log.append(f"{prefix} 🔧 {v_type}(attempt {attempt+1}): {query[:50]} → 修正字段: {fixed_fields}")
                schema_json = _fix_fields_from_sql(sql, table_name, schema_json, error_contexts)
                final_status = v_type

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

    Returns:
        The updated schema_json (also mutated in place).
    """
    all_progress: list[str] = []

    # Phase 0: auto-prune obviously useless fields before exploration
    pruned, schema_json = auto_prune_useless_fields(table_name, schema_json)
    if pruned:
        logger.info("[Auto-Fix] Auto-pruned %d useless fields: %s", len(pruned), pruned)
        all_progress.append(f"🗑️ 自动裁剪了 {len(pruned)} 个无用字段: {', '.join(pruned)}")
    else:
        all_progress.append("✅ 无需裁剪字段")

    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    fields = table.get("fields", {})

    # Initialize field usage counter
    field_usage_count: dict[str, int] = {f: 0 for f in fields}

    for r in range(rounds):
        logger.info("[Auto-Fix] === Round %d/%d ===", r + 1, rounds)
        all_progress.append(f"\n===== 第 {r+1}/{rounds} 轮 =====")
        schema_json, round_log = _exploration_loop(
            table_name,
            schema_json,
            field_usage_count,
            num_queries=queries_per_round,
            progress_callback=progress_callback,
        )
        all_progress.extend(round_log)

    return schema_json, all_progress


def auto_fix_single_field_in_schema(
    table_name: str,
    field_name: str,
    schema_json: dict,
    rounds: int = 3,
) -> tuple[str, dict, list[str]]:
    """
    Focus on a single field: generate queries that involve it, run
    the explore-validate-fix cycle for N rounds.

    Returns (new_description, updated_schema_json, progress_log).
    """
    progress_log: list[str] = []
    tables = schema_json.get("tables", {})
    table = tables.get(table_name, {})
    fields = table.get("fields", {})
    finfo = fields.get(field_name, {})
    current_desc = finfo.get("comment", "")

    all_descs = _get_all_field_descs(schema_json, table_name)

    for round_idx in range(rounds):
        logger.info("[Auto-Fix] Single-field round %d/%d for [%s]", round_idx + 1, rounds, field_name)
        progress_log.append(f"\n--- 第 {round_idx+1}/{rounds} 轮 (字段: {field_name}) ---")

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
            progress_log.append("❌ 查询生成失败")
            continue
        if not query:
            progress_log.append("❌ 查询生成返回空")
            continue
        progress_log.append(f"🔍 生成查询: {query[:80]}")

        error_contexts: list[dict] = []
        error_info: Optional[dict] = None

        for attempt in range(2):
            sql = _generate_sql_from_query(query, table_name, schema_json, error_info)
            if not sql:
                break

            exec_result = execute_sql(None, sql)
            if exec_result.get("status") != "success":
                err_msg = exec_result.get("error", "SQL execution failed")
                error_info = {
                    "intent": query,
                    "sql": sql,
                    "reason": err_msg,
                    "type": "INCORRECT",
                }
                error_contexts.append(error_info)
                progress_log.append(f"⚠️ SQL执行失败: {err_msg[:80]}")
                # Targeted fix
                new_desc = _refine_field_desc(table_name, field_name, schema_json, error_contexts)
                finfo["comment"] = new_desc
                progress_log.append(f"🔧 修正描述 → {new_desc[:60]}")
                continue

            v_type, v_reason = _validate_with_llm(query, sql, schema_json, table_name)
            error_info = {"intent": query, "sql": sql, "reason": v_reason, "type": v_type}
            error_contexts.append(error_info)

            if v_type == "CORRECT":
                logger.info("[Auto-Fix] Single-field query OK: %s", query[:50])
                progress_log.append(f"✅ CORRECT: {query[:60]}")
                break
            else:
                new_desc = _refine_field_desc(table_name, field_name, schema_json, error_contexts)
                finfo["comment"] = new_desc
                progress_log.append(f"🔧 {v_type}: 修正描述 → {new_desc[:60]}")

    return finfo.get("comment", current_desc), schema_json, progress_log
