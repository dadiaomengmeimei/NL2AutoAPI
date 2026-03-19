"""
基于规则的生成（数据驱动）

从原代码迁移的 run_advanced_rule_pipeline
"""

import random
import re
from collections import defaultdict
from typing import Optional

from core.llm import call_llm_json
from core.utils import save_jsonl, save_jsonl_dedup_sql, extract_slots, _normalize_query_semantic, load_jsonl
from core.database import execute_sql
from generation.sql_column_corrector import correct_sql_columns


def _normalize_text_token(text: str) -> str:
    text = (text or "").lower().strip()
    text = re.sub(r"[\s\-_]+", "", text)
    text = text.replace("descr", "")
    text = text.replace("description", "")
    text = text.replace("comment", "")
    return text


def _sample_signature(samples) -> tuple[str, ...]:
    if not isinstance(samples, list):
        return ()
    vals = [_normalize_text_token(str(s)) for s in samples if str(s).strip()]
    vals = [v for v in vals if v]
    if len(vals) < 2:
        return ()
    return tuple(sorted(vals[:3]))


def _query_has_constraint_hint(query: str) -> bool:
    q = query or ""
    hints = [
        "指定", "某", "某个", "某类", "某位", "该", "这个", "这位", "哪位",
        "邮箱", "员工id", "id", "部门", "业务", "地区", "地点", "状态", "类型",
        "岗位", "职级", "姓名", "名字", "路径", "公司邮箱", "编号", "条件"
    ]
    return any(h in q for h in hints)


def _query_looks_like_total_count(query: str) -> bool:
    q = query or ""
    patterns = ["一共有多少", "总共有多少", "多少员工", "总人数", "整体的人数", "公司现在有多少"]
    return any(p in q for p in patterns)


def _semantic_alignment_check(query: str, sql: str, query_type: str) -> tuple[bool, str]:
    q = (query or "").strip()
    sql_norm = f" {(sql or '').lower()} "

    has_where_slot = " where " in sql_norm and ":" in sql_norm
    has_group_by = " group by " in sql_norm
    has_count = "count(" in sql_norm
    exact_equal_lookup = "select *" in sql_norm and has_where_slot and " like " not in sql_norm

    if has_where_slot and has_count and not has_group_by:
        if _query_looks_like_total_count(q) and not _query_has_constraint_hint(q):
            return False, "问句像全量统计但SQL带筛选"

    if has_group_by and _query_looks_like_total_count(q):
        return False, "问句像总人数但SQL是分组统计"

    if exact_equal_lookup or query_type == "exact_query":
        fuzzy_markers = ["开头", "有哪些", "都有哪些", "名单", "列表", "怎么叫", "昵称", "写法"]
        if any(m in q for m in fuzzy_markers):
            return False, "问句像模糊列表但SQL是精确匹配"

    return True, ""


def _is_numeric_type(col_type: str) -> bool:
    t = str(col_type or "").upper()
    return any(x in t for x in ["INT", "FLOAT", "DECIMAL", "DOUBLE", "BIGINT"])


def _is_date_like(col_name: str, col_type: str) -> bool:
    name = (col_name or "").lower()
    t = str(col_type or "").upper()
    return "DATE" in t or name.endswith("_dt") or "date" in name or name.endswith("_time")


def _is_identity_lookup_column(col_name: str, comment: str) -> bool:
    text = f"{col_name} {comment}".lower()
    keywords = ["emplid", "员工id", "邮箱", "email", "mail", "phone", "手机号", "工号"]
    return any(k in text for k in keywords)


def _field_priority(col_name: str, comment: str) -> int:
    text = f"{col_name} {comment}".lower()
    ordered_keywords = [
        ["hr_status", "状态"],
        ["business_unit", "业务单位"],
        ["t_business", "业务板块", "业务线"],
        ["dept", "部门"],
        ["location", "地点", "城市"],
        ["empl_class", "员工类别"],
        ["mgr", "管理", "职级"],
        ["company", "公司"],
    ]
    for idx, group in enumerate(ordered_keywords):
        if any(k in text for k in group):
            return idx
    return len(ordered_keywords) + 1


def _pick_display_fields(table_info: dict) -> list[str]:
    fields = table_info.get("fields", {})
    if isinstance(fields, list):
        field_names = [f.get("name") for f in fields if isinstance(f, dict) and f.get("name")]
    else:
        field_names = list(fields.keys())
    preferred = [
        "emplid", "name_formal", "name_display", "dept_descr", "t_email_busn",
        "business_unit", "location_descr", "jobcode_descr"
    ]
    selected = [f for f in preferred if f in field_names]
    if not selected:
        selected = field_names[:3]
    return selected[:3]


def _select_group_distribution_cols(profiles: dict, limit: int = 5) -> list[str]:
    candidates = []
    for col_name, p in profiles.items():
        if 1 < p.get("d_cnt", 0) <= 12:
            candidates.append((
                _field_priority(col_name, p.get("comment", "")),
                p.get("d_cnt", 999),
                col_name,
            ))
    candidates.sort()
    return [col_name for _, _, col_name in candidates[:limit]]


def _select_filter_dims(profiles: dict, limit: int = 4) -> list[tuple[str, dict]]:
    candidates = []
    for col_name, p in profiles.items():
        d_cnt = p.get("d_cnt", 0)
        if 1 < d_cnt <= 100 and not _is_numeric_type(p.get("type", "")):
            candidates.append((
                _field_priority(col_name, p.get("comment", "")),
                d_cnt,
                col_name,
                p,
            ))
    candidates.sort()
    return [(col_name, p) for _, _, col_name, p in candidates[:limit]]


def _build_richer_bootstrap_proposals(table_name: str, table_info: dict, profiles: dict) -> list[dict]:
    proposals: list[dict] = []
    selected_fields = _pick_display_fields(table_info)
    filter_dims = _select_filter_dims(profiles, limit=4)
    date_cols = [(col_name, p) for col_name, p in profiles.items() if _is_date_like(col_name, p.get("type", ""))]

    if selected_fields:
        field_sql = ", ".join(f"`{f}`" for f in selected_fields)
        proposals.append({
            "query_type": "list_no_filter",
            "col_name": selected_fields[0],
            "col_comment": "基础员工信息",
            "samples": [],
            "sql": f"SELECT {field_sql} FROM `{table_name}` LIMIT 100",
            "slots": [],
            "human_query": "能给我看一下当前员工名单的基础信息吗？",
            "api_description": "查看员工基础信息列表",
            "layer_tag": "Layer-A",
        })

    for col_name, p in filter_dims[:3]:
        slot_name = f"slot_{col_name}"
        proposals.append({
            "query_type": "aggregate_with_filter",
            "col_name": col_name,
            "col_comment": p.get("comment") or col_name,
            "samples": p.get("samples", []),
            "sql": f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col_name}` = :{slot_name}",
            "slots": [col_name],
            "human_query": f"我想知道指定{p.get('comment') or col_name}下有多少名员工。",
            "api_description": f"统计指定{p.get('comment') or col_name}下的员工总数",
            "layer_tag": "Layer-A",
        })

        if selected_fields:
            field_sql = ", ".join(f"`{f}`" for f in selected_fields)
            proposals.append({
                "query_type": "list_with_filter",
                "col_name": col_name,
                "col_comment": p.get("comment") or col_name,
                "samples": p.get("samples", []),
                "sql": f"SELECT {field_sql} FROM `{table_name}` WHERE `{col_name}` = :{slot_name} LIMIT 100",
                "slots": [col_name],
                "human_query": f"帮我列一下指定{p.get('comment') or col_name}下的员工名单。",
                "api_description": f"查看指定{p.get('comment') or col_name}下的员工列表",
                "layer_tag": "Layer-A",
            })

    if len(filter_dims) >= 2:
        (col1, p1), (col2, p2) = filter_dims[:2]
        proposals.append({
            "query_type": "aggregate_with_multi_filter",
            "col_name": f"{col1}_{col2}",
            "col_comment": f"{p1.get('comment') or col1}+{p2.get('comment') or col2}",
            "samples": (p1.get("samples", []) or []) + (p2.get("samples", []) or []),
            "sql": f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col1}` = :slot_{col1} AND `{col2}` = :slot_{col2}",
            "slots": [col1, col2],
            "human_query": f"我想知道指定{p1.get('comment') or col1}、指定{p2.get('comment') or col2}下共有多少员工。",
            "api_description": f"统计指定{p1.get('comment') or col1}与{p2.get('comment') or col2}组合条件下的员工数",
            "layer_tag": "Layer-A",
        })

        proposals.append({
            "query_type": "group_aggregate_with_filter",
            "col_name": col2,
            "col_comment": p2.get("comment") or col2,
            "samples": p2.get("samples", []),
            "sql": f"SELECT `{col2}`, COUNT(*) FROM `{table_name}` WHERE `{col1}` = :slot_{col1} GROUP BY `{col2}`",
            "slots": [col1],
            "human_query": f"在指定{p1.get('comment') or col1}下，不同{p2.get('comment') or col2}各有多少人？",
            "api_description": f"统计指定{p1.get('comment') or col1}下各{p2.get('comment') or col2}的人数分布",
            "layer_tag": "Layer-A",
        })

    for col_name, p in date_cols[:1]:
        proposals.append({
            "query_type": "aggregate_with_range",
            "col_name": col_name,
            "col_comment": p.get("comment") or col_name,
            "samples": p.get("samples", []),
            "sql": f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col_name}` BETWEEN :slot_{col_name}_start AND :slot_{col_name}_end",
            "slots": [f"{col_name}_start", f"{col_name}_end"],
            "human_query": f"我想看一下指定{p.get('comment') or col_name}区间内入表的员工有多少。",
            "api_description": f"统计指定{p.get('comment') or col_name}范围内的员工数量",
            "layer_tag": "Layer-A",
        })

    return proposals


def _layer_a_type_max_success(query_type: str) -> int:
    limits = {
        "table_count": 1,
        "aggregate_no_filter": 1,
        "list_no_filter": 1,
        "aggregate_with_filter": 2,
        "list_with_filter": 2,
        "aggregate_with_multi_filter": 1,
        "group_aggregate_with_filter": 1,
        "aggregate_with_range": 1,
        "exact_query": 2,
        "group_distribution": 4,
        "numeric_stats": 1,
        "group_aggregate": 1,
    }
    return limits.get(query_type, 1)


def _should_stop_layer_a_type(type_stats: dict, query_type: str) -> bool:
    stats = type_stats.get(query_type, {})
    success = int(stats.get("success", 0))
    attempts = int(stats.get("attempts", 0))
    hard_cap = _layer_a_type_max_success(query_type)

    if success >= hard_cap:
        return True

    # 如果这个类型已经探索过多次仍没有有效样本，提前停止继续试探
    if attempts >= 3 and success == 0:
        return True

    return False


def refine_api_semantics_by_llm(table_context: dict, api_proposal: dict):
    """
    使用 LLM 对规则生成的硬核描述进行"丝滑化"处理
    """
    prompt = f"""
你是一个专业的商业分析师。请将数据库层面的 API 描述翻译成自然、拟人化的业务查询意图。

【上下文信息】
- 表名: {table_context['table_name']} (备注: {table_context['table_comment']})
- 当前 API 基础逻辑: {api_proposal['query_type']}
- 涉及字段: {api_proposal['col_name']} ({api_proposal['col_comment']})
- 数据采样示例: {api_proposal['samples']}

【任务】
请生成两个字段：
1. human_query: 模拟真实用户（非技术人员）会如何口头询问这个问题。要多样化、口语化。
2. api_description: 简洁专业的业务功能描述。

【约束】
- 禁止出现"字段"、"表格"、"SQL"、"数据库"等技术词汇。
- 如果采样值是 0/1 或 M/F，请根据字段备注推断其业务含义。
- 输出格式必须为 JSON。

示例输出:
{{
  "human_query": "咱们公司现在的员工男女比例分布是怎么样的？",
  "api_description": "统计员工性别分布情况"
}}
"""
    response = call_llm_json(prompt)
    return response


def _fallback_refined_semantics(table_name: str, prop: dict) -> dict:
    query_type = prop.get("query_type", "query")
    col_comment = prop.get("col_comment") or prop.get("col_name") or "条件"
    mapping = {
        "table_count": ("当前总共有多少条记录", "统计总记录数"),
        "aggregate_no_filter": ("当前总共有多少条记录", "统计总记录数"),
        "aggregate_with_filter": (f"指定{col_comment}下有多少条记录", f"统计指定{col_comment}下的记录数"),
        "aggregate_with_multi_filter": (f"指定多个条件下有多少条记录", f"统计组合条件下的记录数"),
        "aggregate_with_range": (f"指定{col_comment}范围内有多少条记录", f"统计指定{col_comment}范围内的记录数"),
        "group_distribution": (f"按{col_comment}统计分布情况", f"统计{col_comment}分布"),
        "group_aggregate_with_filter": (f"指定条件下按{col_comment}统计分布情况", f"统计指定条件下的{col_comment}分布"),
        "numeric_stats": (f"{col_comment}的平均值、最大值和最小值是多少", f"统计{col_comment}数值指标"),
        "exact_query": (f"查询指定{col_comment}对应的明细", f"按{col_comment}精确查询"),
        "list_no_filter": (f"查看全部{table_name}基础列表", f"查看{table_name}列表"),
        "list_with_filter": (f"查看指定{col_comment}下的列表", f"查看指定{col_comment}下的列表"),
    }
    human_query, api_desc = mapping.get(query_type, (f"查询{table_name}相关数据", f"{query_type}查询"))
    return {
        "human_query": human_query,
        "api_description": api_desc,
    }


def _build_schema_based_fallback_proposals(table_name: str, table_info: dict) -> list[dict]:
    fields = table_info.get("fields", {})
    if isinstance(fields, list):
        field_map = {f.get("name"): f for f in fields if isinstance(f, dict) and f.get("name")}
    else:
        field_map = fields

    proposals = [
        {
            "query_type": "table_count",
            "col_name": "ALL",
            "col_comment": "全表",
            "samples": "N/A",
            "sql": f"SELECT COUNT(*) FROM {table_name}",
            "slots": [],
            "layer_tag": "Layer-A",
        }
    ]

    dim_col = None
    num_col = None
    for col_name, info in field_map.items():
        col_type = str((info or {}).get("type", "")).upper()
        if not dim_col and not any(t in col_type for t in ["INT", "FLOAT", "DECIMAL", "DOUBLE", "BIGINT"]):
            dim_col = (col_name, info)
        if not num_col and any(t in col_type for t in ["INT", "FLOAT", "DECIMAL", "DOUBLE", "BIGINT"]):
            num_col = (col_name, info)

    if dim_col:
        col_name, info = dim_col
        proposals.append({
            "query_type": "group_distribution",
            "col_name": col_name,
            "col_comment": (info or {}).get("comment") or col_name,
            "samples": [],
            "sql": f"SELECT `{col_name}`, COUNT(*) FROM `{table_name}` GROUP BY `{col_name}`",
            "slots": [],
            "layer_tag": "Layer-A",
        })

    if num_col:
        col_name, info = num_col
        proposals.append({
            "query_type": "numeric_stats",
            "col_name": col_name,
            "col_comment": (info or {}).get("comment") or col_name,
            "samples": [],
            "sql": f"SELECT AVG(`{col_name}`), MAX(`{col_name}`), MIN(`{col_name}`) FROM `{table_name}`",
            "slots": [],
            "layer_tag": "Layer-A",
        })

    return proposals


def _generate_layer_a_query_variants(table_name: str, table_comment: str, seed_records: list[dict]) -> list[dict]:
    if not seed_records:
        return []

    seed_items = []
    for r in seed_records[:12]:
        api = r.get("api_schema", {}) if isinstance(r, dict) else {}
        query = r.get("query", "")
        if not query or not isinstance(api, dict):
            continue
        seed_items.append({
            "query": query,
            "api_name": api.get("name", ""),
            "query_type": r.get("query_type") or api.get("query_type", ""),
            "sql": api.get("bound_sql", ""),
            "description": api.get("description", ""),
        })

    if not seed_items:
        return []

    prompt = f"""
你要基于已有业务问询，生成“同一业务意图”的自然扩写，不要发明新意图。
表名: {table_name}
表备注: {table_comment}

已有种子样本:
{seed_items}

要求:
1. 每个 seed 最多扩写 1 条
2. 保持原有业务意图和 SQL 语义不变
3. 表达更像真实用户说法，不要使用“指定的值/给定的值/某字段”这类模板词
4. 不要重复已有问句
5. 输出 JSON: {{"variants": [{{"api_name":"...", "query":"..."}}]}}
"""

    result = call_llm_json(prompt)
    if not isinstance(result, dict):
        return []
    variants = result.get("variants")
    if not isinstance(variants, list):
        return []
    return [v for v in variants if isinstance(v, dict) and v.get("api_name") and v.get("query")]


def _select_augment_seed_records(seed_records: list[dict]) -> list[dict]:
    by_type: dict[str, list[dict]] = defaultdict(list)
    for r in seed_records:
        api = r.get("api_schema", {}) if isinstance(r, dict) else {}
        if not isinstance(api, dict):
            continue
        if not r.get("query"):
            continue
        by_type[r.get("query_type") or api.get("query_type") or "unknown"].append(r)

    budgets = {
        "table_count": 1,
        "aggregate_no_filter": 1,
        "list_no_filter": 1,
        "exact_query": 2,
        "group_distribution": 3,
        "numeric_stats": 1,
    }

    selected = []
    for query_type, records in by_type.items():
        cap = budgets.get(query_type, 1)
        selected.extend(records[:cap])
    return selected


def profile_table_with_data(db_conn, table_name: str, fields):
    """
    通过数据采样，识别字段的基数（Cardinality）和业务特征
    """
    profiles = {}
    sample_size = 3000

    # 兼容 fields 可以是 dict（旧格式）或 list（schema.loader当前输出）
    if isinstance(fields, list):
        normalized = {}
        for f in fields:
            if isinstance(f, dict):
                col_name = f.get("name")
                normalized[col_name] = f
            else:
                # 可能是Pydantic模型
                col_name = getattr(f, "name", None)
                normalized[col_name] = f.__dict__ if hasattr(f, "__dict__") else f
        fields = normalized
    elif isinstance(fields, dict):
        # 可以是 FieldInfo对象结构
        normalized = {}
        for k, v in fields.items():
            if hasattr(v, "dict"):
                normalized[k] = v.dict()
            else:
                normalized[k] = v
        fields = normalized

    for col_name, col_info in fields.items():
        try:
            # 获取基数和采样
            sql = f"""
                SELECT 
                    COUNT({col_name}) as total,
                    COUNT(DISTINCT {col_name}) as distinct_cnt
                FROM (SELECT {col_name} FROM {table_name} LIMIT {sample_size}) t
            """
            res = execute_sql(db_conn, sql)
            if res["status"] != "success":
                continue
            
            total, d_cnt = res["data"][0] if res["data"] else (0, 0)
            
            # 采3个真实样本
            res_samples = execute_sql(
                db_conn,
                f"SELECT DISTINCT `{col_name}` FROM `{table_name}` WHERE `{col_name}` IS NOT NULL LIMIT 3"
            )
            samples = [str(r[0]) for r in res_samples.get("data", [])] if res_samples["status"] == "success" else []
            
            uniqueness = d_cnt / total if total > 0 else 0
            col_type = str(col_info.get("type", "VARCHAR")).upper()

            profiles[col_name] = {
                "d_cnt": d_cnt,
                "uniqueness": uniqueness,
                "samples": samples,
                "type": col_type,
                "comment": col_info.get("comment") or col_name
            }
        except Exception as e:
            print(f"  [Profile] {col_name} 分析失败: {e}")
            continue
    
    return profiles


class RuleBasedGenerator:
    """规则生成器（简化实现）"""
    def _analyze_table(self, profile: dict) -> dict:
        result = {}
        for name, info in profile.get("fields", {}).items():
            col_type = str(info.get("type", "")).upper()
            if col_type in ("INT", "BIGINT", "FLOAT", "DECIMAL", "DOUBLE"):
                result[name] = "measure"
            else:
                result[name] = "dimension"
        return result

    def _generate_rules_for_table(self, profile: dict, sample_values: dict = None) -> list[dict]:
        rules = []
        table = profile.get("table_name", "table")

        # 简单生成两个查询类型
        rules.append({
            "query_type": "aggregate_no_filter",
            "sql": f"SELECT COUNT(*) FROM {table}",
            "slot_mapping": {},
            "layer_tag": "Layer-A"
        })

        if profile.get("fields"):
            first_dim = next((n for n,i in profile["fields"].items() if i.get("type", "").upper() not in ("INT","BIGINT","FLOAT","DECIMAL","DOUBLE")), None)
            if first_dim:
                rules.append({
                    "query_type": "group_aggregate",
                    "sql": f"SELECT {first_dim}, COUNT(*) FROM {table} GROUP BY {first_dim}",
                    "slot_mapping": {first_dim: first_dim},
                    "layer_tag": "Layer-A"
                })

        if len(rules) == 1:
            rules.append({
                "query_type": "list_no_filter",
                "sql": f"SELECT * FROM {table} LIMIT 10",
                "slot_mapping": {},
                "layer_tag": "Layer-A"
            })

        return rules


def _generate_llm_baserules(table_name: str, table_comment: str, profiles: dict) -> list[dict]:
    """通过LLM补充更多基于业务的统计查询规则"""
    if not profiles:
        return []

    key_info = []
    for col, info in profiles.items():
        sample = info.get("samples")
        if isinstance(sample, list) and sample:
            key_info.append(f"{col}={sample[0]}")
        else:
            key_info.append(col)

    prompt = f"""
你是一个可生成业务统计式API的智能助手。
表名: {table_name}，备注: {table_comment}
字段样本: {', '.join(key_info[:20])}
请基于上述表信息，生成最多 5 条“非技术用户友好”的统计型问题（例如：深圳地区有多少员工？）以及对应SQL模板。

要求:
1. 输出JSON，只包含字段 rules
2. rules 是数组，元素包含: query_type、human_query、sql
3. sql 用 :参数 方式，不填实际值
4. query_type 仅限: aggregate_with_filter/group_aggregate/list_no_filter/exact_query

示例：
{{ "rules": [
  {{"query_type": "aggregate_with_filter", "human_query": "深圳地区有多少员工", "sql": "SELECT COUNT(*) FROM base_staff WHERE location = :location"}},
  ...
] }}
"""
    try:
        result = call_llm_json(prompt)
    except Exception:
        result = None

    proposals = []
    if result and isinstance(result, dict):
        rules = result.get("rules") or []
        for r in rules:
            if not isinstance(r, dict):
                continue
            if "sql" in r and "human_query" in r:
                proposals.append({
                    "query_type": r.get("query_type", "aggregate_with_filter"),
                    "col_name": "ALL",
                    "col_comment": "",
                    "samples": "N/A",
                    "sql": r["sql"],
                    "slots": extract_slots(r["sql"]),
                    "human_query": r.get("human_query", ""),
                    "layer_tag": "Layer-A"
                })

    # 失败时降级：根据常见字段生成规则
    if not proposals:
        candidate_cols = [c for c in profiles.keys() if any(x in c.lower() for x in ["city", "location", "area", "province", "region", "department", "dept", "business", "company"])]
        for c in candidate_cols[:3]:
            proposals.append({
                "query_type": "aggregate_with_filter",
                "col_name": c,
                "col_comment": profiles[c].get("comment", "") if profiles.get(c) else "",
                "samples": profiles[c].get("samples", []) if profiles.get(c) else [],
                "sql": f"SELECT COUNT(*) FROM {table_name} WHERE {c} = :{c}",
                "slots": [c],
                "human_query": f"统计{c}为指定值的员工数量",
            })

    # 通用基线统计（upto 2条）
    if len(proposals) < 3:
        proposals.append({
            "query_type": "aggregate_no_filter",
            "col_name": "ALL",
            "col_comment": "全表",
            "samples": "N/A",
            "sql": f"SELECT COUNT(*) FROM {table_name}",
            "slots": []
        })

    return proposals

    return proposals


def run_advanced_rule_pipeline(
    db_conn,
    full_schema: dict,
    valid_path: str = "dataset_valid.jsonl",
    invalid_path: str = "dataset_invalid.jsonl",
    gate=None,
    mode: str = "bootstrap",
):
    """
    运行高级规则生成Pipeline
    
    基于数据探查自动生成基础API
    """
    print("\n" + "="*60)
    print("🚀 语义增强型 NL2AutoAPI 语料生成开启")
    print("="*60)

    tables = full_schema["tables"]

    for table_name, table_info in tables.items():
        table_comment = table_info.get("comment") or table_name
        print(f"\n[处理表] {table_name} ({table_comment})")
        local_query_signatures: set[tuple[str, str]] = set()
        group_distribution_sample_signatures: set[tuple[str, ...]] = set()
        type_stats: dict[str, dict[str, int]] = {}

        if mode == "augment":
            existing = [
                r for r in load_jsonl(valid_path)
                if r.get("table") == table_name and r.get("layer_tag") == "Layer-A"
            ]
            selected_seeds = _select_augment_seed_records(existing)
            variants = _generate_layer_a_query_variants(table_name, table_comment, selected_seeds)
            api_by_name = {
                r.get("api_schema", {}).get("name"): r for r in existing if isinstance(r.get("api_schema"), dict)
            }
            for variant in variants:
                seed = api_by_name.get(variant.get("api_name"))
                if not seed:
                    continue
                new_query = str(variant.get("query", "")).strip()
                if not new_query:
                    continue
                api_schema = dict(seed.get("api_schema", {}))
                api_schema["query"] = new_query
                aligned, align_reason = _semantic_alignment_check(
                    query=new_query,
                    sql=api_schema.get("bound_sql", ""),
                    query_type=api_schema.get("query_type", ""),
                )
                if not aligned:
                    print(f"  [SKIP] augment 语义错位: {align_reason}")
                    continue
                if gate is not None:
                    accept, reason = gate.check(
                        query=new_query,
                        sql=api_schema.get("bound_sql", ""),
                        table=table_name,
                        query_type=api_schema.get("query_type", ""),
                    )
                    if not accept:
                        gate.reject(
                            query=new_query,
                            sql=api_schema.get("bound_sql", ""),
                            table=table_name,
                            query_type=api_schema.get("query_type", ""),
                            layer_tag="Layer-A",
                            reason=reason or "不符合常识问法",
                        )
                        continue
                written = save_jsonl_dedup_sql(valid_path, {
                    "source": "prebuild_generation",
                    "source_stage": "prebuild",
                    "source_method": "rule_based_augment",
                    "source_channel": "build_pipeline",
                    "table": table_name,
                    "query": new_query,
                    "api_schema": api_schema,
                    "test_execution": seed.get("test_execution", {}),
                    "query_type": seed.get("query_type") or api_schema.get("query_type", "unknown"),
                    "layer_tag": "Layer-A",
                }, allow_same_sql_duplicates=True)
                if written:
                    print(f"  [AUGMENT] {new_query[:50]}...")
            continue

        # Step 1: 数据探查
        profiles = profile_table_with_data(db_conn, table_name, table_info["fields"])
        
        # Step 2: 收集待加工的 API 建议
        api_proposals = []
        selected_group_distribution_cols = set(_select_group_distribution_cols(profiles, limit=5))

        # LLM增强规则（覆盖更多业务语义）
        api_proposals.extend(_generate_llm_baserules(table_name, table_comment, profiles))
        api_proposals.extend(_build_richer_bootstrap_proposals(table_name, table_info, profiles))

        if not profiles:
            api_proposals.extend(_build_schema_based_fallback_proposals(table_name, table_info))
        
        # A. 全表统计
        api_proposals.append({
            "query_type": "table_count",
            "col_name": "ALL",
            "col_comment": "全表",
            "samples": "N/A",
            "sql": f"SELECT COUNT(*) FROM {table_name}",
            "slots": [],
            "layer_tag": "Layer-A",
        })

        for col_name, p in profiles.items():
            # B. 分类汇总 (低基数列)
            if col_name in selected_group_distribution_cols and 1 < p["d_cnt"] <= 12:
                sample_sig = _sample_signature(p.get("samples", []))
                if sample_sig and sample_sig in group_distribution_sample_signatures:
                    print(f"  [SKIP] group_distribution 同族字段重复: {col_name}")
                    continue
                if sample_sig:
                    group_distribution_sample_signatures.add(sample_sig)
                api_proposals.append({
                    "query_type": "group_distribution",
                    "col_name": col_name,
                    "col_comment": p["comment"],
                    "samples": p["samples"],
                    "sql": f"SELECT `{col_name}`, COUNT(*) FROM `{table_name}` GROUP BY `{col_name}`",
                    "slots": [],
                    "layer_tag": "Layer-A",
                })
            
            # C. 数值统计 (度量列)
            elif "INT" in p["type"] or "FLOAT" in p["type"] or "DECIMAL" in p["type"]:
                if p["d_cnt"] > 12:  # 排除掉可能是枚举的数字
                    api_proposals.append({
                        "query_type": "numeric_stats",
                        "col_name": col_name,
                        "col_comment": p["comment"],
                        "samples": p["samples"],
                        "sql": f"SELECT AVG(`{col_name}`), MAX(`{col_name}`), MIN(`{col_name}`) FROM `{table_name}`",
                        "slots": [],
                        "layer_tag": "Layer-A",
                    })
            
            # D. 精确匹配 (高唯一率)
            if p["uniqueness"] > 0.8 and _is_identity_lookup_column(col_name, p.get("comment", "")):
                slot_name = f"slot_{col_name}"
                api_proposals.append({
                    "query_type": "exact_query",
                    "col_name": col_name,
                    "col_comment": p["comment"],
                    "samples": p["samples"],
                    "sql": f"SELECT * FROM `{table_name}` WHERE `{col_name}` = :{slot_name}",
                    "slots": [col_name],
                    "layer_tag": "Layer-A",
                })

        # Step 3: LLM 润色 + 执行验证
        table_context = {"table_name": table_name, "table_comment": table_comment}
        
        for prop in api_proposals:
            try:
                query_type = prop.get("query_type", "unknown")
                if _should_stop_layer_a_type(type_stats, query_type):
                    print(f"  [STOP] {query_type} 已达到停止探索条件")
                    continue
                type_stats.setdefault(query_type, {"attempts": 0, "success": 0})
                type_stats[query_type]["attempts"] += 1

                prop_sql = correct_sql_columns(
                    prop.get("sql", ""),
                    table_name=table_name,
                    schema_fields=table_info.get("fields", {}),
                )
                prop["sql"] = prop_sql

                # LLM 加工描述，失败时兜底
                refined = None
                if prop.get("human_query") and prop.get("api_description"):
                    refined = {
                        "human_query": prop.get("human_query", ""),
                        "api_description": prop.get("api_description", ""),
                    }
                else:
                    try:
                        refined = refine_api_semantics_by_llm(table_context, prop)
                    except Exception:
                        refined = None
                if not refined:
                    refined = _fallback_refined_semantics(table_name, prop)
                aligned, align_reason = _semantic_alignment_check(
                    query=refined.get("human_query", ""),
                    sql=prop.get("sql", ""),
                    query_type=prop.get("query_type", ""),
                )
                if not aligned:
                    print(f"  [SKIP] 语义错位: {align_reason}")
                    continue
                
                # 构建 API Schema
                api_schema = {
                    "name": f"{prop['query_type']}_{table_name}_{prop['col_name']}",
                    "description": refined.get("api_description", f"{prop['query_type']}查询"),
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "required": []
                    },
                    "query": refined.get("human_query", ""),
                    "bound_sql": prop["sql"],
                    "slot_mapping": {f"slot_{s}": f"slot_{s}" for s in prop["slots"]},
                    "query_type": prop["query_type"],
                    "source": "data_driven_rule",
                    "table": table_name,
                }

                if gate is not None:
                    accept, reason, final_query = gate.check_with_concretize(
                        query=api_schema.get("query", ""),
                        sql=prop.get("sql", ""),
                        table=table_name,
                        query_type=prop.get("query_type", ""),
                    )
                    if not accept:
                        gate.reject(
                            query=final_query,
                            sql=prop.get("sql", ""),
                            table=table_name,
                            query_type=prop.get("query_type", ""),
                            layer_tag=prop.get("layer_tag", "Layer-A"),
                            reason=reason or "不符合常识问法",
                        )
                        print(f"  [GATE] 拒绝入库: {reason}")
                        continue
                    # Update query if it was concretized
                    if final_query != api_schema.get("query", ""):
                        api_schema["query"] = final_query
                        print(f"  [GATE] Query concretized: {final_query[:60]}...")

                query_signature = (
                    prop.get("query_type", ""),
                    _normalize_query_semantic(api_schema.get("query", "")),
                )
                if query_signature[1] and query_signature in local_query_signatures:
                    print("  [SKIP] Layer-A 本轮近义问法重复")
                    continue
                local_query_signatures.add(query_signature)

                # 添加slot到inputSchema
                for slot in prop["slots"]:
                    slot_key = f"slot_{slot}"
                    api_schema["inputSchema"]["properties"][slot_key] = {
                        "type": "string",
                        "description": f"{prop['col_comment']}的值"
                    }
                    api_schema["inputSchema"]["required"].append(slot_key)

                # 执行验证
                slot_values = {}
                placeholders = extract_slots(prop["sql"])
                if placeholders:
                    sample_val = None
                    if prop.get("samples") and prop["samples"] != "N/A":
                        if isinstance(prop["samples"], list) and prop["samples"]:
                            sample_val = prop["samples"][0]
                        elif isinstance(prop["samples"], str):
                            sample_val = prop["samples"]
                    if sample_val is None:
                        sample_val = "test"

                    for slot_name in placeholders:
                        slot_values[slot_name] = sample_val
                        if slot_name.startswith("slot_"):
                            raw = slot_name.replace("slot_", "", 1)
                            slot_values.setdefault(raw, sample_val)
                        else:
                            slot_values.setdefault(f"slot_{slot_name}", sample_val)

                exec_sql = prop["sql"]
                for k, v in slot_values.items():
                    exec_sql = exec_sql.replace(f":{k}", f"'{v}'" if isinstance(v, str) else str(v))
                
                if db_conn is None:
                    res = {
                        "status": "success",
                        "columns": [],
                        "data": [],
                        "row_count": 0,
                        "note": "no_db_mode_skip_execution",
                    }
                else:
                    res = execute_sql(db_conn, exec_sql)
                
                if res["status"] == "success":
                    print(f"  [SUCCESS] {api_schema['description'][:50]}...")
                    type_stats[query_type]["success"] += 1
                    save_jsonl_dedup_sql(valid_path, {
                        "source": "prebuild_generation",
                        "source_stage": "prebuild",
                        "source_method": "rule_based",
                        "source_channel": "build_pipeline",
                        "table": table_name,
                        "query": api_schema["query"],
                        "api_schema": api_schema,
                        "test_execution": res,
                        "query_type": prop["query_type"],
                        "layer_tag": prop.get("layer_tag", "Layer-A"),
                    })
                else:
                    print(f"  [FAILED] 执行失败: {res.get('error', 'unknown')}")
                    save_jsonl(invalid_path, {
                        "api": api_schema,
                        "error": res,
                        "reason": "execution_failed",
                        "table": table_name
                    })

            except Exception as e:
                print(f"  [ERROR] {e}")
                continue

    print("\n✅ 规则生成任务完成。")