import os
import random
import json
from datetime import datetime
from typing import List, Optional

from core.utils import save_jsonl, save_jsonl_dedup_sql, load_jsonl, overwrite_jsonl
from core.llm import call_llm_json
from core.database import execute_sql
from generation.api_generator import APIGenerator
from schema.loader import SchemaLoader
from runtime.router import RuntimeRouter
from validation.intent_verify import IntentVerifier


def generate_queries_from_desc(table_desc: str, n: int = 20) -> List[str]:
    """根据表描述生成随机业务查询"""
    prompt = f"""
给定一张表的业务描述，请生成{n}条用户自然语言的统计/查询问题。
表描述：{table_desc}
要求：
1. 返回JSON对象，key为 queries，值为问题列表
2. 问题应面向非技术人员，简洁明了
3. 不重复

示例：
{{
  "queries": ["深圳地区有多少员工？", "各业务单元员工人数分布是多少？", ...]
}}
"""
    result = call_llm_json(prompt)
    if not isinstance(result, dict):
        return []

    queries = result.get("queries")
    if not isinstance(queries, list):
        return []

    return [q.strip() for q in queries if isinstance(q, str) and q.strip()][:n]


def _build_table_prompt(table_name: str, table_desc: str, schema_loader: Optional[SchemaLoader] = None):
    """构建表字段与描述的 LLM 召回信息"""
    fields_text = ""
    if schema_loader:
        schema = schema_loader.get_table(table_name)
        if schema:
            fields = schema.fields
            lines = []
            for f in fields:
                lines.append(f"- {f.name} ({f.type}) {f.comment or ''}".strip())
            fields_text = "\n".join(lines)

    if not fields_text:
        fields_text = "表字段信息不可用，请根据业务描述理解字段语义。"

    return f"""
表名: {table_name}
表描述: {table_desc}
字段:
{fields_text}
"""


def _fallback_generate_api(query: str, table_name: str, table_desc: str, schema_loader: Optional[SchemaLoader] = None):
    """从表字段/描述生成SQL和API，用于未命中现有API的情况"""
    table_prompt = _build_table_prompt(table_name, table_desc, schema_loader)

    prompt = f"""
基于以下数据库表信息和用户查询，生成一个适用SQL（可含过滤条件）以及本次SQL对应的API描述。
{table_prompt}
用户查询: {query}

要求：
1. SQL 使用表名 {table_name}，必要时加 WHERE / GROUP BY / HAVING。
2. 可包含slot占位符，如 :city, :status 等。
3. 输出JSON仅包含字段：sql, api_description, query_type（如 exact_query/group_aggregate ），input_slots。
"""

    result = call_llm_json(prompt)
    if not isinstance(result, dict):
        return None

    sql = result.get("sql")
    if not sql or not isinstance(sql, str):
        return None
    description = result.get("api_description", query)
    query_type = result.get("query_type", "exact_query")

    generator = APIGenerator()
    api_schema = generator.generate_from_runtime(query, sql, table_name, description)

    return {
        "api_schema": api_schema,
        "sql": sql,
        "query_type": query_type,
        "description": description,
    }


def run_runtime_loop(
    router: RuntimeRouter,
    table_name: str,
    table_desc: str,
    num_queries: int = 20,
    batch_size: int = 5,
    max_rounds: int = 3,
    output_dir: str = "./output",
    schema_path: Optional[str] = None,
):
    """主流程：生成 query -> route -> verify -> 保存结果"""
    queries = generate_queries_from_desc(table_desc, num_queries)
    if not queries:
        raise ValueError("无法生成 query，请检查 table_desc 或 LLM 返回")

    os.makedirs(output_dir, exist_ok=True)
    runtime_valid_path = os.path.join(output_dir, "runtime_valid.jsonl")
    runtime_invalid_path = os.path.join(output_dir, "runtime_invalid.jsonl")
    manual_review_path = os.path.join(output_dir, "manual_review.jsonl")

    intent_verifier = IntentVerifier()

    def auto_refine_query(q, router, attempts: int = 3):
        """用RuntimeRouter尝试自我校验三次，自动修正后重试。"""
        for i in range(1, attempts + 1):
            print(f"[AutoRefine] 第{i}/{attempts}次重试: {q}")
            candidate = router.route(q)
            # 再次判定
            if candidate.status == "success" and candidate.verification and candidate.verification.type == "CORRECT":
                return candidate, True
            if candidate.status == "success" and not candidate.verification:
                # 兜底：执行成功也可认定
                return candidate, True
        return candidate, False

    def check_and_persist(q, res):
        pass_flag = False
        try:
            pass_flag = intent_verifier.verify(q, res.invoked_sql or "", res.exec_result or {})
        except Exception as e:
            pass_flag = False

        rec = {
            "source": "runtime",
            "query": q,
            "api_name": res.api_name,
            "sql": res.invoked_sql,
            "params": res.params,
            "status": res.status,
            "row_count": res.row_count,
            "columns": res.columns,
            "data": res.data,
            "verification": {
                "pass": pass_flag,
                "reason": getattr(res, "verification", None),
            },
            "runtime_source": "online",  # 区分事前/事中
            "review_status": "undecided",
            "source_stage": "runtime",
            "source_method": "initial_verify",
            "source_channel": "online_runtime",
        }

        if pass_flag and res.status == "success":
            rec["review_status"] = "auto_pass"
            rec["review_method"] = "initial_verify"
            save_jsonl_dedup_sql(runtime_valid_path, rec)
            return True

        # 1) 先尝试自动自我修正（LLM self refine）
        refined, refined_ok = auto_refine_query(q, router)
        if refined_ok:
            refined_rec = {
                "source": "runtime",
                "query": q,
                "api_name": refined.api_name,
                "sql": refined.invoked_sql,
                "params": refined.params,
                "status": refined.status,
                "row_count": refined.row_count,
                "columns": refined.columns,
                "data": refined.data,
                "verification": {
                    "pass": True,
                    "reason": getattr(refined, "verification", None),
                },
                "runtime_source": "online",
                "review_status": "auto_pass",
                "review_method": "llm_self_refine",
                "source_stage": "runtime",
                "source_method": "llm_self_refine",
                "source_channel": "online_runtime",
            }
            save_jsonl_dedup_sql(runtime_valid_path, refined_rec)
            return True

        # 2) 还是不通过，转入人工审核
        rec["review_status"] = "needs_manual"
        rec["review_method"] = "llm_self_refine_failed"
        rec["auto_refine_attempts"] = 3
        save_jsonl(runtime_invalid_path, rec)
        return False

    schema_loader = SchemaLoader(schema_path) if schema_path else None

    # 多轮召回试错
    for round_idx in range(1, max_rounds + 1):
        print(f"\n=== Runtime Round {round_idx}/{max_rounds} ===")
        start = (round_idx - 1) * batch_size
        sub = queries[start:start + batch_size]
        if not sub:
            break

        for q in sub:
            result = router.route(q)
            if result.status != "success":
                # 跳过 API 回路，走预设invalid的字段级生成流程
                fb = _fallback_generate_api(q, table_name, table_desc, schema_loader)
                if fb is not None and fb.get("api_schema") and fb.get("sql"):
                    # 执行生成SQL
                    exec_result = execute_sql(None, fb["sql"])
                    intent_pass = False
                    try:
                        intent_pass = IntentVerifier().verify(q, fb["sql"], exec_result)
                    except Exception:
                        intent_pass = False

                    rec = {
                        "source": "fallback",
                        "query": q,
                        "api_name": fb["api_schema"].name,
                        "sql": fb["sql"],
                        "params": fb["api_schema"].slot_mapping,
                        "status": "success" if exec_result.get("status") == "success" else "error",
                        "row_count": exec_result.get("row_count"),
                        "columns": exec_result.get("columns"),
                        "data": exec_result.get("data"),
                        "verification": {
                            "pass": intent_pass,
                            "reason": fb.get("description"),
                        },
                        "runtime_source": "online_fallback",
                        "review_status": "auto_pass" if intent_pass else "needs_manual",
                        "review_method": "fields_generate",
                        "source_stage": "runtime",
                        "source_method": "fields_generate",
                        "source_channel": "online_runtime",
                    }

                    if intent_pass:
                        save_jsonl_dedup_sql(runtime_valid_path, rec)
                        print(f"[Fallback] {q} 生成并通过 -> valid")
                        continue

                    rec["review_reason"] = "融通过自动生成+验证失败"
                    save_jsonl(runtime_invalid_path, rec)
                    print(f"[Fallback] {q} 生成未通过 -> invalid")
                    continue

                # 走不到这说明预设和fallback都失败，直接写manual invalid
                save_jsonl(runtime_invalid_path, {
                    "source": "runtime",
                    "query": q,
                    "status": result.status,
                    "error": result.error,
                    "review_status": "needs_manual",
                })
                continue

            pass_flag = check_and_persist(q, result)
            if not pass_flag:
                print(f"[Runtime] {q} 判定不通过，加入invalid")
            else:
                print(f"[Runtime] {q} 判定通过 -> valid")

    # 只要有不通过的，就写一份人工review文件
    if os.path.exists(runtime_invalid_path):
        with open(runtime_invalid_path, "r", encoding="utf8") as fr, open(manual_review_path, "w", encoding="utf8") as fw:
            for line in fr:
                rec = json.loads(line)
                rec.setdefault("review_status", "needs_manual")
                rec.setdefault("reviewer", None)
                rec.setdefault("review_reason", None)
                rec.setdefault("reviewed_at", None)
                fw.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

    # 追加标注接口工具（可直接在外部调用）
    def manual_review_record(query_key: str, decision: str, reviewer: str, reason: Optional[str] = None):
        """手动检查runtime_invalid或manual_review并同步到runtime_valid/invalid。"""
        records = load_jsonl(manual_review_path)
        updated = False
        for r in records:
            if r.get("query") == query_key and r.get("review_status") in ["needs_manual", "undecided"]:
                r["review_status"] = "manual_pass" if decision == "pass" else "manual_reject"
                r["reviewer"] = reviewer
                r["review_reason"] = reason
                r["reviewed_at"] = datetime.now().isoformat()
                r["review_method"] = "manual"

                if decision == "pass":
                    # 还需通过LLM二次验证
                    lv = IntentVerifier().verify(r.get("query", ""), r.get("sql", ""), {"data": r.get("data"), "columns": r.get("columns"), "row_count": r.get("row_count")})
                    if lv:
                        save_jsonl_dedup_sql(runtime_valid_path, r)
                    else:
                        r["review_status"] = "manual_reject"
                        save_jsonl(runtime_invalid_path, r)
                else:
                    save_jsonl(runtime_invalid_path, r)

                updated = True

        if not updated:
            raise ValueError(f"未找到可审核记录 query={query_key}")

        overwrite_jsonl(manual_review_path, records)

        return updated

    def expand_query_apis(query: str, router: RuntimeRouter, table_desc: str, n: int = 5):
        """基于已通过的query做横向/纵向补充候选API（示意接口）"""
        # 这里我们只提供思路：可以用现有的generate_queries_from_desc或外部表描述 + LLM，
        # 生成相关度高的衍生 query 并循环执行路由结果，补充在runtime_valid / schema pool。
        derived = generate_queries_from_desc(table_desc, n)
        new_records = []
        for dq in derived:
            if dq == query:
                continue
            res = router.route(dq)
            if res.status == "success" and res.verification and res.verification.type == "CORRECT":
                rec = {
                    "source": "runtime_expand",
                    "query": dq,
                    "api_name": res.api_name,
                    "sql": res.invoked_sql,
                    "params": res.params,
                    "status": res.status,
                    "row_count": res.row_count,
                    "columns": res.columns,
                    "data": res.data,
                    "verification": {
                        "pass": True,
                        "reason": getattr(res, "verification", None),
                    },
                    "runtime_source": "online_expand",
                    "review_status": "auto_pass",
                    "review_method": "expand",
                    "source_stage": "runtime",
                    "source_method": "expand",
                    "source_channel": "online_runtime",
                }
                save_jsonl_dedup_sql(runtime_valid_path, rec)
                new_records.append(rec)

        return new_records

    print(f"runtime_valid: {runtime_valid_path}")
    print(f"runtime_invalid: {runtime_invalid_path}")
    print(f"manual_review: {manual_review_path}")

    return {
        "runtime_valid": runtime_valid_path,
        "runtime_invalid": runtime_invalid_path,
        "manual_review": manual_review_path,
        "manual_review_record": manual_review_record,
        "expand_query_apis": expand_query_apis,
        "process_invalid_source": lambda path: process_invalid_source(path, router, table_name, table_desc, output_dir, schema_path),
    }


def process_invalid_source(
    invalid_path: str,
    router: RuntimeRouter,
    table_name: str,
    table_desc: str,
    output_dir: str = "./output",
    schema_path: Optional[str] = None,
):
    """处理已有 invalid 文件，走字段召回+API生成+校验流程"""
    if not os.path.exists(invalid_path):
        raise FileNotFoundError(f"invalid文件不存在: {invalid_path}")

    records = load_jsonl(invalid_path)
    schema_loader = SchemaLoader(schema_path) if schema_path else None

    runtime_valid_path = os.path.join(output_dir, "runtime_valid.jsonl")
    runtime_invalid_path = os.path.join(output_dir, "runtime_invalid.jsonl")
    manual_review_path = os.path.join(output_dir, "manual_review.jsonl")

    for r in records:
        q = r.get("query")
        if not q:
            continue

        fb = _fallback_generate_api(q, table_name, table_desc, schema_loader)
        if not fb or not fb.get("sql"):
            r["review_status"] = "manual_reject"
        r["review_reason"] = "fallback generation 失败"
        save_jsonl(runtime_invalid_path, r)
        continue

        exec_result = execute_sql(None, fb["sql"])
        intent_pass = False
        try:
            intent_pass = IntentVerifier().verify(q, fb["sql"], exec_result)
        except Exception:
            intent_pass = False

        r["source"] = "invalid_fallback"
        r["sql"] = fb["sql"]
        r["api_name"] = fb["api_schema"].name if fb.get("api_schema") else None
        r["review_status"] = "manual_pass" if intent_pass else "needs_manual"
        r["review_reason"] = fb.get("description")

        if intent_pass:
            save_jsonl_dedup_sql(runtime_valid_path, r)
        else:
            save_jsonl(runtime_invalid_path, r)

    # 同步 manual_review
    if os.path.exists(runtime_invalid_path):
        with open(runtime_invalid_path, "r", encoding="utf8") as fr, open(manual_review_path, "w", encoding="utf8") as fw:
            for line in fr:
                rec = json.loads(line)
                rec.setdefault("review_status", "needs_manual")
                rec.setdefault("reviewer", None)
                rec.setdefault("review_reason", None)
                rec.setdefault("reviewed_at", None)
                fw.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

    return {
        "runtime_valid": runtime_valid_path,
        "runtime_invalid": runtime_invalid_path,
        "manual_review": manual_review_path,
    }
