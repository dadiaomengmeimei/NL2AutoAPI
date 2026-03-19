"""预热阶段 query 常识过滤 gate。"""

from __future__ import annotations

import json
import os
from collections import Counter

from core.llm import call_llm_json
from core.utils import save_jsonl


class QueryCommonSenseGate:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.rejected_path = os.path.join(output_dir, "query_gate_rejected.jsonl")
        self.rules_path = os.path.join(output_dir, "query_gate_rules.json")

    def _load_reason_counts(self) -> Counter:
        if not os.path.exists(self.rules_path):
            return Counter()
        try:
            with open(self.rules_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return Counter(data if isinstance(data, dict) else {})
        except Exception:
            return Counter()

    def _save_reason_counts(self, counts: Counter):
        with open(self.rules_path, "w", encoding="utf-8") as f:
            json.dump(dict(counts), f, ensure_ascii=False, indent=2)

    def _heuristic_reject_reason(self, query: str) -> str | None:
        q = (query or "").lower()

        # 只基于 query 文本判断，不依赖 SQL/字段存在性
        if any(k in q for k in ["字段长度", "字符长度", "长度分布", "字数分布"]) and not any(
            k in q for k in ["姓名", "地址", "文本质量", "内容长度限制"]
        ):
            return "机器统计偏好：关注字符长度分布"

        if all(k in q for k in ["平均", "最大", "最小", "总和"]) and len(q) > 30:
            return "机器拼接痕迹：一次堆叠过多统计指标"

        if "前100条" in q and any(k in q for k in ["总人数", "总条数"]) and "为什么" not in q:
            return "非自然业务目标：机械限定前N条再做总体统计"

        if any(k in q for k in ["统计一下这个字段", "任意字段", "字段A", "字段B"]):
            return "技术化表达：不像真实业务用户提问"

        return None

    def check(self, query: str, sql: str, table: str, query_type: str) -> tuple[bool, str]:
        heuristic = self._heuristic_reject_reason(query)
        if heuristic:
            return False, heuristic

        reasons = self._load_reason_counts()
        top_reasons = [k for k, _ in reasons.most_common(5)]
        reason_text = "；".join(top_reasons) if top_reasons else "无历史经验"

        prompt = f"""
    你是“用户问法常识过滤器”，只判断 query 是否像真实业务用户会问的问题。
    禁止使用“字段是否存在、SQL是否可执行”作为拒绝依据。

    判定标准（精简）：
    1) 用户视角：有明确业务目标，不是技术指标堆砌
    2) 自然表达：不像机器模板拼接
    3) 可沟通性：产品/运营/管理人员能直接理解

    只输出 JSON: {{"accept": true/false, "reason": "<=20字"}}
    历史不符合经验: {reason_text}
    table: {table}
    query_type: {query_type}
    query: {query}
    """
        result = call_llm_json(prompt)
        if not isinstance(result, dict):
            return True, ""

        accept = bool(result.get("accept", True))
        reason = str(result.get("reason", "")).strip()
        return accept, reason

    # Keywords indicating the query is semantically vague and worth concretizing
    _VAGUE_KEYWORDS = ["语义模糊", "模糊", "未指定", "不具体", "缺少具体", "抽象", "vague", "ambiguous"]

    def _is_vague_rejection(self, reason: str) -> bool:
        """Check if the rejection reason indicates a vague/abstract query."""
        r = (reason or "").lower()
        return any(k in r for k in self._VAGUE_KEYWORDS)

    def concretize_query(self, query: str, sql: str, table: str, sample_hints: str = "") -> str | None:
        """Use LLM to concretize an abstract query into a specific one."""
        prompt = f"""
你是数据标注助手。请把下面的抽象query改写成一个更具体、可执行的用户query。
要求：
1. 保持原始意图不变；
2. 将"指定/某/给定"等占位词替换为合理的具体值；
3. 输出一个具体query，不要解释；
4. 不要生成SQL。

表名: {table}
当前query: {query}
当前SQL(如有): {sql}
{f"样例值: {sample_hints}" if sample_hints else ""}

仅输出JSON: {{"query": "..."}}
""".strip()
        result = call_llm_json(prompt)
        if isinstance(result, dict) and isinstance(result.get("query"), str):
            concrete = result["query"].strip()
            if concrete and concrete != query:
                return concrete
        return None

    def check_with_concretize(
        self, query: str, sql: str, table: str, query_type: str,
        sample_hints: str = "",
    ) -> tuple[bool, str, str]:
        """
        Check query through GATE; if rejected due to vagueness, auto-concretize and retry.

        Returns:
            (accept, reason, final_query) — final_query may differ from input if concretized.
        """
        accept, reason = self.check(query, sql, table, query_type)
        if accept:
            return True, reason, query

        # If rejection is due to vagueness, try concretizing
        if self._is_vague_rejection(reason):
            concrete = self.concretize_query(query, sql, table, sample_hints)
            if concrete:
                print(f"  [GATE] Vague query detected, concretized: {query[:40]}... → {concrete[:40]}...")
                accept2, reason2 = self.check(concrete, sql, table, query_type)
                if accept2:
                    return True, reason2, concrete
                # Still rejected after concretize
                return False, reason2, concrete

        return False, reason, query

    def reject(self, query: str, sql: str, table: str, query_type: str, layer_tag: str, reason: str):
        save_jsonl(self.rejected_path, {
            "query": query,
            "sql": sql,
            "table": table,
            "query_type": query_type,
            "layer_tag": layer_tag,
            "reason": reason,
        })
        counts = self._load_reason_counts()
        counts[reason or "其他"] += 1
        self._save_reason_counts(counts)
