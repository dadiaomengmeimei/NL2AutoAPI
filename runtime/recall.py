"""
召回模块：从API集合中召回候选
"""
import re
from typing import Optional

from core.llm import call_llm_json
from schema.models import APISchema, RecallCandidate


class APIRecaller:
    """API召回器"""

    def _extract_info_subject(self, query: str) -> Optional[str]:
        text = (query or "").strip()
        patterns = [
            r"查询([\u4e00-\u9fa5A-Za-z0-9_·]{1,30})的信息",
            r"查一下([\u4e00-\u9fa5A-Za-z0-9_·]{1,30})的信息",
            r"([\u4e00-\u9fa5A-Za-z0-9_·]{1,30})的(信息|资料|记录|详情)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return (match.group(1) or "").strip()
        return None

    def _fallback_rank_candidates(self, query: str, apis: list[APISchema], top_k: int) -> list[str]:
        text = (query or "").strip().lower()
        info_subject = self._extract_info_subject(query)
        want_info = any(word in query for word in ["信息", "资料", "记录", "详情"])
        want_count = any(word in query for word in ["多少", "几", "人数", "数量", "总数"])

        scored = []
        for api in apis:
            score = 0.0
            name = (getattr(api, "name", "") or "").lower()
            desc = (getattr(api, "description", "") or "").lower()
            input_schema = getattr(api, "inputSchema", {}) or {}
            props = input_schema.get("properties", {}) if isinstance(input_schema, dict) else {}
            prop_text = " ".join(
                [
                    str(k).lower() + " " + str((v or {}).get("description") or "").lower()
                    for k, v in props.items()
                ]
            )
            full_text = f"{name} {desc} {prop_text}"

            if want_info:
                if "exact_query" in name:
                    score += 3
                if any(k in full_text for k in ["name", "姓名", "display", "formal", "昵称"]):
                    score += 6
                if "count_" in name:
                    score -= 3

            if want_count:
                if "count_" in name or "统计" in desc:
                    score += 4
                if any(k in full_text for k in ["dept", "department", "部门"]):
                    score += 3

            if info_subject and any(k in full_text for k in ["name", "姓名", "display", "formal"]):
                score += 2

            token_hits = sum(1 for token in re.findall(r"[\u4e00-\u9fa5A-Za-z0-9_·]{2,20}", text) if token and token in full_text)
            score += token_hits * 0.5
            scored.append((score, getattr(api, "name", "")))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [name for score, name in scored[:top_k] if score > 0]
    
    def __init__(self, registry, top_k: int = 5):
        self.registry = registry
        self.top_k = top_k
    
    def recall(self, query: str, table_hint: Optional[str] = None) -> list[APISchema]:
        """
        召回候选API
        
        Args:
            query: 用户查询
            table_hint: 表名提示（如果已知）
        
        Returns:
            候选API列表
        """
        # 如果有表提示，只从该表召回
        if table_hint and table_hint in self.registry.get_all_tables():
            candidates = self.registry.get_by_table(table_hint)
            # 如果不多，直接返回
            if len(candidates) <= self.top_k * 2:
                return candidates
            # 否则需要进一步筛选
            return self._filter_in_shard(query, candidates, self.top_k * 2)
        
        # 否则全量分片召回
        return self._recall_by_shards(query)
    
    def _recall_by_shards(self, query: str) -> list[APISchema]:
        """分片召回"""
        all_candidates: dict[str, APISchema] = {}
        
        shards = self.registry.get_shards()
        for i, shard in enumerate(shards):
            print(f"  [Recall] 处理分片 {i+1}/{len(shards)}")
            names = self._recall_in_shard(query, shard, top_k=self.top_k)
            for name in names:
                api = self.registry.get_api_by_name(name)
                if api:
                    all_candidates[name] = api

        return list(all_candidates.values())
    
    def _recall_in_shard(self, query: str, shard: list[APISchema], top_k: int) -> list[str]:
        """单个分片内召回"""
        api_list = [
            {"name": a.name, "description": a.description}
            for a in shard
        ]
        
        prompt = f"""
用户问题: {query}

可用 API 列表:
{__import__('json').dumps(api_list, ensure_ascii=False, indent=2)}

从中选出最多 {top_k} 个最可能回答该问题的 API，按相关度排列。

输出 JSON（仅输出 JSON）:
{{
  "candidates": ["api_name_1", ...]
}}
"""
        result = call_llm_json(prompt)
        names = result.get("candidates", []) if result else []
        if not names:
            names = self._fallback_rank_candidates(query, shard, top_k)
        return names
    
    def _filter_in_shard(self, query: str, apis: list[APISchema], top_k: int) -> list[APISchema]:
        """在单个分片/集合内筛选"""
        names = self._recall_in_shard(query, apis, top_k)
        return [self.registry.get(n) for n in names if self.registry.get(n)]
    
    def select_best(self, query: str, candidates: list[APISchema]) -> Optional[APISchema]:
        """
        从候选中选择最佳API
        
        Args:
            query: 用户查询
            candidates: 候选API列表
        
        Returns:
            最佳API或None
        """
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        
        api_list = [
            {
                "name": a.name,
                "description": a.description,
                "inputSchema": a.inputSchema
            }
            for a in candidates
        ]
        
        prompt = f"""
用户问题: {query}

候选 API:
{__import__('json').dumps(api_list, ensure_ascii=False, indent=2)}

选择最合适的一个 API 回答该问题。

输出 JSON（仅输出 JSON）:
{{
  "selected": "api_name",
  "reason": "简要说明选择原因"
}}
"""
        result = call_llm_json(prompt)
        if not result:
            # 默认选第一个
            return candidates[0]
        
        name = result.get("selected", "")
        # 找到对应的API
        for api in candidates:
            if api.name == name:
                return api
        
        # 没找到，默认第一个
        return candidates[0]