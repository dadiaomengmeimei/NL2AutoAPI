"""
往返验证：确保query→api→sql方向通畅
"""

from typing import Optional

from core.llm import call_llm_json
from core.database import execute_sql
from core.utils import fill_sql_with_values
from core.logger import get_logger
from schema.models import APISchema

logger = get_logger()


class RoundTripChecker:
    """往返验证器"""
    
    def __init__(self, db_conn):
        self.db_conn = db_conn
    
    def check(
        self,
        query: str,
        api_gt: APISchema,
        api_pool: list[APISchema]
    ) -> bool:
        """
        完整round-trip验证
        
        Args:
            query: 用户查询
            api_gt: 正确的API（ground truth）
            api_pool: 候选API池
        
        Returns:
            是否通过验证
        """
        # 1. 粗召回
        candidates = self._recall_candidates(query, api_pool, top_k=5)
        if not candidates:
            logger.warning("RoundTrip: 召回为空")
            return False
        
        # 2. 精选
        best = self._select_best(query, candidates)
        if not best:
            logger.warning("RoundTrip: 精选失败")
            return False
        
        # 3. API命中校验
        if best.name != api_gt.name:
            logger.warning("RoundTrip: API不匹配, 召回=%s 预期=%s", best.name, api_gt.name)
            return False
        
        # 4. LLM填槽
        params = self._extract_params(query, best)
        slot_mapping = best.slot_mapping
        missing = [s for s in slot_mapping if not params.get(s)]
        if missing:
            logger.warning("RoundTrip: 填槽缺失: %s", missing)
            return False
        
        # 5. SQL执行
        exec_sql = fill_sql_with_values(best.bound_sql, params)
        exec_result = execute_sql(self.db_conn, exec_sql)
        
        if exec_result["status"] != "success":
            logger.error("RoundTrip: SQL 执行失败: %s", exec_result.get('error'))
            return False
        
        logger.info("RoundTrip ✓ 通过 (rows=%s)", exec_result['row_count'])
        return True
    
    def _recall_candidates(
        self,
        query: str,
        apis: list[APISchema],
        top_k: int
    ) -> list[APISchema]:
        """粗召回候选API"""
        if not apis:
            return []
        
        api_list = [{"name": a.name, "description": a.description} for a in apis]
        
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
        r = call_llm_json(prompt)
        names = r.get("candidates", []) if r else []
        idx = {a.name: a for a in apis}
        return [idx[n] for n in names if n in idx]
    
    def _select_best(
        self,
        query: str,
        candidates: list[APISchema]
    ) -> Optional[APISchema]:
        """从候选中选择最佳API"""
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        
        prompt = f"""
用户问题: {query}

候选 API:
{__import__('json').dumps([{"name": a.name, "description": a.description, "inputSchema": a.inputSchema} for a in candidates], ensure_ascii=False, indent=2)}

选择最合适的一个 API 回答该问题。

输出 JSON（仅输出 JSON）:
{{
  "selected": "api_name"
}}
"""
        r = call_llm_json(prompt)
        name = r.get("selected", "") if r else ""
        idx = {a.name: a for a in candidates}
        return idx.get(name, candidates[0])
    
    def _extract_params(self, query: str, api: APISchema) -> dict:
        """从query中提取API参数"""
        required = api.inputSchema.get("required", [])
        props = api.inputSchema.get("properties", {})
        if not required:
            return {}
        
        prompt = f"""
用户问题: {query}

需要提取以下参数:
{__import__('json').dumps(props, ensure_ascii=False, indent=2)}

必填参数: {required}

从用户问题中提取对应的值，找不到的填 null。

输出 JSON（仅输出 JSON）:
{{
  {", ".join(f'"{p}": null' for p in required)}
}}
"""
        r = call_llm_json(prompt)
        return r if r else {}