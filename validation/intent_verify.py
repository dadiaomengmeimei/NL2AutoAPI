"""
意图验证：验证SQL是否能正确回答用户问题
"""
import json
from core.llm import call_llm_json


class IntentVerifier:
    """意图验证器"""
    
    def verify(self, query: str, sql: str, exec_result: dict) -> bool:
        """
        验证SQL结构是否能正确回答用户问题
        
        Args:
            query: 用户问题
            sql: SQL语句
            exec_result: 执行结果
        
        Returns:
            是否通过验证
        """
        exec_summary = {
            "status": exec_result.get("status"),
            "error": exec_result.get("error"),
            "columns": exec_result.get("columns"),
        }
        exec_result_str = json.dumps(exec_summary, ensure_ascii=False, indent=2, default=str)

        prompt = f"""
用户问题: {query}
执行的 SQL: {sql}
    SQL执行摘要:
{exec_result_str}

    判断：SQL 的结构、筛选条件、聚合方式、返回列，是否能正确回答用户问题？
    注意：
    1. 不要根据结果是否有数据来判断对错；0条结果也可以是正确SQL。
    2. 只要SQL可执行，且语义结构与用户问题一致，就应判定通过。
    3. 重点看 query 与 sql 的语义匹配，而不是返回了什么具体值。

输出 JSON（仅输出 JSON）:
{{
  "pass": true,
  "reason": "..."
}}
"""
        result = call_llm_json(prompt)
        return bool(result.get("pass", False)) if result else False