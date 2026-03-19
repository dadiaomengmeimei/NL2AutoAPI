"""
LLM评判器：详细验证结果质量
"""
import json
from typing import Tuple

from core.llm import call_llm
from schema.models import VerificationResult, VerificationType


class LLMJudge:
    """LLM评判器"""

    def _normalize_verification_type(self, raw_type) -> VerificationType:
        """兼容LLM返回的大小写/别名，统一映射为VerificationType"""
        if isinstance(raw_type, VerificationType):
            return raw_type

        value = str(raw_type or "INCORRECT").strip().upper()
        alias = {
            "CORRECT": "CORRECT",
            "PASS": "CORRECT",
            "TRUE": "CORRECT",
            "PARTIAL": "PARTIAL",
            "PARTIALLY_CORRECT": "PARTIAL",
            "INCORRECT": "INCORRECT",
            "WRONG": "INCORRECT",
            "FALSE": "INCORRECT",
            "ERROR": "INCORRECT",
        }
        normalized = alias.get(value, "INCORRECT")
        return VerificationType(normalized)
    
    def judge(
        self,
        user_query: str,
        sql: str,
        results: dict
    ) -> VerificationResult:
        """
        评判SQL执行结果
        
        Args:
            user_query: 用户查询
            sql: 执行的SQL
            results: 执行结果
        
        Returns:
            验证结果
        """
        prompt = f"""
你是一个数据验证专家。
用户查询: {user_query}
生成的 SQL: {sql}
SQL执行结果: {json.dumps(results, ensure_ascii=False, default=str)}

请诊断类型，仅从以下枚举中选择，并简要说明原因。
    - "CORRECT": 完全正确，SQL 正确实现了用户查询意图
    - "PARTIAL": 部分正确（如字段匹配但逻辑不完整，或结果缺少部分信息）
    - "INCORRECT": SQL存在严重错误或结果和预期结果完全不匹配

输出 JSON 格式:
{{
    "reason": "原因",
    "type": "以上英文枚举值",
    "confidence": 0.95
}}
"""
        for retry in range(3):
            response = call_llm(prompt)
            try:
                # 清理可能的markdown
                cleaned = response.replace("```json", "").replace("```", "").strip()
                result_json = json.loads(cleaned)
                normalized_type = self._normalize_verification_type(result_json.get("type", "INCORRECT"))
                confidence = float(result_json.get("confidence", 0.0) or 0.0)
                
                return VerificationResult(
                    type=normalized_type,
                    reason=result_json.get("reason", ""),
                    confidence=confidence
                )
            except Exception as e:
                print(f"  [LLM Judge] 解析失败 (attempt {retry+1}): {e}")
                continue
        
        # 默认返回INCORRECT
        return VerificationResult(
            type="INCORRECT",
            reason="LLM评判解析失败",
            confidence=0.0
        )

    def _build_judge_prompt(self, user_query: str, executed_sql: str, query_result: dict) -> str:
        """构建评判Prompt（测试用）"""
        return f"""
你是一个数据验证专家。
用户查询: {user_query}
生成的 SQL: {executed_sql}
SQL执行结果: {json.dumps(query_result, ensure_ascii=False, default=str)}

请诊断类型：
- CORRECT: 结果完全回答了用户问题
- PARTIAL: 结果部分回答了问题
- INCORRECT: 结果完全不符合用户问题

输出 JSON:
{{"type": "...", "reason": "...", "confidence": 0.95}}
"""

    def _parse_judge_result(self, response: str) -> VerificationResult:
        """解析LLM评判输出"""
        try:
            cleaned = response.replace("```json", "").replace("```", "").strip()
            data = json.loads(cleaned)
            vtype = self._normalize_verification_type(data.get("type", "INCORRECT"))

            return VerificationResult(
                type=vtype,
                reason=data.get("reason", ""),
                confidence=float(data.get("confidence", 0.0) or 0.0)
            )
        except Exception:
            return VerificationResult(
                type=VerificationType.INCORRECT,
                reason="解析失败",
                confidence=0.0
            )

    def is_acceptable(self, result: VerificationResult, 
                     accept_partial: bool = True) -> bool:
        """判断是否可接受"""
        if result.type == "CORRECT":
            return True
        if accept_partial and result.type == "PARTIAL":
            return result.confidence >= 0.6
        return False