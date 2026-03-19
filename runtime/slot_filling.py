"""
填槽模块：从查询中提取参数值
"""
import re

from core.llm import call_llm_json
from core.logger import get_logger
from schema.models import APISchema

logger = get_logger()


class SlotFiller:
    """Slot填充器"""

    def _extract_info_subject(self, query: str) -> str | None:
        text = (query or "").strip()
        if not text:
            return None

        patterns = [
            r"查询([\u4e00-\u9fa5A-Za-z0-9_·]{1,30})的信息",
            r"查一下([\u4e00-\u9fa5A-Za-z0-9_·]{1,30})的信息",
            r"查看([\u4e00-\u9fa5A-Za-z0-9_·]{1,30})的信息",
            r"([\u4e00-\u9fa5A-Za-z0-9_·]{1,30})的(信息|资料|记录|详情)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                value = (match.group(1) or "").strip()
                if value:
                    return value
        return None

    def _extract_dept_subject(self, query: str) -> str | None:
        text = (query or "").strip()
        if not text:
            return None
        patterns = [
            r"([\u4e00-\u9fa5A-Za-z0-9_·]{1,30}部)有多少人",
            r"([\u4e00-\u9fa5A-Za-z0-9_·]{1,30}部)多少人",
            r"统计([\u4e00-\u9fa5A-Za-z0-9_·]{1,30}部)",
            r"([\u4e00-\u9fa5A-Za-z0-9_·]{1,30}部门)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                value = (match.group(1) or "").strip()
                if value:
                    return value
        return None

    def _rule_fill(self, query: str, api: APISchema, result: dict) -> dict:
        _, _, input_schema = self._build_min_api_context(api)
        required = input_schema.get("required", []) or []
        props = input_schema.get("properties", {}) or {}

        info_subject = self._extract_info_subject(query)
        dept_subject = self._extract_dept_subject(query)

        for slot in required:
            current = result.get(slot)
            if current not in [None, ""]:
                continue

            slot_lower = slot.lower()
            desc = str((props.get(slot) or {}).get("description") or "").lower()

            if info_subject and (
                "name" in slot_lower
                or "姓名" in desc
                or "名字" in desc
                or "昵称" in desc
                or "员工" in desc
            ):
                result[slot] = info_subject
                continue

            if dept_subject and (
                "dept" in slot_lower
                or "department" in slot_lower
                or "部门" in desc
            ):
                result[slot] = dept_subject
                continue

        return result

    def _build_min_api_context(self, api: APISchema) -> tuple[str, str, dict]:
        """仅保留填槽所需的最小API上下文。"""
        api_name = getattr(api, "name", "") or ""
        api_desc = getattr(api, "description", "") or ""
        input_schema = getattr(api, "inputSchema", {}) or {}
        if not isinstance(input_schema, dict):
            input_schema = {}
        return api_name, api_desc, input_schema
    
    def fill(self, query: str, api: APISchema) -> dict:
        """
        从查询中提取API所需参数
        
        Args:
            query: 用户查询
            api: API Schema
        
        Returns:
            参数名到值的映射
        """
        api_name, api_desc, input_schema = self._build_min_api_context(api)
        required = input_schema.get("required", [])
        props = input_schema.get("properties", {})
        
        if not required:
            return {}
        
        # 构建参数描述
        param_desc = []
        for param_name in required:
            prop_info = props.get(param_name, {})
            param_type = prop_info.get("type", "string")
            param_description = prop_info.get("description", param_name)
            param_desc.append(f"- {param_name} ({param_type}): {param_description}")
        
        prompt = f"""
用户问题: {query}

    API 名称: {api_name}
    API 描述: {api_desc}

需要提取以下参数:
{chr(10).join(param_desc)}

必填参数: {required}

从用户问题中提取对应的值，找不到的填 null。

输出 JSON（仅输出 JSON）:
{{
  {", ".join(f'"{p}": null' for p in required)}
}}
"""
        result = call_llm_json(prompt)
        if not result:
            logger.warning("SlotFiller: LLM提取返回空或解析失败，required=%s", required)
            # 返回空值
            return {p: None for p in required}
        
        # 确保所有required参数都存在
        for p in required:
            if p not in result:
                result[p] = None

        result = self._rule_fill(query, api, result)
        
        return result
    
    def validate(self, params: dict, api: APISchema) -> tuple[bool, list[str]]:
        """
        验证参数是否完整
        
        Args:
            params: 提取的参数
            api: API Schema
        
        Returns:
            (是否完整, 缺失参数列表)
        """
        required = api.inputSchema.get("required", [])
        missing = [p for p in required if p not in params or params[p] is None]
        return len(missing) == 0, missing