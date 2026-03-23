"""
SQL生成器
"""
import json
import random
from typing import Optional

from core.llm import call_llm_json
from core.utils import extract_slots
from core.logger import get_logger
from generation.query_types import QUERY_TYPES
from schema.models import DatabaseSchema

logger = get_logger()


def build_sql_prompt(table: dict, query_type: str, selected_fields: list[str] = None, slot_fields: list[str] = None) -> str:
    """构建SQL生成器提示词（供测试使用）"""
    selected_fields = selected_fields or []
    slot_fields = slot_fields or []
    qt = QUERY_TYPES.get(query_type)
    need_fields = getattr(qt, 'need_fields', False) if qt else False
    slot_required = getattr(qt, 'slot_required', False) if qt else False

    field_names = ', '.join([f['name'] for f in table.get('fields', [])])
    slot_names = ', '.join(slot_fields)

    prompt = f"""
【强制约束】
query_type: {query_type}
need_fields: {need_fields}
slot_required: {slot_required}
selected_fields: {selected_fields}
slot_fields: {slot_fields}
表名: {table.get('name')}
字段: {field_names}

请按上述规则输出JSON:
{{
  "sql": "..."
}}
"""
    return prompt


class SQLGenerator:
    """SQL生成器"""
    
    def __init__(self):
        self.generated_sqls: set[str] = set()  # 去重
    
    def generate(
        self,
        table_name: str,
        schema_subset: dict,
        query_type: str,
        max_retries: int = 2
    ) -> Optional[str]:
        """
        生成SQL
        
        Args:
            table_name: 表名
            schema_subset: 子集Schema
            query_type: 查询类型
            max_retries: 最大重试次数
        
        Returns:
            生成的SQL或None
        """
        qt = QUERY_TYPES[query_type]
        has_fields = bool(schema_subset["tables"][table_name]["fields"])
        
        # 构建字段区域
        field_section = self._build_field_section(
            has_fields, query_type, schema_subset, table_name
        )
        
        # 构建规则
        col_rule = self._build_col_rule(qt)
        slot_rule = self._build_slot_rule(qt)
        extra_rules = self._build_extra_rules(query_type, qt)
        
        prompt = self._build_prompt(
            table_name, field_section, qt, col_rule, slot_rule, extra_rules
        )
        
        # 尝试生成，带重试
        for attempt in range(max_retries):
            result = call_llm_json(prompt)
            if not result:
                continue
            
            sql = result.get("sql", "").strip()
            if not sql:
                continue
            
            # 验证生成的SQL
            if self._validate_sql(sql, query_type, table_name):
                # 去重检查
                sql_normalized = " ".join(sql.split())
                if sql_normalized in self.generated_sqls:
                    logger.warning("SQL duplicate, 重复SQL，重试")
                    continue
                
                self.generated_sqls.add(sql_normalized)
                return sql
        
        return None
    
    def _build_field_section(
        self,
        has_fields: bool,
        query_type: str,
        schema_subset: dict,
        table_name: str
    ) -> str:
        """构建字段信息区域"""
        if not has_fields:
            return (
                "字段信息: 本次查询只需统计整张表，无需使用任何具体字段。\n"
                "严格禁止: 生成任何 WHERE 条件 / slot 占位符（:param）/ 字段引用"
            )
        
        return (
            "可用字段:\n"
            + json.dumps(
                schema_subset["tables"][table_name]["fields"],
                ensure_ascii=False, indent=2
            )
        )
    
    def _build_col_rule(self, qt) -> str:
        """构建SELECT列规则"""
        if getattr(qt, 'allow_select_cols', False):
            return "SELECT 具体业务列（可附加聚合值）"
        return "SELECT 只使用聚合函数（COUNT / AVG / MAX / MIN / SUM），不要裸列名"
    
    def _build_slot_rule(self, qt) -> str:
        """构建slot规则"""
        if getattr(qt, 'slot_required', False):
            return (
                "WHERE 条件中的过滤参数必须使用 slot 占位符：:param_name\n"
                "  - slot 名称与列名保持一致，例如 WHERE dept_id = :dept_id\n"
                "  - 支持范围条件，例如 WHERE hire_date >= :start_date AND hire_date <= :end_date"
            )
        return "不需要 WHERE 过滤条件（GROUP BY / ORDER BY 正常写）"
    
    def _build_extra_rules(self, query_type: str, qt: dict) -> str:
        """构建额外规则"""
        extra = []
        
        if "topn" in query_type:
            extra.append("必须包含 ORDER BY 和 LIMIT（LIMIT 建议 5~20）")
        if "group" in query_type:
            extra.append("必须包含 GROUP BY")
        if getattr(qt, 'allow_select_cols', False) and not getattr(qt, 'slot_required', False) and "topn" not in query_type:
            extra.append("必须加 LIMIT（建议 20~100），防止全表扫描")
        
        return "\n".join(f"- {r}" for r in extra)
    
    def _build_prompt(
        self,
        table_name: str,
        field_section: str,
        qt,
        col_rule: str,
        slot_rule: str,
        extra_rules: str
    ) -> str:
        """构建完整提示词"""
        query_desc = getattr(qt, 'description', '')
        examples = getattr(qt, 'examples', [])
        return f"""
你是 SQL 专家。请根据以下信息生成一条 SQL 查询。

表名: {table_name}
{field_section}

查询类型: {query_desc}
参考场景: {examples}

生成规则:
- 只允许操作表 {table_name}
- {col_rule}
- {slot_rule}
{extra_rules}
- 禁止使用: OR / subquery / JOIN / CASE WHEN

输出 JSON（仅输出 JSON，不要任何说明）:
{{
  "sql": "..."
}}
"""
    
    def _validate_sql(self, sql: str, query_type: str, table_name: str) -> bool:
        """验证生成的SQL"""
        # 基础检查
        if not sql.upper().startswith("SELECT"):
            logger.error("SQL validate: 不是SELECT语句")
            return False
        
        # 检查是否操作了其他表
        # 简单检查：不应该出现其他表名（实际应该用SQL解析器）
        
        # 检查query_type约束
        qt = QUERY_TYPES[query_type]
        
        # need_fields=False时不应有slot
        if not getattr(qt, 'need_fields', False):
            slots = extract_slots(sql)
            if slots:
                logger.warning("SQL validate: %s 不应有 slot，但发现%s", query_type, slots)
                return False
        
        # slot_required=True时必须有slot
        if getattr(qt, 'slot_required', False):
            slots = extract_slots(sql)
            if not slots:
                logger.warning("SQL validate: %s 需要有slot，但未发现", query_type)
                # 不完全失败，可能LLM用了字面量
                if "WHERE" in sql.upper():
                    logger.warning("SQL validate: 警告：有WHERE但无slot")
        
        return True