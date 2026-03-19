"""
Schema扩展器：基于扩写查询生成新Schema
"""

from typing import Optional

from core.llm import call_llm_json
from core.database import db_manager
from schema.models import APISchema
from generation.api_generator import APIGenerator
from review.submitter import ReviewSubmitter


class SchemaExpander:
    """Schema扩展器（事后优化）"""
    
    def __init__(
        self,
        api_generator: Optional[APIGenerator] = None,
        submitter: Optional[ReviewSubmitter] = None
    ):
        self.api_generator = api_generator or APIGenerator()
        self.submitter = submitter or ReviewSubmitter()
    
    def expand_from_case(
        self,
        original_query: str,
        base_api: APISchema,
        augmented_queries: list[str],
        auto_submit: bool = True
    ) -> list[APISchema]:
        """
        基于扩写查询生成Schema扩展
        
        Args:
            original_query: 原始查询
            base_api: 基础API
            augmented_queries: 扩写的查询列表
            auto_submit: 是否自动提交审核
        
        Returns:
            生成的Schema列表
        """
        generated_schemas = []
        
        for aug_query in augmented_queries:
            # 为每个扩写查询生成对应的SQL
            sql = self._generate_sql_for_query(
                aug_query, base_api, original_query
            )
            if not sql:
                continue
            
            # 验证SQL可执行
            test_result = db_manager.execute(sql.replace(r':\w+', "'test'"))  # 简单替换测试
            if test_result.get("status") != "success":
                print(f"  [Expander] SQL测试失败: {aug_query[:50]}...")
                continue
            
            # 生成API Schema
            table_name = self._extract_table_from_api(base_api)
            new_api = self.api_generator.generate_from_runtime(
                query=aug_query,
                sql=sql,
                table_name=table_name,
                description=f"从'{original_query}'扩写生成"
            )
            
            if new_api:
                generated_schemas.append(new_api)
        
        # 提交审核
        if auto_submit and generated_schemas:
            self.submitter.submit_schema_expansion(
                original_query=original_query,
                expanded_queries=augmented_queries,
                base_api=base_api.dict(),
                generated_schemas=[s.dict() for s in generated_schemas]
            )
        
        return generated_schemas
    
    def _generate_sql_for_query(
        self,
        query: str,
        base_api: APISchema,
        original_query: str
    ) -> Optional[str]:
        """为扩写查询生成SQL"""
        prompt = f"""
基于原查询和基础API，为新查询生成SQL。

原查询: {original_query}
新查询: {query}

基础API信息:
- 名称: {base_api.name}
- SQL: {base_api.bound_sql}
- 参数: {base_api.slot_mapping}

要求：
- 保持与基础API相似的查询模式
- 根据新查询的语义调整WHERE条件或聚合方式
- 使用slot参数表示可变部分

输出JSON:
{{
  "sql": "生成的SQL",
  "reasoning": "调整说明"
}}
"""
        result = call_llm_json(prompt)
        return result.get("sql") if result else None
    
    def _extract_table_from_api(self, api: APISchema) -> str:
        """从API信息中提取表名"""
        # 尝试从name解析
        if "_" in api.name:
            return api.name.split("_")[0]
        
        # 尝试从SQL解析
        import re
        if api.bound_sql:
            match = re.search(r'FROM\s+`?(\w+)`?', api.bound_sql, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return "unknown"