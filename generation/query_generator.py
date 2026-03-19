"""
Query生成器：将SQL转换为自然语言查询
"""
from core.llm import call_llm_json
from generation.query_types import QUERY_TYPES


class QueryGenerator:
    """自然语言查询生成器"""
    
    def __init__(self):
        self.generated_queries: set[str] = set()  # 去重
    
    def generate(self, sql: str, query_type: str, max_retries: int = 3) -> str | None:
        """
        将SQL转换为自然语言问题
        
        Args:
            sql: SQL语句
            query_type: 查询类型
            max_retries: 最大重试次数
        
        Returns:
            生成的查询或None
        """
        qt = QUERY_TYPES[query_type]
        
        prompt = f"""
将以下 SQL 转换为用户自然语言问题。

SQL:
{sql}

查询类型: {getattr(qt, 'description', '')}

要求:
1. 必须是自然语言，不允许出现任何 SQL 关键字（SELECT/WHERE/GROUP BY 等）
2. 语气像真实用户在业务系统中提问
3. slot 参数（如 :dept_id）在问题中体现为"指定的XXX"或"给定的XXX"，
   让读者知道这里需要填入一个具体值
4. 问题必须包含足够的语义线索，使得仅凭这句话就能判断需要调用哪个 API

输出 JSON（仅输出 JSON）:
{{
  "query": "..."
}}
"""
        
        for attempt in range(max_retries):
            result = call_llm_json(prompt)
            if not result:
                continue
            
            query = result.get("query", "").strip()
            if not query:
                continue
            
            # 去重检查
            if query in self.generated_queries:
                print(f"  [Query duplicate] 重复查询，重试")
                continue
            
            # 基础验证：不应包含SQL关键字
            sql_keywords = ["SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", 
                          "HAVING", "JOIN", "INNER", "LEFT", "RIGHT"]
            upper_query = query.upper()
            for kw in sql_keywords:
                if kw in upper_query:
                    print(f"  [Query validate] 包含SQL关键字: {kw}")
                    # 不完全失败，继续尝试
            
            self.generated_queries.add(query)
            return query
        
        return None