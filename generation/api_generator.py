"""
API Schema生成器
"""
import re
from core.llm import call_llm_json
from core.utils import extract_slots
from generation.query_types import QUERY_TYPES
from schema.models import APISchema


class APIGenerator:
    """API Schema生成器"""
    
    def __init__(self):
        self.name_counter: dict[str, int] = {}  # 名称冲突计数
    
    def generate(
        self,
        sql: str,
        query: str,
        query_type: str,
        ensure_unique: bool = True
    ) -> APISchema | None:
        """
        生成API Schema
        
        Args:
            sql: SQL语句
            query: 自然语言查询
            query_type: 查询类型
            ensure_unique: 是否确保名称唯一
        
        Returns:
            API Schema或None
        """
        slots = extract_slots(sql)
        qt = QUERY_TYPES[query_type]
        
        prompt = f"""
根据 SQL 和用户问题，生成一个 MCP Tool Schema。

SQL:
{sql}

用户问题:
{query}

查询类型: {getattr(qt, 'description', '')}
SQL 中的 slot 参数: {slots}

规则:
1. SQL 中每个 :param_name 必须成为 inputSchema 的参数，名称完全一致
2. 根据参数语义推断 type（string / integer / number）和中文 description
3. 如果没有 slot，properties 为空对象，required 为空数组
4. name 用英文小写下划线，description 用中文简洁描述 API 功能
5. description 要足够准确，使得仅凭 description 就能判断该 API 回答什么问题

输出 JSON（仅输出 JSON）:
{{
  "api_schema": {{
    "name": "...",
    "description": "...",
    "inputSchema": {{
      "type": "object",
      "properties": {{}},
      "required": []
    }}
  }}
}}
"""
        
        result = call_llm_json(prompt)
        if not result or "api_schema" not in result:
            return None
        
        api_data = result["api_schema"]
        
        # 确保名称唯一
        base_name = api_data.get("name", "unnamed_api")
        if ensure_unique:
            name = self._ensure_unique_name(base_name)
        else:
            name = base_name
        
        # 提取主表名称，兼容简单SQL
        table_match = re.search(r"FROM\s+[`'\"]?(\w+)[`'\"]?", sql, flags=re.IGNORECASE)
        table_name = table_match.group(1) if table_match else "unknown"

        # 兼容描述调整：对“非技术同学”友好
        raw_desc = api_data.get("description", "") or ""
        if not raw_desc or len(raw_desc.strip()) < 6:
            query_brief = query.strip().strip("。?？").replace(";", "，")
            if query_brief:
                raw_desc = f"{query_brief}。"
            else:
                raw_desc = f"执行SQL查询：{sql.split('LIMIT')[0]}"

        api_schema = APISchema(
            name=name,
            description=raw_desc,
            inputSchema=api_data.get("inputSchema", {"type": "object", "properties": {}, "required": []}),
            bound_sql=sql,
            slot_mapping={s: s for s in slots},
            query_type=query_type,
            table=table_name,
            source="generated"
        )
        
        return api_schema

    def generate_from_sql(
        self,
        sql: str,
        query_type: str,
        table: str,
        fields: list[dict],
        ensure_unique: bool = True
    ) -> APISchema:
        """从SQL和表结构直接构造API Schema（兼容测试）"""
        product_name = self._generate_api_name(table, [f['name'] for f in fields if f.get('type')], query_type, None)
        if ensure_unique:
            product_name = self._ensure_unique_name(product_name)

        slots = extract_slots(sql)

        return APISchema(
            name=product_name,
            description=f"从SQL生成: {query_type}",
            inputSchema={
                "type": "object",
                "properties": {s: {"type": "string", "description": s} for s in slots},
                "required": slots
            },
            outputSchema={"type": "array", "items": {"type": "object"}},
            bound_sql=sql,
            slot_mapping={s: s for s in slots},
            query_type=query_type,
            table=table,
            examples=[{"query": "示例", "params": {s: "值" for s in slots}}],
            source="generated"
        )

    def _generate_api_name(self, table: str, slots: list[str], query_type: str, desc_hint: str | None = None) -> str:
        """兼容旧测试的API命名生成器"""
        # 简化命名规则
        name_base = table
        if query_type.startswith("aggregate"):
            name_base = f"{name_base}_count"
        elif query_type.startswith("list"):
            name_base = f"{name_base}_list"
        elif query_type.startswith("exact"):
            name_base = f"{name_base}_get"
        else:
            name_base = f"{name_base}_query"

        if slots:
            name_base += "_by_" + "_".join(slots)

        if desc_hint:
            safe_hint = desc_hint.replace(" ", "_").replace("/", "_")
            name_base += f"_{safe_hint}"

        name_base = re.sub(r"_+", "_", name_base).strip("_")

        return name_base

    def _ensure_unique_name(self, base_name: str) -> str:
        """确保名称唯一"""
        if base_name not in self.name_counter:
            self.name_counter[base_name] = 0
            return base_name
        
        self.name_counter[base_name] += 1
        return f"{base_name}_{self.name_counter[base_name]}"
    
    def generate_from_runtime(
        self,
        query: str,
        sql: str,
        table_name: str,
        description: str = ""
    ) -> APISchema | None:
        """
        从运行时场景生成API Schema（用于纠错）
        
        Args:
            query: 用户查询
            sql: 正确的SQL
            table_name: 表名
            description: 可选描述
        
        Returns:
            API Schema或None
        """
        slots = extract_slots(sql)
        
        prompt = f"""
根据用户查询和正确的SQL，生成API Schema。

用户查询: {query}
对应表: {table_name}
SQL: {sql}
Slot参数: {slots}
{"功能描述: " + description if description else ""}

请生成符合以下格式的API Schema：
- name: 使用 {table_name}_ 前缀，描述查询功能
- description: 准确描述这个API能解决什么问题
- inputSchema: 包含所有slot参数

输出 JSON:
{{
  "api_schema": {{
    "name": "{table_name}_...",
    "description": "...",
    "inputSchema": {{
      "type": "object",
      "properties": {{...}},
      "required": [...]
    }}
  }}
}}
"""
        
        result = call_llm_json(prompt)
        if not result or "api_schema" not in result:
            # 使用默认生成
            return self._default_runtime_schema(query, sql, table_name, slots)
        
        api_data = result["api_schema"]
        
        # 确保有表名前缀
        name = api_data.get("name", "unnamed")
        if not name.startswith(f"{table_name}_"):
            name = f"{table_name}_{name}"
        
        # 确保唯一
        name = self._ensure_unique_name(name)
        
        return APISchema(
            name=name,
            description=api_data.get("description", f"查询{table_name}数据"),
            inputSchema=api_data.get("inputSchema", {"type": "object", "properties": {}, "required": []}),
            bound_sql=sql,
            slot_mapping={s: s for s in slots},
            query_type="runtime_generated",
            source="runtime_generated",
            table=table_name,
        )
    
    def _default_runtime_schema(
        self,
        query: str,
        sql: str,
        table_name: str,
        slots: list[str]
    ) -> APISchema:
        """默认运行时Schema"""
        # 构建基本参数
        properties = {}
        for slot in slots:
            properties[slot] = {
                "type": "string",
                "description": f"参数 {slot}"
            }
        
        name = self._ensure_unique_name(f"{table_name}_query")
        
        return APISchema(
            name=name,
            description=f"处理查询: {query[:50]}...",
            inputSchema={
                "type": "object",
                "properties": properties,
                "required": slots
            },
            bound_sql=sql,
            slot_mapping={s: s for s in slots},
            query_type="runtime_generated",
            source="runtime_generated",
            table=table_name,
        )