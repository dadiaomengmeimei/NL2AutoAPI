"""
Schema采样器
"""

import random
from typing import Tuple

from schema.loader import SchemaLoader
from generation.query_types import QUERY_TYPES


class SchemaSampler:
    """Schema采样器"""
    
    def __init__(self, schema: SchemaLoader):
        self.schema = schema
    
    def sample_for_query_type(self, query_type: str) -> Tuple[str, dict]:
        """
        为指定查询类型采样Schema子集
        
        Returns:
            (表名, 子集Schema字典)
        """
        qt = QUERY_TYPES[query_type]
        tables = self.schema.get_schema().tables
        
        # 随机选择表
        table_name = random.choice(list(tables.keys()))
        table_info = tables[table_name]
        fields = table_info.fields

        # 支持fields是list或dict
        if isinstance(fields, dict):
            field_map = fields
        else:
            # 由TableSchema中的FieldInfo列表转换为映射
            field_map = {f.name: f for f in fields}

        # 根据need_fields决定是否包含字段
        if not qt.need_fields:
            subset_fields = {}
        else:
            # 随机采样2-4个字段
            field_names = list(field_map.keys())
            n = min(random.randint(2, 4), len(field_names))
            selected = random.sample(field_names, n)
            subset_fields = {
                k: {
                    "type": field_map[k].type,
                    "comment": field_map[k].comment
                }
                for k in selected
            }
        
        # 兼容 SchemaLoader 和 DatabaseSchema
        db_schema = self.schema.get_schema() if hasattr(self.schema, "get_schema") else self.schema
        db_id = getattr(db_schema, "database", None) or getattr(db_schema, "db_id", "default")

        # 构建子集Schema
        schema_subset = {
            "db_id": db_id,
            "tables": {
                table_name: {
                    "fields": subset_fields,
                    "comment": table_info.comment
                }
            }
        }
        
        return table_name, schema_subset
    
    def sample_fields(self, table_name: str, num_fields: int = 3) -> dict:
        """从指定表采样字段"""
        table = self.schema.get_table(table_name)
        if not table:
            return {}
        
        field_names = list(table.fields.keys())
        selected = random.sample(field_names, min(num_fields, len(field_names)))
        
        return {
            k: {
                "type": table.fields[k].type,
                "comment": table.fields[k].comment
            }
            for k in selected
        }