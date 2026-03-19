"""
Schema加载器
"""
import json
from typing import Optional

from .models import DatabaseSchema, TableSchema, FieldInfo


class SchemaLoader:
    """Schema加载器"""
    
    def __init__(self, schema_path: Optional[str] = None):
        self.schema_path = schema_path
        self._schema: Optional[DatabaseSchema] = None
    
    def load(self, path: Optional[str] = None) -> DatabaseSchema:
        """
        从JSON文件加载Schema
        
        Args:
            path: 文件路径，默认使用初始化路径
        """
        path = path or self.schema_path
        if not path:
            raise ValueError("必须提供schema文件路径")
        
        with open(path, "r", encoding="utf8") as f:
            data = json.load(f)
        
        # 兼容旧格式
        if "tables" not in data:
            # 可能是直接的表映射
            data = {"db_id": "default", "tables": data}
        
        # 转换字段格式
        tables = {}
        for table_name, table_info in data.get("tables", {}).items():
            fields = {}
            for field_name, field_info in table_info.get("fields", {}).items():
                if isinstance(field_info, dict):
                    # 兼容两种结构:
                    # 1) {"name": "col", "type": "..."}
                    # 2) "type"结构：{"type": "...", ...}
                    info_copy = field_info.copy()
                    if "name" not in info_copy:
                        info_copy["name"] = field_name
                    fields[field_name] = FieldInfo(**info_copy)
                else:
                    # 兼容简单字符串类型
                    fields[field_name] = FieldInfo(name=field_name, type=str(field_info))
            
            tables[table_name] = TableSchema(
                name=table_name,
                fields=list(fields.values()),
                comment=table_info.get("comment")
            )
        
        self._schema = DatabaseSchema(
            database=data.get("db_id", "default"),
            tables=tables
        )
        
        return self._schema
    
    def get_schema(self) -> DatabaseSchema:
        """获取已加载的Schema"""
        if self._schema is None:
            self.load()
        return self._schema
    
    def get_table_names(self) -> list[str]:
        """获取所有表名"""
        return list(self.get_schema().tables.keys())
    
    def get_table(self, name: str) -> Optional[TableSchema]:
        """获取指定表的Schema"""
        return self.get_schema().tables.get(name)


def load_from_json_file(path: str) -> DatabaseSchema:
    """从JSON文件加载Schema（便捷函数）"""
    loader = SchemaLoader(path)
    return loader.load(path)