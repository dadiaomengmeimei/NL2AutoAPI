"""
导出工具：按表名分文件导出Schema
"""

import json
import os
from typing import Optional, List

from core.utils import load_jsonl, sanitize_filename


class SchemaExporter:
    """Schema导出器"""
    
    def __init__(self, output_dir: str = "./output/schemas_by_table"):
        self.output_dir = output_dir
    
def export(self, valid_path: str, format: str = "json", allowed_tables: Optional[List[str]] = None):
        """
        按表名导出Schema
        
        Args:
            valid_path: valid数据集路径
            format: 输出格式 (json, openapi, mcp)
            allowed_tables: 限定输出的表名，None则全量
        """
        os.makedirs(self.output_dir, exist_ok=True)
        
        records = load_jsonl(valid_path)
        allowed = set(allowed_tables or [])
        
        # 按表分组
        by_table: dict[str, list[dict]] = {}
        
        for r in records:
            api = r.get("api_schema", r)
            table_name = self._extract_table_name(api, r)
            
            if table_name not in by_table:
                by_table[table_name] = []
            
            by_table[table_name].append({
                "api_schema": api,
                "query": r.get("query", ""),
                "query_type": r.get("query_type", api.get("query_type", "unknown")),
            })
        
        # 导出每个表，使用独立目录（table_name）隔离多表场景
        for table_name, apis in by_table.items():
            if allowed and table_name not in allowed:
                continue

            safe_name = sanitize_filename(table_name)
            table_dir = os.path.join(self.output_dir, safe_name)
            os.makedirs(table_dir, exist_ok=True)

            if format == "json":
                self._export_json(table_name, safe_name, apis, output_dir=table_dir)
            elif format == "openapi":
                self._export_openapi(table_name, safe_name, apis, output_dir=table_dir)
            elif format == "mcp":
                self._export_mcp(table_name, safe_name, apis, output_dir=table_dir)

        print(f"[Export] 导出完成: {len(by_table)} 个表, {len(records)} 个API")

        # 生成汇总文件
        self._generate_index(by_table)
    
    def _extract_table_name(self, api: dict, record: dict) -> str:
        """提取表名"""
        # 优先直接使用table字段（Schema中真实字段）
        table_attr = api.get("table")
        if table_attr:
            return table_attr

        # 尝试从SQL解析
        sql = api.get("bound_sql", record.get("sql", ""))
        import re
        match = re.search(r'FROM\s+[`\'"]?(\w+)[`\'"]?', sql, re.IGNORECASE)
        if match:
            return match.group(1)

        # 尝试从API name解析
        name = api.get("name", "")
        if "_" in name:
            parts = name.split("_")
            if len(parts) >= 2 and parts[0] in ["get", "query", "count", "list", "group", "exact", "table"]:
                return parts[1] if len(parts) > 1 else "default"
            return parts[0]

        # 尝试从record中的schema提取
        schema = record.get("schema", {})
        tables = schema.get("tables", {})
        if tables:
            return list(tables.keys())[0]

        return "default"
        import re
        match = re.search(r'FROM\s+`?(\w+)`?', sql, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # 尝试从record中的schema提取
        schema = record.get("schema", {})
        tables = schema.get("tables", {})
        if tables:
            return list(tables.keys())[0]
        
        return "default"
    
    def _export_json(self, table_name: str, safe_name: str, apis: list[dict], output_dir: str):
        """导出为JSON格式"""
        unique_api_map = {}
        examples = []
        for a in apis:
            api = a["api_schema"]
            key = (api.get("name", ""), api.get("bound_sql", ""))
            unique_api_map[key] = api
            if a.get("query"):
                examples.append({
                    "query": a["query"],
                    "api_name": api.get("name", "")
                })

        output = {
            "table_name": table_name,
            "api_count": len(unique_api_map),
            "apis": list(unique_api_map.values()),
            "examples": examples[:10]
        }

        path = os.path.join(output_dir, f"{safe_name}.json")
        with open(path, "w", encoding="utf8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        print(f"  [Export] {table_name} -> {path}")
    
    def _export_openapi(self, table_name: str, safe_name: str, apis: list[dict], output_dir: str):
        """导出为OpenAPI格式"""
        paths = {}
        for a in apis:
            api = a["api_schema"]
            # 构建OpenAPI路径
            path_key = f"/{api['name'].replace('_', '/')}"
            paths[path_key] = {
                "post": {
                    "summary": api["description"],
                    "operationId": api["name"],
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": api.get("inputSchema", {})
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Success",
                            "content": {
                                "application/json": {
                                    "schema": {"type": "object"}
                                }
                            }
                        }
                    }
                }
            }
        
        output = {
            "openapi": "3.0.0",
            "info": {
                "title": f"{table_name} API",
                "version": "1.0.0"
            },
            "paths": paths
        }
        
        path = os.path.join(output_dir, f"{safe_name}_openapi.json")
        with open(path, "w", encoding="utf8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
    
    def _export_mcp(self, table_name: str, safe_name: str, apis: list[dict], output_dir: str):
        """导出为MCP (Model Context Protocol) 格式"""
        tools = []
        for a in apis:
            api = a["api_schema"]
            tools.append({
                "name": api["name"],
                "description": api["description"],
                "inputSchema": api.get("inputSchema", {}),
                "_meta": {
                    "bound_sql": api.get("bound_sql"),
"slot_mapping": api.get("slot_mapping", {}),
                    "query_type": api.get("query_type", "unknown"),
                    "source_query": a.get("query", "")
                }
            })
        
        output = {
            "tools": tools,
            "table_name": table_name,
            "version": "1.0.0"
        }
        
        path = os.path.join(output_dir, f"{safe_name}_mcp.json")
        with open(path, "w", encoding="utf8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
    
    def _generate_index(self, by_table: dict):
        """生成汇总索引"""
        index = {
            "tables": [
                {
                    "name": name,
                    "api_count": len(apis),
                    "dir": sanitize_filename(name),
                    "file": os.path.join(sanitize_filename(name), f"{sanitize_filename(name)}.json")
                }
                for name, apis in sorted(by_table.items())
            ],
            "total_apis": sum(len(apis) for apis in by_table.values()),
            "total_tables": len(by_table)
        }
        
        path = os.path.join(self.output_dir, "_index.json")
        with open(path, "w", encoding="utf8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2)
        
        print(f"  [Export] 索引 -> {path}")

def export_records_by_table(self, valid_path: str, invalid_path: str, output_dir: str, allowed_tables: Optional[List[str]] = None):
        """按表导出valid/invalid jsonl"""
        os.makedirs(output_dir, exist_ok=True)

        valid_records = load_jsonl(valid_path)
        invalid_records = load_jsonl(invalid_path)

        allowed = set(allowed_tables or [])

        by_table_valid: dict[str, list[dict]] = {}
        by_table_invalid: dict[str, list[dict]] = {}

        def get_table_from_record(r):
            api = r.get("api_schema", {})
            if isinstance(api, dict):
                t = api.get("table")
                if t:
                    return t
            return self._extract_table_name(api if isinstance(api, dict) else {}, r)

        for r in valid_records:
            t = get_table_from_record(r)
            by_table_valid.setdefault(t, []).append(r)

        for r in invalid_records:
            t = get_table_from_record(r)
            by_table_invalid.setdefault(t, []).append(r)

        for t in sorted(set(list(by_table_valid.keys()) + list(by_table_invalid.keys()))):
            if allowed and t not in allowed:
                continue
            safe_name = sanitize_filename(t)
            table_dir = os.path.join(output_dir, safe_name)
            os.makedirs(table_dir, exist_ok=True)

            with open(os.path.join(table_dir, "valid.jsonl"), "w", encoding="utf8") as f:
                for r in by_table_valid.get(t, []):
                    f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")

            with open(os.path.join(table_dir, "invalid.jsonl"), "w", encoding="utf8") as f:
                for r in by_table_invalid.get(t, []):
                    f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")

            print(f"  [Export] {t} -> {table_dir}/valid.jsonl + invalid.jsonl")

if __name__ == "__main__":
    main()