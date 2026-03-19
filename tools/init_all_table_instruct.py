"""
初始化 all_table_instruct.json
从valid.jsonl中提取表能力信息，生成表级别的约束说明
"""
import json
import os
import sys
from collections import defaultdict

# 添加父路径到sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.utils import load_jsonl
from core.logger import get_logger

logger = get_logger()


def extract_table_capabilities_from_valid(valid_jsonl_path: str) -> dict:
    """
    从valid.jsonl提取表能力信息
    """
    table_apis = defaultdict(list)
    
    records = load_jsonl(valid_jsonl_path)
    for record in records:
        table = record.get("table")
        api_schema = record.get("api_schema", {})
        
        if table:
            table_apis[table].append(api_schema)
    
    return dict(table_apis)


def generate_all_table_instruct(
    valid_jsonl_path: str,
    output_path: str
):
    """
    生成 all_table_instruct.json
    """
    logger.info(f"读取valid.jsonl: {valid_jsonl_path}")
    
    # 1. 从valid.jsonl提取信息
    records = load_jsonl(valid_jsonl_path)
    
    table_info = defaultdict(lambda: {
        "apis": [],
        "capabilities": set(),
        "supported_fields": set(),
    })
    
    for record in records:
        table = record.get("table", "unknown")
        api_schema = record.get("api_schema", {})
        api_name = api_schema.get("name", "")
        sql = api_schema.get("bound_sql", "")
        
        table_info[table]["apis"].append(api_name)
        
        # 推断能力类型
        if "exact_query" in api_name.lower():
            table_info[table]["capabilities"].add("精准查询 (exact_query)")
        elif "group_" in api_name.lower():
            table_info[table]["capabilities"].add("分组聚合 (group_by)")
        elif "count" in api_name.lower():
            table_info[table]["capabilities"].add("计数统计 (count)")
        
        # 从SQL提取可能的字段（简单启发式）
        common_fields = [
            "dept_descr", "t_business_descr", "emplid", "name", "t_email_busn",
            "empl_class", "hr_status", "setid", "t_is_from_tencent", "t_mgr_attr",
            "business_unit", "contract_company"
        ]
        for field in common_fields:
            if field in sql:
                table_info[table]["supported_fields"].add(field)
    
    # 2. 构建约束文档
    all_instruct = {
        "description": "所有表的能力范畴和约束说明",
        "generated_at": "2024-现在",
        "tables": {}
    }
    
    for table_name, info in table_info.items():
        all_instruct["tables"][table_name] = {
            "table": table_name,
            "description": f"{table_name}表，包含员工、组织等维度数据",
            "supported_fields": sorted(list(info["supported_fields"])),
            "capabilities": sorted(list(info["capabilities"])),
            "limitations": [
                "仅支持单表查询（不支持跨表Join）",
                "不支持自定义复杂窗口函数",
                "不支持机器学习或高级分析函数",
                "不支持实时数据（数据延迟1天）"
            ],
            "api_count": len(set(info["apis"])),
            "sample_apis": list(set(info["apis"]))[:5]
        }
    
    # 3. 保存
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_instruct, f, ensure_ascii=False, indent=2)
    
    logger.info(f"生成{len(all_instruct['tables'])}个表的能力说明到: {output_path}")
    return all_instruct


if __name__ == "__main__":
    # 使用
    valid_path = "./output/base_staff/valid.jsonl"
    output_path = "./output/all_table_instruct.json"
    
    if os.path.exists(valid_path):
        generate_all_table_instruct(valid_path, output_path)
        print(f"✅ 已生成: {output_path}")
    else:
        print(f"❌ 找不到valid.jsonl: {valid_path}")
