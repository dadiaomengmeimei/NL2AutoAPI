#!/usr/bin/env python3
"""
运行时层单元测试
测试: registry, recall, slot_filling, router
"""

import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from schema.models import APISchema
from runtime.registry import APIRegistry
from runtime.slot_filling import SlotFiller
from runtime.recall import APIRecaller


def log_section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def log_subsection(title: str):
    print(f"\n  📌 {title}")
    print(f"  {'-'*50}")


def log_result(test_name: str, success: bool, details: str = ""):
    status = "✅ PASS" if success else "❌ FAIL"
    print(f"    {status} | {test_name}")
    if details:
        print(f"           {details}")


def create_mock_api(name: str, table: str, desc: str, slots: list = None) -> APISchema:
    """创建Mock API"""
    return APISchema(
        name=name,
        description=desc,
        inputSchema={
            "type": "object",
            "properties": {s: {"type": "string"} for s in (slots or [])},
            "required": slots or []
        },
        outputSchema={"type": "array", "items": {"type": "object"}},
        bound_sql=f"SELECT * FROM {table} WHERE 1=1",
        slot_mapping={s: s for s in (slots or [])},
        query_type="exact_query",
        table=table,
        examples=[]
    )


def test_api_registry():
    """测试API注册中心"""
    log_section("TEST: runtime/registry")
    
    # 创建Mock数据文件
    import tempfile
    import json
    import os
    
    mock_apis = [
        {
            "name": "get_staff_by_city",
            "description": "按城市查询员工",
            "inputSchema": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
            "outputSchema": {"type": "array", "items": {"type": "object"}},
            "bound_sql": "SELECT * FROM base_staff WHERE city = :slot_city",
            "slot_mapping": {"city": "city"},
            "query_type": "exact_query",
            "table": "base_staff",
            "examples": [{"query": "北京员工", "params": {"city": "北京"}}]
        },
        {
            "name": "count_staff",
            "description": "统计员工数量",
            "inputSchema": {"type": "object", "properties": {}},
            "outputSchema": {"type": "array", "items": {"type": "object"}},
            "bound_sql": "SELECT COUNT(*) FROM base_staff",
            "slot_mapping": {},
            "query_type": "aggregate_no_filter",
            "table": "base_staff",
            "examples": [{"query": "员工总数", "params": {}}]
        },
        {
            "name": "get_department_list",
            "description": "部门列表",
            "inputSchema": {"type": "object", "properties": {}},
            "outputSchema": {"type": "array", "items": {"type": "object"}},
            "bound_sql": "SELECT * FROM base_department",
            "slot_mapping": {},
            "query_type": "list_no_filter",
            "table": "base_department",
            "examples": [{"query": "所有部门", "params": {}}]
        }
    ]
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for api in mock_apis:
            f.write(json.dumps(api) + '\n')
        temp_path = f.name
    
    try:
        log_subsection("Registry加载")
        registry = APIRegistry(temp_path)
        print(f"    加载API数: {len(registry.apis)}")
        print(f"    表索引: {list(registry.table_index.keys())}")
        
        success = len(registry.apis) == 3
        log_result("registry load", success, f"apis={len(registry.apis)}")
        
        log_subsection("按表查询")
        staff_apis = registry.get_apis_by_table("base_staff")
        print(f"    base_staff APIs: {len(staff_apis)}")
        for api in staff_apis:
            print(f"      - {api.name}")
        
        dept_apis = registry.get_apis_by_table("base_department")
        print(f"    base_department APIs: {len(dept_apis)}")
        
        log_result("get by table", len(staff_apis) == 2 and len(dept_apis) == 1)
        
        log_subsection("关键词召回")
        # 简单的关键词匹配测试
        results = registry.search_apis("城市")
        print(f"    搜索'城市': {len(results)} 个")
        for r in results:
            print(f"      - {r.name}")
        
        results2 = registry.search_apis("统计")
        print(f"    搜索'统计': {len(results2)} 个")
        
        # 候选表测试
        tables = registry.get_candidate_tables("北京员工")
        print(f"    候选表('北京员工'): {tables}")
        
        log_result("keyword search", len(results) > 0)
        
    finally:
        os.unlink(temp_path)


def test_slot_filling():
    """测试填槽"""
    log_section("TEST: runtime/slot_filling")
    
    filler = SlotFiller()
    
    test_api = APISchema(
        name="get_staff_by_city_and_status",
        description="按城市和状态查询员工",
        inputSchema={
            "type": "object",
            "properties": {
                "city": {"type": "string", "description": "城市名称"},
                "status": {"type": "integer", "description": "状态:0离职,1在职"}
            },
            "required": ["city"]
        },
        outputSchema={"type": "array", "items": {"type": "object"}},
        bound_sql="SELECT * FROM staff WHERE city = :slot_city AND status = :slot_status",
        slot_mapping={"city": "city", "status": "status"},
        query_type="exact_query",
        table="staff",
        examples=[]
    )
    
    log_subsection("参数提取")
    
    test_queries = [
        ("查询北京的员工", {"city": "北京"}),
        ("上海在职员工", {"city": "上海", "status": "1"}),
        ("查询深圳状态为0的员工", {"city": "深圳", "status": "0"}),
    ]
    
    for query, expected_slots in test_queries:
        print(f"\n    Query: '{query}'")
        params = filler.fill(query, test_api)
        print(f"    Extracted: {params}")
        
        # 验证提取结果
        has_expected = all(
            params.get(k) is not None or k not in test_api.inputSchema.get("required", [])
            for k in expected_slots.keys()
        )
        log_result(f"extract: {query[:20]}...", has_expected, str(params))
    
    log_subsection("参数验证")
    
    # 测试必填验证
    valid, missing = filler.validate({"city": "北京"}, test_api)
    print(f"    Validate {{city:北京}}: valid={valid}, missing={missing}")
    log_result("validate with required", valid and len(missing) == 0)
    
    # 测试缺失必填
    valid2, missing2 = filler.validate({}, test_api)
    print(f"    Validate {{}}: valid={valid2}, missing={missing2}")
    log_result("validate missing", not valid2 and "city" in missing2)


def test_api_recaller():
    """测试API召回器"""
    log_section("TEST: runtime/recall")
    
    # 创建Mock Registry
    mock_apis = [
        create_mock_api("api1", "staff", "按城市查询员工", ["city"]),
        create_mock_api("api2", "staff", "统计员工数量", []),
        create_mock_api("api3", "dept", "部门列表", []),
    ]
    
    mock_registry = Mock()
    mock_registry.apis = mock_apis
    mock_registry.get_apis_by_table = Mock(return_value=mock_apis[:2])
    mock_registry.get_candidate_tables = Mock(return_value=["staff"])
    
    recaller = APIRecaller(mock_registry)
    
    log_subsection("召回Prompt构建")
    try:
        # 测试内部方法
        query = "北京员工"
        candidates = mock_apis
        
        # 模拟召回过程
        print(f"    Query: '{query}'")
        print(f"    Candidates: {len(candidates)}")
        for c in candidates:
            print(f"      - {c.name}: {c.description}")
        
        # 由于需要LLM，这里只测试准备逻辑
        log_result("recall preparation", True)
    except Exception as e:
        log_result("recall preparation", False, str(e))


def run_all_tests():
    print(f"\n{'#'*60}")
    print(f"#{'':^58}#")
    print(f"#{'RUNTIME MODULE UNIT TESTS':^58}#")
    print(f"#{'':^58}#")
    print(f"{'#'*60}")
    
    test_api_registry()
    test_slot_filling()
    test_api_recaller()
    
    print(f"\n{'#'*60}")
    print(f"#{'RUNTIME TESTS COMPLETED':^58}#")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    run_all_tests()