#!/usr/bin/env python3
"""
Schema层单元测试
测试: models, loader, sampler
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from schema.models import (
    FieldInfo, TableSchema, DatabaseSchema,
    APISchema, GenerationRecord, ReviewTask, ReviewTaskType
)
from schema.loader import SchemaLoader, load_from_json_file
from schema.sampler import SchemaSampler


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


def test_field_info():
    """测试FieldInfo模型"""
    log_section("TEST: schema/models - FieldInfo")
    
    test_cases = [
        {
            "name": "user_id",
            "type": "INT",
            "is_primary": True,
            "comment": "用户ID"
        },
        {
            "name": "created_at",
            "type": "DATETIME",
            "is_nullable": True,
            "comment": "创建时间"
        }
    ]
    
    for data in test_cases:
        try:
            field = FieldInfo(**data)
            print(f"    Field: {field.name}")
            print(f"      type: {field.type}")
            print(f"      is_primary: {field.is_primary}")
            print(f"      is_nullable: {field.is_nullable}")
            log_result(f"FieldInfo: {field.name}", True)
        except Exception as e:
            log_result(f"FieldInfo: {data.get('name')}", False, str(e))


def test_table_schema():
    """测试TableSchema模型"""
    log_section("TEST: schema/models - TableSchema")
    
    table_data = {
        "name": "base_staff",
        "comment": "员工基础信息表",
        "fields": [
            {"name": "staff_id", "type": "INT", "is_primary": True, "comment": "员工ID"},
            {"name": "staff_name", "type": "VARCHAR", "length": 100, "comment": "姓名"},
            {"name": "department_id", "type": "INT", "comment": "部门ID"},
            {"name": "status", "type": "TINYINT", "comment": "状态:0离职,1在职"},
        ]
    }
    
    try:
        table = TableSchema(**table_data)
        print(f"    Table: {table.name}")
        print(f"    Comment: {table.comment}")
        print(f"    Fields: {len(table.fields)}")
        for f in table.fields:
            print(f"      - {f.name}: {f.type}{f'({f.length})' if f.length else ''}")
        
        # 测试方法
        primary = table.get_primary_key()
        print(f"    Primary Key: {primary.name if primary else 'None'}")
        
        log_result("TableSchema构建", True, f"{len(table.fields)} fields")
    except Exception as e:
        log_result("TableSchema构建", False, str(e))


def test_api_schema():
    """测试APISchema模型"""
    log_section("TEST: schema/models - APISchema")
    
    api_data = {
        "name": "get_base_staff_by_department",
        "description": "查询指定部门的员工列表",
        "inputSchema": {
            "type": "object",
            "properties": {
                "department_id": {"type": "integer", "description": "部门ID"}
            },
            "required": ["department_id"]
        },
        "outputSchema": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "staff_id": {"type": "integer"},
                    "staff_name": {"type": "string"}
                }
            }
        },
        "bound_sql": "SELECT staff_id, staff_name FROM base_staff WHERE department_id = :slot_department_id",
        "slot_mapping": {"department_id": "department_id"},
        "query_type": "exact_query",
        "table": "base_staff",
        "examples": [
            {"query": "研发部有哪些员工", "params": {"department_id": 1}}
        ]
    }
    
    try:
        api = APISchema(**api_data)
        print(f"    API: {api.name}")
        print(f"    Desc: {api.description}")
        print(f"    Type: {api.query_type}")
        print(f"    Table: {api.table}")
        print(f"    Required slots: {api.inputSchema.get('required', [])}")
        print(f"    SQL: {api.bound_sql[:60]}...")
        print(f"    Examples: {len(api.examples)}")
        
        # 测试序列化
        json_str = api.model_dump_json(indent=2)[:200]
        print(f"    JSON preview: {json_str}...")
        
        log_result("APISchema构建", True, f"sql_len={len(api.bound_sql)}")
    except Exception as e:
        log_result("APISchema构建", False, str(e))


def test_schema_loader():
    """测试SchemaLoader"""
    log_section("TEST: schema/loader")
    
    # 创建测试数据
    test_schema = {
        "database": "test_db",
        "tables": [
            {
                "name": "users",
                "comment": "用户表",
                "fields": [
                    {"name": "id", "type": "INT", "is_primary": True},
                    {"name": "name", "type": "VARCHAR", "length": 50},
                ]
            },
            {
                "name": "orders",
                "comment": "订单表",
                "fields": [
                    {"name": "order_id", "type": "BIGINT", "is_primary": True},
                    {"name": "user_id", "type": "INT"},
                    {"name": "amount", "type": "DECIMAL", "precision": 10, "scale": 2},
                ]
            }
        ]
    }
    
    log_subsection("SchemaLoader基本功能")
    try:
        loader = SchemaLoader(test_schema)
        print(f"    Database: {loader.schema.database}")
        print(f"    Tables: {len(loader.schema.tables)}")
        
        # 获取表
        users = loader.get_table("users")
        print(f"    Get 'users': {users.name if users else 'Not found'}")
        
        # 获取所有表名
        names = loader.get_table_names()
        print(f"    All tables: {names}")
        
        # 随机选择
        random_tables = loader.get_random_tables(2)
        print(f"    Random select: {[t.name for t in random_tables]}")
        
        log_result("SchemaLoader", True, f"{len(names)} tables")
    except Exception as e:
        log_result("SchemaLoader", False, str(e))
    
    log_subsection("SchemaSampler")
    try:
        sampler = SchemaSampler(loader.schema)
        
        # 测试子集采样
        print("    子集采样 (n_fields=2):")
        for _ in range(3):
            subset = sampler.sample_table_subset("orders", n_fields=2)
            fields = [f.name for f in subset.fields]
            print(f"      → {fields}")
        
        # 测试变异
        print("    字段变异:")
        original = test_schema["tables"][0]
        mutated = sampler.mutate_field_types(original)
        for orig, new in zip(original["fields"], mutated["fields"]):
            status = "changed" if orig["type"] != new["type"] else "same"
            print(f"      {orig['name']}: {orig['type']} → {new['type']} ({status})")
        
        log_result("SchemaSampler", True)
    except Exception as e:
        log_result("SchemaSampler", False, str(e))


def test_review_models():
    """测试审核模型"""
    log_section("TEST: schema/models - ReviewTask")
    
    task_data = {
        "task_id": "task_001",
        "task_type": ReviewTaskType.RUNTIME_CORRECTION,
        "source_query": "查询北京研发部员工",
        "candidate_tables": ["base_staff", "base_department"],
        "wrong_api": {"name": "get_staff_by_city", "description": "按城市查询"},
        "correct_api": {"name": "get_staff_by_dept_and_city", "description": "按部门和城市查询"},
        "distinction_instruction": "当同时出现部门和城市时，使用组合查询API"
    }
    
    try:
        task = ReviewTask(**task_data)
        print(f"    Task ID: {task.task_id}")
        print(f"    Type: {task.task_type.value}")
        print(f"    Status: {task.status.value}")
        print(f"    Candidate tables: {task.candidate_tables}")
        print(f"    Has distinction: {task.distinction_instruction is not None}")
        
        log_result("ReviewTask构建", True)
    except Exception as e:
        log_result("ReviewTask构建", False, str(e))


def run_all_tests():
    print(f"\n{'#'*60}")
    print(f"#{'':^58}#")
    print(f"#{'SCHEMA MODULE UNIT TESTS':^58}#")
    print(f"#{'':^58}#")
    print(f"{'#'*60}")
    
    test_field_info()
    test_table_schema()
    test_api_schema()
    test_schema_loader()
    test_review_models()
    
    print(f"\n{'#'*60}")
    print(f"#{'SCHEMA TESTS COMPLETED':^58}#")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    run_all_tests()