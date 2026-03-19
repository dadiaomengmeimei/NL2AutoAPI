#!/usr/bin/env python3
"""
生成层单元测试
测试: query_types, sql_generator, query_generator, api_generator, rule_based
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from generation.query_types import QUERY_TYPES, QueryTypeConfig, get_random_query_type
from generation.sql_generator import SQLGenerator, build_sql_prompt
from generation.query_generator import QueryGenerator
from generation.api_generator import APIGenerator
from generation.rule_based import RuleBasedGenerator


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


def test_query_types():
    """测试查询类型配置"""
    log_section("TEST: generation/query_types")
    
    log_subsection("配置完整性检查")
    print(f"    定义类型数: {len(QUERY_TYPES)}")
    
    for name, config in QUERY_TYPES.items():
        print(f"\n    [{name}]")
        print(f"      need_fields: {config.need_fields}")
        print(f"      slot_required: {config.slot_required}")
        print(f"      weight: {config.weight}")
        print(f"      examples: {len(config.examples)}")
        
        # 验证配置一致性
        success = True
        detail = ""
        
        # aggregate_no_filter 必须没有slot
        if name == "aggregate_no_filter" and config.slot_required:
            success = False
            detail = "aggregate_no_filter should not require slots"
        
        # 有slot_required必须有need_fields
        if config.slot_required and not config.need_fields:
            success = False
            detail = "slot_required needs need_fields=True"
        
        log_result(f"config: {name}", success, detail)
    
    log_subsection("随机采样")
    try:
        samples = {}
        for _ in range(100):
            qtype = get_random_query_type()
            samples[qtype] = samples.get(qtype, 0) + 1
        
        print("    采样分布 (100次):")
        for name, count in sorted(samples.items()):
            expected = QUERY_TYPES[name].weight / sum(q.weight for q in QUERY_TYPES.values())
            actual = count / 100
            print(f"      {name:25s}: {count:3d} (expected ~{expected:.2f}, actual {actual:.2f})")
        
        log_result("random sampling", len(samples) > 1)
    except Exception as e:
        log_result("random sampling", False, str(e))


def test_sql_generator():
    """测试SQL生成器"""
    log_section("TEST: generation/sql_generator")
    
    # 测试表
    test_table = {
        "name": "base_staff",
        "comment": "员工基础信息表",
        "fields": [
            {"name": "staff_id", "type": "INT", "is_primary": True, "comment": "员工ID"},
            {"name": "staff_name", "type": "VARCHAR", "length": 100, "comment": "姓名"},
            {"name": "department_id", "type": "INT", "comment": "部门ID"},
            {"name": "city", "type": "VARCHAR", "length": 50, "comment": "城市"},
            {"name": "salary", "type": "DECIMAL", "precision": 10, "scale": 2, "comment": "薪资"},
            {"name": "status", "type": "TINYINT", "comment": "状态"},
        ]
    }
    
    generator = SQLGenerator()
    
    log_subsection("Prompt构建")
    try:
        prompt = build_sql_prompt(
            table=test_table,
            query_type="aggregate_with_filter",
            selected_fields=["city", "salary"],
            slot_fields=["city"]
        )
        print(f"    Prompt长度: {len(prompt)}")
        print(f"    包含约束标记: {'【强制约束】' in prompt}")
        print(f"    包含字段信息: {'salary' in prompt and 'city' in prompt}")
        
        # 检查约束描述
        has_need_fields = "need_fields: True" in prompt
        has_slot_required = "slot_required: True" in prompt
        print(f"    need_fields标记: {has_need_fields}")
        print(f"    slot_required标记: {has_slot_required}")
        
        log_result("prompt construction", True, f"len={len(prompt)}")
    except Exception as e:
        log_result("prompt construction", False, str(e))
    
    log_subsection("SQL约束验证")
    
    test_sqls = [
        # (sql, query_type, need_fields, slot_required, should_pass)
        ("SELECT COUNT(*) FROM base_staff", "aggregate_no_filter", False, False, True),
        ("SELECT COUNT(*) FROM base_staff WHERE city = :slot_city", "aggregate_no_filter", False, False, False),  # 有WHERE但不应有
        ("SELECT city, COUNT(*) FROM base_staff WHERE city = :slot_city GROUP BY city", "aggregate_with_filter", True, True, True),
        ("SELECT city, COUNT(*) FROM base_staff GROUP BY city", "aggregate_with_filter", True, True, False),  # 缺slot
        ("SELECT * FROM base_staff LIMIT 10", "list_no_filter", True, False, True),
        ("SELECT * FROM base_staff WHERE id = 1", "list_no_filter", True, False, False),  # 有WHERE但不应有
    ]
    
    for sql, qtype, need_f, slot_req, should_pass in test_sqls:
        # 构建配置
        config = QueryTypeConfig(
            need_fields=need_f,
            slot_required=slot_req,
            weight=1,
            examples=[]
        )
        
        # 简化验证：只检查关键特征
        issues = []
        
        if not need_f and ("SELECT *" not in sql and "COUNT(*)" not in sql):
            if any(f in sql for f in test_table["fields"] if f["name"] not in ["staff_id"]):
                issues.append("should not reference fields")
        
        if not slot_req and ":slot_" in sql:
            issues.append("should not have slots")
        
        if slot_req and ":slot_" not in sql:
            issues.append("missing required slots")
        
        passed = len(issues) == 0
        success = passed == should_pass
        
        detail = f"expected={'pass' if should_pass else 'fail'}, actual={'pass' if passed else 'fail'}"
        if issues:
            detail += f", issues={issues}"
        
        log_result(f"validate: {qtype[:20]}", success, detail[:60])


def test_query_generator():
    """测试Query生成器"""
    log_section("TEST: generation/query_generator")
    
    generator = QueryGenerator()
    
    test_api = {
        "name": "get_base_staff_by_city",
        "description": "查询指定城市的员工列表",
        "inputSchema": {
            "properties": {
                "city": {"type": "string", "description": "城市名称"}
            },
            "required": ["city"]
        },
        "query_type": "exact_query",
        "table": "base_staff"
    }
    
    log_subsection("边界示例生成")
    try:
        examples = generator._generate_boundary_examples(test_api, num_examples=3)
        print(f"    生成示例数: {len(examples)}")
        
        for i, ex in enumerate(examples):
            query = ex.get("query", "N/A")
            params = ex.get("params", {})
            print(f"      [{i}] {query[:50]}")
            print(f"          params: {params}")
        
        success = len(examples) > 0 and all("query" in ex for ex in examples)
        log_result("boundary examples", success)
    except Exception as e:
        log_result("boundary examples", False, str(e))
    
    log_subsection("Prompt构建")
    try:
        prompt = generator._build_prompt(test_api, num_variants=2)
        print(f"    Prompt长度: {len(prompt)}")
        print(f"    包含示例: {'边界示例' in prompt}")
        print(f"    包含要求: {'JSON格式' in prompt}")
        
        log_result("query prompt", True, f"len={len(prompt)}")
    except Exception as e:
        log_result("query prompt", False, str(e))


def test_api_generator():
    """测试API生成器"""
    log_section("TEST: generation/api_generator")
    
    generator = APIGenerator()
    
    test_sql = "SELECT city, COUNT(*) as cnt FROM base_staff WHERE status = 1 GROUP BY city"
    test_query_type = "group_aggregate_with_filter"
    test_table = "base_staff"
    test_fields = [
        {"name": "city", "type": "VARCHAR", "comment": "城市"},
        {"name": "status", "type": "TINYINT", "comment": "状态"},
    ]
    
    log_subsection("Schema生成")
    try:
        api = generator.generate_from_sql(
            sql=test_sql,
            query_type=test_query_type,
            table=test_table,
            fields=test_fields
        )
        
        print(f"    Generated API:")
        print(f"      Name: {api.name}")
        print(f"      Desc: {api.description}")
        print(f"      Table: {api.table}")
        print(f"      QueryType: {api.query_type}")
        print(f"      SQL: {api.bound_sql[:60]}...")
        
        # 验证结构
        has_input = "properties" in api.inputSchema
        has_output = "type" in api.outputSchema
        has_slots = len(api.slot_mapping) > 0
        has_examples = len(api.examples) > 0
        
        print(f"      Has inputSchema: {has_input}")
        print(f"      Has outputSchema: {has_output}")
        print(f"      Has slot_mapping: {has_slots} ({api.slot_mapping})")
        print(f"      Has examples: {has_examples} ({len(api.examples)})")
        
        success = all([has_input, has_output, has_slots, has_examples])
        log_result("API generation", success)
        
    except Exception as e:
        log_result("API generation", False, str(e))
        import traceback
        traceback.print_exc()
    
    log_subsection("名称生成验证")
    name_tests = [
        ("base_staff", [], "aggregate_no_filter", "table_count"),
        ("base_staff", ["city"], "exact_query", "city_query"),
        ("user_order", ["user_id", "status"], "aggregate_with_filter", None),
    ]
    
    for table, slots, qtype, desc_hint in name_tests:
        name = generator._generate_api_name(table, slots, qtype, desc_hint)
        print(f"    {table}, slots={slots}, {qtype} → {name}")


def test_rule_based_generator():
    """测试规则生成器"""
    log_section("TEST: generation/rule_based")
    
    generator = RuleBasedGenerator()
    
    # 模拟数据探查结果
    table_profile = {
        "table_name": "test_orders",
        "fields": {
            "order_id": {"type": "BIGINT", "is_primary": True, "comment": "订单ID"},
            "user_id": {"type": "INT", "comment": "用户ID"},
            "status": {"type": "VARCHAR", "length": 20, "distinct_count": 5, "comment": "状态"},
            "amount": {"type": "DECIMAL", "distinct_count": 1000, "comment": "金额"},
            "created_at": {"type": "DATETIME", "comment": "创建时间"},
        }
    }
    
    log_subsection("数据探查解释")
    try:
        analysis = generator._analyze_table(table_profile)
        print("    字段分析:")
        for fname, ftype in analysis.items():
            print(f"      {fname}: {ftype}")
        
        # 验证分类
        has_dimension = "dimension" in analysis.values()
        has_measure = "measure" in analysis.values()
        print(f"    Has dimension: {has_dimension}")
        print(f"    Has measure: {has_measure}")
        
        log_result("table analysis", has_dimension or has_measure)
    except Exception as e:
        log_result("table analysis", False, str(e))
    
    log_subsection("规则SQL生成")
    try:
        sqls = generator._generate_rules_for_table(table_profile, sample_values={
            "status": ["pending", "paid", "shipped"],
            "user_id": [1001, 1002, 1003]
        })
        
        print(f"    生成SQL数: {len(sqls)}")
        for i, sql_info in enumerate(sqls[:5]):
            print(f"      [{i}] {sql_info['query_type'][:25]:25s} | {sql_info['sql'][:50]}...")
        
        # 验证多样性
        types = set(s["query_type"] for s in sqls)
        print(f"    覆盖类型: {types}")
        
        log_result("rule generation", len(sqls) > 0 and len(types) > 1)
    except Exception as e:
        log_result("rule generation", False, str(e))


def run_all_tests():
    print(f"\n{'#'*60}")
    print(f"#{'':^58}#")
    print(f"#{'GENERATION MODULE UNIT TESTS':^58}#")
    print(f"#{'':^58}#")
    print(f"{'#'*60}")
    
    test_query_types()
    test_sql_generator()
    test_query_generator()
    test_api_generator()
    test_rule_based_generator()
    
    print(f"\n{'#'*60}")
    print(f"#{'GENERATION TESTS COMPLETED':^58}#")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    run_all_tests()