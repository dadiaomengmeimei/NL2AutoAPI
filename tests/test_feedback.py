import sys
from pathlib import Path
from unittest.mock import Mock, patch  # 确保导入patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from feedback.query_augment import QueryAugmenter, AugmentStrategy
from feedback.schema_expander import SchemaExpander


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


def test_query_augmenter():
    """测试查询扩写器"""
    log_section("TEST: feedback/query_augment")
    
    augmenter = QueryAugmenter()
    
    test_query = "查询北京研发部的员工数量"
    table_hint = "base_staff"
    
    log_subsection("语义扩写 (Semantic)")
    try:
        variants = augmenter.augment(
            original_query=test_query,
            table_hint=table_hint,
            num_variants=3,
            strategy=AugmentStrategy.SEMANTIC
        )
        print(f"    原查询: {test_query}")
        print(f"    生成变体 ({len(variants)}):")
        for i, v in enumerate(variants):
            print(f"      [{i}] {v}")
        
        # 验证变体多样性
        unique = len(set(variants))
        success = len(variants) > 0 and unique == len(variants)
        log_result("semantic augment", success, f"{len(variants)} variants, {unique} unique")
    except Exception as e:
        log_result("semantic augment", False, str(e))
    
    log_subsection("结构扩写 (Structural)")
    try:
        variants = augmenter.augment(
            original_query=test_query,
            table_hint=table_hint,
            num_variants=3,
            strategy=AugmentStrategy.STRUCTURAL
        )
        print(f"    生成变体 ({len(variants)}):")
        for i, v in enumerate(variants):
            print(f"      [{i}] {v}")
        
        log_result("structural augment", len(variants) > 0)
    except Exception as e:
        log_result("structural augment", False, str(e))
    
    log_subsection("上下文扩写 (Contextual)")
    try:
        variants = augmenter.augment(
            original_query="员工列表",
            table_hint=table_hint,
            num_variants=3,
            strategy=AugmentStrategy.CONTEXTUAL
        )
        print(f"    原查询: '员工列表' (更简短，适合上下文扩写)")
        print(f"    生成变体 ({len(variants)}):")
        for i, v in enumerate(variants):
            print(f"      [{i}] {v}")
        
        log_result("contextual augment", len(variants) > 0)
    except Exception as e:
        log_result("contextual augment", False, str(e))
    
    log_subsection("多策略组合")
    try:
        all_variants = []
        for strategy in [AugmentStrategy.SEMANTIC, AugmentStrategy.STRUCTURAL]:
            vs = augmenter.augment("统计各部门人数", table_hint, 2, strategy)
            all_variants.extend(vs)
        
        # 去重
        unique_variants = list(dict.fromkeys(all_variants))
        print(f"    总变体: {len(all_variants)}, 去重后: {len(unique_variants)}")
        for i, v in enumerate(unique_variants[:5]):
            print(f"      [{i}] {v}")
        
        log_result("multi-strategy", len(unique_variants) > 1)
    except Exception as e:
        log_result("multi-strategy", False, str(e))


def test_schema_expander():
    """测试Schema扩展器"""
    log_section("TEST: feedback/schema_expander")
    
    mock_submitter = Mock()
    mock_submitter.submit = Mock()
    mock_submitter.submitted_count = 0
    
    expander = SchemaExpander(submitter=mock_submitter)
    
    from schema.models import APISchema
    
    base_api = APISchema(
        name="get_staff_by_city",
        description="按城市查询员工",
        inputSchema={
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"]
        },
        outputSchema={"type": "array", "items": {"type": "object"}},
        bound_sql="SELECT * FROM base_staff WHERE city = :slot_city",
        slot_mapping={"city": "city"},
        query_type="exact_query",
        table="base_staff",
        examples=[{"query": "北京员工", "params": {"city": "北京"}}]
    )
    
    log_subsection("SQL模板提取")
    try:
        template = expander._extract_sql_template(base_api)
        print(f"    原SQL: {base_api.bound_sql}")
        print(f"    模板: {template}")
        
        has_placeholder = ":slot_" in template or "{}" in template
        log_result("template extract", has_placeholder)
    except Exception as e:
        log_result("template extract", False, str(e))
    
    log_subsection("Query变体应用")
    try:
        queries = ["上海员工", "深圳研发人员", "广州销售"]
        variants = expander._apply_query_to_template(base_api, queries)
        
        print(f"    生成变体数: {len(variants)}")
        for i, (sql, slots) in enumerate(variants[:3]):
            print(f"      [{i}] SQL: {sql[:60]}...")
            print(f"           Slots: {slots}")
        
        log_result("apply template", len(variants) == len(queries))
    except Exception as e:
        log_result("apply template", False, str(e))
    
    log_subsection("完整扩展流程（Mock）")
    try:
        augmented_queries = ["上海员工", "深圳员工", "杭州员工"]
        
        # Mock LLM生成 - 修复：确保patch已导入
        with patch('feedback.schema_expander.call_llm_json') as mock_llm:
            mock_llm.return_value = {
                "name": "get_staff_by_city_v2",
                "description": "查询指定城市的员工信息",
                "inputSchema": {
                    "type": "object",
                    "properties": {"city": {"type": "string", "description": "城市名"}},
                    "required": ["city"]
                },
                "outputSchema": {"type": "array", "items": {"type": "object"}},
                "slot_mapping": {"city": "city"},
                "examples": [{"query": "上海员工", "params": {"city": "上海"}}]
            }
            
            results = expander.expand_with_augmented_queries(
                base_api=base_api,
                augmented_queries=augmented_queries[:2],  # 减少测试量
                auto_submit=False  # 不实际提交
            )
            
            print(f"    扩展结果数: {len(results)}")
            for i, api in enumerate(results):
                print(f"      [{i}] {api.name}: {api.description[:50]}")
            
            log_result("full expand flow", len(results) > 0)
    except Exception as e:
        log_result("full expand flow", False, str(e))
        import traceback
        traceback.print_exc()
    
    log_subsection("Schema完整性验证")
    try:
        # 验证生成的Schema包含必要字段
        test_api = APISchema(
            name="test_api",
            description="测试API",
            inputSchema={"type": "object", "properties": {}},
            outputSchema={"type": "array", "items": {"type": "object"}},
            bound_sql="SELECT 1",
            slot_mapping={},
            query_type="aggregate_no_filter",
            table="test",
            examples=[]
        )
        
        required_fields = ["name", "description", "inputSchema", "outputSchema", 
                          "bound_sql", "slot_mapping", "query_type", "table", "examples"]
        has_all = all(hasattr(test_api, f) for f in required_fields)
        
        print(f"    Schema字段检查: {required_fields}")
        print(f"    全部存在: {has_all}")
        
        log_result("schema completeness", has_all)
    except Exception as e:
        log_result("schema completeness", False, str(e))


def run_all_tests():
    print(f"\n{'#'*60}")
    print(f"#{'':^58}#")
    print(f"#{'FEEDBACK MODULE UNIT TESTS':^58}#")
    print(f"#{'':^58}#")
    print(f"{'#'*60}")
    
    test_query_augmenter()
    test_schema_expander()
    
    print(f"\n{'#'*60}")
    print(f"#{'FEEDBACK TESTS COMPLETED':^58}#")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    run_all_tests()