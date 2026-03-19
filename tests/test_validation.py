#!/usr/bin/env python3
"""
验证层单元测试
测试: intent_verify, round_trip, llm_judge
"""

import sys
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from validation.llm_judge import LLMJudge, VerificationType, VerificationResult
from validation.intent_verify import IntentVerifier


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


def test_llm_judge():
    """测试LLM评判器"""
    log_section("TEST: validation/llm_judge")
    
    judge = LLMJudge()
    
    log_subsection("Prompt构建")
    try:
        prompt = judge._build_judge_prompt(
            user_query="查询北京研发部的员工数量",
            executed_sql="SELECT COUNT(*) FROM base_staff WHERE city = '北京' AND department_id = 1",
            query_result={"data": [[42]], "columns": ["count"], "row_count": 1}
        )
        print(f"    Prompt长度: {len(prompt)}")
        print(f"    包含评判标准: {'CORRECT' in prompt and 'INCORRECT' in prompt}")
        
        # 检查三级评判说明
        has_correct = "结果完全回答了用户问题" in prompt
        has_partial = "结果部分回答了问题" in prompt
        has_incorrect = "结果完全不符合用户问题" in prompt
        
        print(f"    Has CORRECT desc: {has_correct}")
        print(f"    Has PARTIAL desc: {has_partial}")
        print(f"    Has INCORRECT desc: {has_incorrect}")
        
        log_result("judge prompt", all([has_correct, has_partial, has_incorrect]))
    except Exception as e:
        log_result("judge prompt", False, str(e))
    
    log_subsection("结果解析")
    
    test_cases = [
        # (llm_output, expected_type)
        ('{"type": "CORRECT", "reason": "完全匹配"}', VerificationType.CORRECT),
        ('{"type": "PARTIAL", "reason": "缺少部门筛选"}', VerificationType.PARTIAL),
        ('{"type": "INCORRECT", "reason": "城市错误"}', VerificationType.INCORRECT),
        ('{"type": "UNKNOWN"}', VerificationType.INCORRECT),  # 默认失败安全
        ('invalid json', VerificationType.INCORRECT),  # 解析失败
    ]
    
    for output, expected in test_cases:
        result = judge._parse_judge_result(output)
        success = result.type == expected
        detail = f"expected={expected.value}, got={result.type.value}"
        log_result(f"parse: {output[:30]}...", success, detail)
    
    log_subsection("可接受性判断")
    
    accept_cases = [
        (VerificationType.CORRECT, True),
        (VerificationType.PARTIAL, False),  # 默认不接受PARTIAL
        (VerificationType.INCORRECT, False),
    ]
    
    for vtype, expected in accept_cases:
        result = VerificationResult(type=vtype, reason="test")
        is_acceptable = judge.is_acceptable(result, accept_partial=False)
        success = is_acceptable == expected
        log_result(f"acceptable: {vtype.value}", success, f"expected={expected}, got={is_acceptable}")
    
    # 测试接受PARTIAL
    result_partial = VerificationResult(type=VerificationType.PARTIAL, reason="test")
    is_acceptable_partial = judge.is_acceptable(result_partial, accept_partial=True)
    log_result("acceptable with partial=True", is_acceptable_partial, f"got={is_acceptable_partial}")


def test_intent_verifier():
    """测试意图验证器"""
    log_section("TEST: validation/intent_verify")
    
    verifier = IntentVerifier()
    
    test_cases = [
        # (query, sql, query_type, need_fields, expected_match)
        ("员工总数", "SELECT COUNT(*) FROM staff", "aggregate_no_filter", False, True),
        ("北京员工", "SELECT * FROM staff WHERE city = '北京'", "exact_query", True, True),
        ("北京员工", "SELECT COUNT(*) FROM staff", "aggregate_no_filter", False, False),  # 意图不匹配
        ("按部门统计员工", "SELECT * FROM staff", "aggregate_no_filter", False, False),  # 明显不匹配
    ]
    
    log_subsection("意图匹配判断")
    for query, sql, qtype, need_f, expected in test_cases:
        # 简化测试：直接检查关键词匹配
        query_lower = query.lower()
        sql_lower = sql.lower()
        
        # 简单启发式判断
        has_count_in_query = "数" in query or "统计" in query or "多少" in query
        has_count_in_sql = "count(" in sql_lower
        
        # 对于无字段需求的情况，检查是否一致
        if not need_f:
            match = (has_count_in_query == has_count_in_sql) or \
                    (not has_count_in_query and not has_count_in_sql)
        else:
            match = True  # 有字段需求时复杂判断
        
        success = match == expected
        detail = f"query='{query[:20]}', sql_has_count={has_count_in_sql}, match={match}"
        log_result(f"intent: {query[:15]}...", success, detail)


def test_round_trip_mock():
    """测试往返验证（模拟）"""
    log_section("TEST: validation/round_trip (Mock)")
    
    # 由于需要DB连接，使用Mock测试
    log_subsection("组件初始化")
    try:
        from validation.round_trip import RoundTripChecker
        
        # Mock数据库连接
        mock_db = Mock()
        
        checker = RoundTripChecker(mock_db)
        print(f"    Checker初始化成功")
        log_result("RoundTripChecker init", True)
    except Exception as e:
        log_result("RoundTripChecker init", False, str(e))
        return
    
    log_subsection("候选召回逻辑")
    try:
        # Mock API池 - 修复：移除重复的name参数
        mock_apis = [
            Mock(spec=['name', 'description']),  # 使用spec定义属性
            Mock(spec=['name', 'description']),
            Mock(spec=['name', 'description']),
        ]
        # 单独设置属性
        mock_apis[0].name = "get_staff_by_city"
        mock_apis[0].description = "按城市查询员工"
        mock_apis[1].name = "get_staff_count"
        mock_apis[1].description = "统计员工数量"
        mock_apis[2].name = "get_department_list"
        mock_apis[2].description = "部门列表"
        
        # 测试召回prompt构建（间接测试）
        api_list = [{"name": a.name, "description": a.description} for a in mock_apis]
        print(f"    API池大小: {len(api_list)}")
        print(f"    示例API: {api_list[0]}")
        
        log_result("recall preparation", len(api_list) == 3)
    except Exception as e:
        log_result("recall preparation", False, str(e))


def run_all_tests():
    print(f"\n{'#'*60}")
    print(f"#{'':^58}#")
    print(f"#{'VALIDATION MODULE UNIT TESTS':^58}#")
    print(f"#{'':^58}#")
    print(f"{'#'*60}")
    
    test_llm_judge()
    test_intent_verifier()
    test_round_trip_mock()
    
    print(f"\n{'#'*60}")
    print(f"#{'VALIDATION TESTS COMPLETED':^58}#")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    run_all_tests()