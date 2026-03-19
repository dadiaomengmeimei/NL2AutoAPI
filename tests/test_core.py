#!/usr/bin/env python3
"""
核心层单元测试
测试: config, database, llm, utils
"""

import sys
import os
import json
import tempfile
from pathlib import Path

# 添加项目根目录
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import pipeline_config, get_db_config, get_llm_config  # 修复：添加 get_llm_config
from core.utils import (
    parse_llm_json,
    generate_api_name,
    get_safe_filename,
    sanitize_api_name,
    fill_sql_with_values,
    load_jsonl,
    save_jsonl
)


def log_section(title: str):
    """打印测试章节"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def log_subsection(title: str):
    """打印子章节"""
    print(f"\n  📌 {title}")
    print(f"  {'-'*50}")


def log_result(test_name: str, success: bool, details: str = ""):
    """打印测试结果"""
    status = "✅ PASS" if success else "❌ FAIL"
    print(f"    {status} | {test_name}")
    if details:
        print(f"           {details}")


def test_config():
    """测试配置模块"""
    log_section("TEST: core/config")
    
    # 测试1: 配置加载
    log_subsection("PipelineConfig 默认值")
    try:
        config = pipeline_config
        print(f"    iterations: {config.iterations}")
        print(f"    output_dir: {config.output_dir}")
        print(f"    do_round_trip: {config.do_round_trip}")
        log_result("配置默认值", True)
    except Exception as e:
        log_result("配置默认值", False, str(e))
    
    # 测试2: 数据库配置
    log_subsection("DB配置获取")
    try:
        db_config = get_db_config()
        print(f"    配置项数: {len(db_config)}")
        # 脱敏打印
        safe_config = {k: "***" if "pass" in k.lower() else v 
                      for k, v in db_config.items()}
        for k, v in safe_config.items():
            print(f"      {k}: {v}")
        log_result("DB配置获取", True)
    except Exception as e:
        log_result("DB配置获取", False, str(e))
    
    # 测试3: LLM配置 - 新增
    log_subsection("LLM配置获取")
    try:
        llm_config = get_llm_config()
        print(f"    配置项数: {len(llm_config)}")
        safe_config = {k: "***" if "key" in k.lower() else v 
                      for k, v in llm_config.items()}
        for k, v in safe_config.items():
            print(f"      {k}: {v}")
        log_result("LLM配置获取", True)
    except Exception as e:
        log_result("LLM配置获取", False, str(e))


def test_utils_json():
    """测试JSON解析工具"""
    log_section("TEST: core/utils - JSON解析")
    
    test_cases = [
        ("标准JSON", '{"name": "test", "value": 123}', {"name": "test", "value": 123}),
        ("带注释", '```json\n{"a": 1}\n```', {"a": 1}),
        ("Markdown代码块", '```\n{"b": 2}\n```', {"b": 2}),
        ("JSON数组", '[1, 2, 3]', [1, 2, 3]),
        ("错误JSON", 'not json', None),
    ]
    
    for name, input_str, expected in test_cases:
        result = parse_llm_json(input_str)
        success = result == expected or (expected is None and result is None)
        detail = f"input_len={len(input_str)}, output_type={type(result).__name__}"
        if result and len(str(result)) < 50:
            detail += f", result={result}"
        log_result(f"parse_llm_json: {name}", success, detail)


def test_utils_naming():
    """测试命名工具"""
    log_section("TEST: core/utils - 命名生成")
    
    # 测试API名称生成
    log_subsection("generate_api_name")
    test_cases = [
        ("base_staff", ["city"], "aggregate_no_filter", None),
        ("user_order", ["status", "date"], "aggregate_with_filter", None),
        ("product_inventory", [], "list_no_filter", "top10"),
    ]
    
    for table, slots, qtype, desc_hint in test_cases:
        name = generate_api_name(table, slots, qtype, desc_hint)
        print(f"    table={table}, slots={slots}")
        print(f"    qtype={qtype}, hint={desc_hint}")
        print(f"    → {name}")
        success = name.startswith("get_") or name.startswith("count_") or \
                 name.startswith("list_") or name.startswith("sum_")
        log_result(f"生成API名: {table}", success, f"len={len(name)}")
    
    # 测试安全文件名
    log_subsection("get_safe_filename / sanitize_api_name")
    filename_tests = [
        "base_staff",
        "user/profile",
        "api<>name",
        "a" * 150,  # 超长
    ]
    for name in filename_tests:
        safe = get_safe_filename(name)
        print(f"    '{name[:30]}{'...' if len(name)>30 else ''}' → '{safe}'")
        log_result(f"safe_filename: {name[:20]}", len(safe) <= 100 and '/' not in safe)


def test_utils_sql():
    """测试SQL工具"""
    log_section("TEST: core/utils - SQL填充")
    
    sql_template = "SELECT * FROM users WHERE city = :slot_city AND age > :slot_age"
    slots = {"city": "Beijing", "age": 18}
    
    result = fill_sql_with_values(sql_template, slots)
    print(f"    Template: {sql_template[:50]}...")
    print(f"    Slots: {slots}")
    print(f"    Result: {result[:80]}...")
    
    success = "Beijing" in result and "18" in result and ":slot_" not in result
    log_result("fill_sql_with_values", success)
    
    # 测试缺失slot
    incomplete_slots = {"city": "Shanghai"}
    result2 = fill_sql_with_values(sql_template, incomplete_slots)
    print(f"    \n  缺失slot测试: {result2[:60]}...")
    log_result("handle_missing_slots", ":slot_age" in result2 or "NULL" in result2)


def test_utils_io():
    """测试IO工具"""
    log_section("TEST: core/utils - 文件IO")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = os.path.join(tmpdir, "test.jsonl")
        
        # 准备测试数据
        test_data = [
            {"id": 1, "name": "test1", "value": 1.5},
            {"id": 2, "name": "test2", "value": 2.5},
            {"id": 3, "name": "test3中文", "value": None},
        ]
        
        # 测试保存
        log_subsection("save_jsonl")
        try:
            save_jsonl(test_data, test_file)
            file_size = os.path.getsize(test_file)
            print(f"    保存 {len(test_data)} 条记录")
            print(f"    文件大小: {file_size} bytes")
            log_result("save_jsonl", True)
        except Exception as e:
            log_result("save_jsonl", False, str(e))
        
        # 测试加载
        log_subsection("load_jsonl")
        try:
            loaded = load_jsonl(test_file)
            print(f"    加载 {len(loaded)} 条记录")
            for i, r in enumerate(loaded[:2]):
                print(f"      [{i}] id={r.get('id')}, name={r.get('name')[:10]}")
            
            success = len(loaded) == 3 and loaded[0].get("id") == 1
            log_result("load_jsonl", success)
        except Exception as e:
            log_result("load_jsonl", False, str(e))
        
        # 测试追加
        log_subsection("save_jsonl (append)")
        try:
            append_data = [{"id": 4, "name": "append"}]
            save_jsonl(append_data, test_file, append=True)
            loaded2 = load_jsonl(test_file)
            print(f"    追加后共 {len(loaded2)} 条")
            log_result("append mode", len(loaded2) == 4)
        except Exception as e:
            log_result("append mode", False, str(e))


def run_all_tests():
    """运行所有测试"""
    print(f"\n{'#'*60}")
    print(f"#{'':^58}#")
    print(f"#{'CORE MODULE UNIT TESTS':^58}#")
    print(f"#{'':^58}#")
    print(f"{'#'*60}")
    
    test_config()
    test_utils_json()
    test_utils_naming()
    test_utils_sql()
    test_utils_io()
    
    print(f"\n{'#'*60}")
    print(f"#{'CORE TESTS COMPLETED':^58}#")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    run_all_tests()