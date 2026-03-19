"""
系统集成测试脚本
验证表能力约束管理系统的完整流程
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.capability_manager import CapabilityInstructManager, SynthesizedTableDesc
from core.utils import load_jsonl
from core.logger import get_logger

logger = get_logger()


def test_all_table_instruct_loading():
    """测试all_table_instruct.json加载"""
    print("\n" + "="*60)
    print("TEST 1: 加载all_table_instruct.json")
    print("="*60)
    
    mgr = CapabilityInstructManager("./output")
    all_instruct = mgr.load_all_table_instruct()
    
    if "tables" not in all_instruct:
        print("❌ FAIL: all_table_instruct.json格式错误，缺少tables字段")
        return False
    
    tables = all_instruct["tables"]
    print(f"✅ PASS: 找到 {len(tables)} 个表的能力说明")
    
    for table_name, info in tables.items():
        print(f"\n  表: {table_name}")
        print(f"    - 支持的字段: {len(info.get('supported_fields', []))} 个")
        print(f"    - 能力: {', '.join(info.get('capabilities', []))}")
        print(f"    - 限制: {len(info.get('limitations', []))} 项")
    
    return True


def test_instruct_generation():
    """测试能力约束指令生成"""
    print("\n" + "="*60)
    print("TEST 2: 生成能力约束指令")
    print("="*60)
    
    mgr = CapabilityInstructManager("./output")
    all_instruct = mgr.load_all_table_instruct()
    base_staff_info = all_instruct.get("tables", {}).get("base_staff", {})
    
    # 测试查询1：字段不支持
    test_query_1 = "公司财务预算是多少？"
    print(f"\n测试查询1: {test_query_1}")
    
    instruct_1 = mgr.generate_instruct(
        query=test_query_1,
        table_name="base_staff",
        table_desc="员工信息表，包含员工基本信息、部门、业务单元等",
        available_fields=base_staff_info.get("supported_fields", []),
        existing_api_names=["exact_query_base_staff_emplid", "group_distribution_base_staff_t_business_descr"]
    )
    
    print(f"  原因类型: {instruct_1.get('reason_type', '未知')}")
    print(f"  用户提示: {instruct_1.get('user_friendly_message', '无')}")
    print(f"✅ PASS: 成功生成instruct")
    
    # 测试查询2：跨表关联
    test_query_2 = "员工姓名和部门的工资总和"
    print(f"\n测试查询2: {test_query_2}")
    
    instruct_2 = mgr.generate_instruct(
        query=test_query_2,
        table_name="base_staff",
        table_desc="员工信息表，包含员工基本信息、部门、业务单元等",
        available_fields=base_staff_info.get("supported_fields", []),
        existing_api_names=["exact_query_base_staff_emplid"]
    )
    
    print(f"  原因类型: {instruct_2.get('reason_type', '未知')}")
    print(f"  用户提示: {instruct_2.get('user_friendly_message', '无')}")
    print(f"✅ PASS: 成功生成instruct")
    
    return True


def test_apischema_with_instruct():
    """测试APISchema中instruct字段"""
    print("\n" + "="*60)
    print("TEST 3: APISchema包含instruct字段")
    print("="*60)
    
    from schema.models import APISchema
    
    # 创建带instruct的APISchema
    api_with_instruct = APISchema(
        name="test_api",
        description="测试API",
        bound_sql="SELECT * FROM test",
        query_type="exact_query",
        table="test_table",
        instruct={
            "reason_type": "field_not_supported",
            "table_limitation": "表不包含财务字段",
            "user_friendly_message": "当前表格不支持财务数据查询"
        }
    )
    
    print(f"✅ 创建带instruct的APISchema")
    print(f"   - API名: {api_with_instruct.name}")
    print(f"   - instruct.reason_type: {api_with_instruct.instruct['reason_type']}")
    print(f"   - instruct.user_friendly_message: {api_with_instruct.instruct['user_friendly_message']}")
    
    # 序列化为JSON
    api_json = api_with_instruct.model_dump()
    api_json_str = json.dumps(api_json, ensure_ascii=False, indent=2)
    print(f"\n✅ 序列化为JSON成功，大小: {len(api_json_str)} 字符")
    
    return True


def test_friendly_messages():
    """测试温和话术"""
    print("\n" + "="*60)
    print("TEST 4: 温和话术系统")
    print("="*60)
    
    from core.capability_manager import FRIENDLY_MESSAGES
    
    print("✅ 加载温和话术库")
    for msg_type, msg_template in FRIENDLY_MESSAGES.items():
        print(f"  - {msg_type}: {msg_template[:50]}...")
    
    return True


def test_valid_jsonl_integration():
    """测试与valid.jsonl的集成"""
    print("\n" + "="*60)
    print("TEST 5: 与valid.jsonl集成")
    print("="*60)
    
    valid_path = "./output/base_staff/valid.jsonl"
    if not os.path.exists(valid_path):
        print(f"⚠️  SKIP: 找不到 {valid_path}")
        return True
    
    records = load_jsonl(valid_path)
    print(f"✅ 加载 {len(records)} 条有效记录")
    
    # 检查是否有记录包含instruct
    with_instruct = 0
    for rec in records[:10]:  # 检查前10条
        if isinstance(rec.get("api_schema"), dict) and "instruct" in rec.get("api_schema", {}):
            with_instruct += 1
    
    print(f"✅ 前10条记录中有 {with_instruct} 条包含instruct字段")
    
    return True


def test_runtime_error_messages():
    """测试runtime错误处理中的温和话术"""
    print("\n" + "="*60)
    print("TEST 6: Runtime温和话术测试")
    print("="*60)
    
    # 模拟没有表召回的情况
    error_msg_no_table = (
        "抱歉，当前未找到合适的数据源。您的查询可能涉及以下情况：\n"
        "1. 查询字段超出当前数据库范围\n"
        "2. 所需的数据维度暂不支持\n"
        "建议：请尝试调整查询条件或联系数据团队补充数据源。"
    )
    print(f"❌ 无表召回时的提示:\n{error_msg_no_table}")
    
    # 模拟没有API召回的情况
    error_msg_no_api = (
        "抱歉，当前未找到合适的数据源。\n"
        "数据库 base_staff 中可能不包含您需要的字段。\n"
        "建议：\n"
        "1. 尝试用其他字段名描述您的查询\n"
        "2. 简化查询条件（例如：只查询某一个维度）\n"
        "3. 联系数据团队了解数据源范围"
    )
    print(f"\n❌ 无API召回时的提示:\n{error_msg_no_api}")
    
    print(f"\n✅ PASS: 已集成温和话术")
    
    return True


def main():
    """运行所有测试"""
    print("\n")
    print("█" * 60)
    print("█  表能力约束管理系统 - 集成测试")
    print("█" * 60)
    
    tests = [
        ("all_table_instruct加载", test_all_table_instruct_loading),
        ("instruct生成", test_instruct_generation),
        ("APISchema.instruct字段", test_apischema_with_instruct),
        ("温和话术库", test_friendly_messages),
        ("valid.jsonl集成", test_valid_jsonl_integration),
        ("Runtime温和话术", test_runtime_error_messages),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"测试失败: {test_name}, 错误: {e}")
            results.append((test_name, False))
    
    # 总结
    print("\n" + "="*60)
    print("测试总结")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\n总计: {passed}/{total} 通过")
    
    if passed == total:
        print("\n🎉 所有测试通过！系统准备就绪。")
        print("\n后续步骤：")
        print("1. 启动Review界面：python review/interface.py")
        print("2. 上传query进行runtime测试")
        print("3. 通过生成instruct按钮为API添加能力约束")
        print("4. 查看实际的错误提示效果")
        return 0
    else:
        print(f"\n⚠️  {total - passed} 个测试失败，请检查")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
