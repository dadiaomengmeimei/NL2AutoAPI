#!/usr/bin/env python3
"""
配置系统测试脚本
"""

import os
import sys

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_config_loading():
    """测试配置加载"""
    print("\n" + "="*60)
    print("测试 1: 配置文件加载")
    print("="*60)
    
    from core.config_loader import load_config
    config = load_config('./config.yaml')
    
    print(f"✓ 配置加载成功")
    print(f"  - 数据库: {config.database.host}:{config.database.port}/{config.database.database}")
    print(f"  - Schema: {config.schema.path}")
    print(f"  - 输出目录: {config.build.output_dir}")
    print(f"  - 迭代次数: {config.build.iterations}")
    
    return True

def test_env_override():
    """测试环境变量覆盖"""
    print("\n" + "="*60)
    print("测试 2: 环境变量覆盖")
    print("="*60)
    
    # 设置测试环境变量
    os.environ['DB_HOST'] = 'test.example.com'
    os.environ['DB_PORT'] = '3307'
    
    from core.config_loader import ConfigLoader
    loader = ConfigLoader('./config.yaml')
    config = loader.load()
    
    print(f"  - 环境变量 DB_HOST: test.example.com")
    print(f"  - 加载结果: {config.database.host}")
    
    assert config.database.host == 'test.example.com', "环境变量覆盖失败"
    assert config.database.port == 3307, "端口环境变量覆盖失败"
    
    print(f"✓ 环境变量覆盖测试通过")
    
    # 恢复环境变量
    os.environ.pop('DB_HOST', None)
    os.environ.pop('DB_PORT', None)
    
    return True

def test_global_config_update():
    """测试全局配置更新"""
    print("\n" + "="*60)
    print("测试 3: 全局配置更新")
    print("="*60)
    
    from core.config_loader import get_config_loader
    loader = get_config_loader('./config.yaml')
    loader.update_all_configs()
    
    from core.config import db_config, llm_config, pipeline_config
    
    print(f"  - db_config.host: {db_config.host}")
    print(f"  - db_config.database: {db_config.database}")
    print(f"  - llm_config.model: {llm_config.model}")
    print(f"  - pipeline_config.iterations: {pipeline_config.iterations}")
    
    print(f"✓ 全局配置更新成功")
    
    return True

def main():
    """运行所有测试"""
    print("="*60)
    print("NL2AutoAPI 配置系统测试")
    print("="*60)
    
    tests = [
        test_config_loading,
        test_env_override,
        test_global_config_update,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ 测试失败: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "="*60)
    print(f"测试结果: {passed} 通过, {failed} 失败")
    print("="*60)
    
    if failed == 0:
        print("\n✅ 所有测试通过！配置系统正常工作\n")
        return 0
    else:
        print(f"\n❌ 有 {failed} 个测试失败\n")
        return 1

if __name__ == "__main__":
    sys.exit(main())
