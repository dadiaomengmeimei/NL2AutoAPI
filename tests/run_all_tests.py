#!/usr/bin/env python3
"""
全量测试运行器
按顺序执行所有模块的单测
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime

# 测试模块列表（按依赖顺序）
TEST_MODULES = [
    ("core", "tests/test_core.py"),
    ("schema", "tests/test_schema.py"),
    ("generation", "tests/test_generation.py"),
    ("validation", "tests/test_validation.py"),
    ("runtime", "tests/test_runtime.py"),
    ("feedback", "tests/test_feedback.py"),
    ("review", "tests/test_review.py"),
]


def print_banner():
    print("\n" + "="*70)
    print(" "*20 + "NL2AutoAPI 全量测试套件")
    print("="*70)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"项目根目录: {Path(__file__).parent.parent.absolute()}")
    print("-"*70)


def run_test_module(name: str, path: str) -> dict:
    """运行单个测试模块"""
    print(f"\n{'>'*70}")
    print(f"  运行模块: {name}")
    print(f"  测试文件: {path}")
    print(f"{'>'*70}")
    
    start_time = datetime.now()
    
    try:
        result = subprocess.run(
            [sys.executable, path],
            capture_output=True,
            text=True,
            timeout=120,  # 2分钟超时
            cwd=Path(__file__).parent.parent
        )
        
        # 打印输出
        print(result.stdout)
        if result.stderr:
            print("  [STDERR]")
            print(result.stderr[:500])  # 限制错误输出
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # 解析结果
        passed = result.returncode == 0 and "FAIL" not in result.stdout.upper().split("COMPLETED")[0] if "COMPLETED" in result.stdout else result.returncode == 0
        
        return {
            "name": name,
            "path": path,
            "success": passed,
            "returncode": result.returncode,
            "elapsed": elapsed,
            "output_len": len(result.stdout)
        }
        
    except subprocess.TimeoutExpired:
        print(f"  ❌ 超时 (>120s)")
        return {
            "name": name,
            "path": path,
            "success": False,
            "returncode": -1,
            "elapsed": 120,
            "error": "timeout"
        }
    except Exception as e:
        print(f"  ❌ 异常: {e}")
        return {
            "name": name,
            "path": path,
            "success": False,
            "returncode": -2,
            "elapsed": 0,
            "error": str(e)
        }


def print_summary(results: list[dict]):
    """打印测试汇总"""
    print("\n" + "="*70)
    print(" "*25 + "测试汇总")
    print("="*70)
    
    total = len(results)
    passed = sum(1 for r in results if r["success"])
    failed = total - passed
    total_time = sum(r["elapsed"] for r in results)
    
    print(f"\n  总模块数: {total}")
    print(f"  通过: {passed} ✅")
    print(f"  失败: {failed} ❌")
    print(f"  总耗时: {total_time:.2f}s")
    
    print(f"\n  {'模块':<15} {'状态':<10} {'耗时(s)':<10} {'返回码':<10}")
    print("  " + "-"*50)
    
    for r in results:
        status = "✅ PASS" if r["success"] else "❌ FAIL"
        print(f"  {r['name']:<15} {status:<10} {r['elapsed']:<10.2f} {r['returncode']:<10}")
    
    print("\n" + "="*70)
    
    if failed > 0:
        print("  失败的模块:")
        for r in results:
            if not r["success"]:
                print(f"    - {r['name']}: {r.get('error', 'see output above')}")
        print("="*70)
        return 1
    else:
        print("  🎉 所有测试通过!")
        print("="*70)
        return 0


def main():
    print_banner()
    
    results = []
    for name, path in TEST_MODULES:
        result = run_test_module(name, path)
        results.append(result)
    
    return print_summary(results)


if __name__ == "__main__":
    sys.exit(main())