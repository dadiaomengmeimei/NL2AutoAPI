#!/usr/bin/env python3
"""
快速构建脚本：100个迭代生成训练数据
使用 config.yaml 配置文件和 main.py build 命令
"""
import os
import sys
import subprocess
import argparse
from pathlib import Path

def main():
    """快速构建主函数"""
    
    parser = argparse.ArgumentParser(description="快速构建脚本（使用配置文件）")
    parser.add_argument("--config", default="../config.yaml", help="配置文件路径")
    parser.add_argument("--schema", help="Schema文件路径（覆盖配置文件）")
    parser.add_argument("--output-dir", help="输出目录（覆盖配置文件）")
    parser.add_argument("--iterations", type=int, help="迭代次数（覆盖配置文件）")
    args = parser.parse_args()
    
    print("="*70)
    print("快速构建脚本：生成训练数据（事前构建）")
    print("="*70)
    
    print(f"\n📋 配置文件: {args.config}")
    
    # 调用main.py build命令
    cmd = [
        sys.executable,
        "main.py",
        "build",
        "--config", args.config
    ]
    
    # 添加覆盖参数
    if args.schema:
        cmd.extend(["--schema", args.schema])
    if args.output_dir:
        cmd.extend(["--output-dir", args.output_dir])
    if args.iterations:
        cmd.extend(["--iterations", str(args.iterations)])
    
    print(f"\n执行命令: {' '.join(cmd)}\n")
    
    try:
        # 切换到项目根目录执行
        project_root = Path(__file__).parent.parent
        result = subprocess.run(cmd, cwd=str(project_root))
        
        if result.returncode == 0:
            print("\n" + "="*70)
            print("✅ 构建完成！")
            print("="*70)
            
            # 从配置文件读取 output_dir
            output_dir = args.output_dir
            if not output_dir:
                # 尝试从配置文件读取
                try:
                    sys.path.insert(0, str(project_root))
                    from core.config_loader import load_config
                    config = load_config(args.config)
                    output_dir = config.build.output_dir
                except:
                    output_dir = "./output"
            
            output_path = project_root / output_dir
            
            # 统计结果（按表目录）
            total_valid = 0
            total_invalid = 0
            per_table = []
            
            if output_path.exists():
                for name in os.listdir(output_path):
                    table_dir = output_path / name
                    if not table_dir.is_dir():
                        continue
                    valid_file = table_dir / "valid.jsonl"
                    invalid_file = table_dir / "invalid.jsonl"
                    if not valid_file.exists() and not invalid_file.exists():
                        continue
                    
                    valid_count = 0
                    invalid_count = 0
                    if valid_file.exists():
                        with open(valid_file) as f:
                            valid_count = len(f.readlines())
                    if invalid_file.exists():
                        with open(invalid_file) as f:
                            invalid_count = len(f.readlines())
                    
                    total_valid += valid_count
                    total_invalid += invalid_count
                    per_table.append((name, valid_count, invalid_count))
            
            print(f"\n📊 结果统计：")
            if per_table:
                for name, v_count, i_count in sorted(per_table):
                    print(f"  {name}: 有效 {v_count} 条, 无效 {i_count} 条")
                print(f"  总计: 有效 {total_valid} 条, 无效 {total_invalid} 条, 合计 {total_valid + total_invalid} 条")
            else:
                print(f"  未找到输出数据，请检查 {output_path}")
            
            print(f"\n🎯 后续步骤：")
            print(f"  1. 启动Review界面: python main.py review --config {args.config}")
            print(f"  2. 在'无效记录审核'中处理无效数据")
            print(f"  3. 点击'🧠 自动生成API+SQL'按钮或手动编辑")
            print(f"  4. 选择'✅ 批准'将记录移到valid集合")
            
            return 0
        else:
            print(f"\n❌ 构建失败，退出码: {result.returncode}")
            return result.returncode
        
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
