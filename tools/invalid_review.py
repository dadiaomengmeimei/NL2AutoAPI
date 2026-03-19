#!/usr/bin/env python3
"""
无效记录修复工具（命令行版）

用于批量处理invalid记录，无需启动Web界面
"""

import argparse
import json
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


from review.models import ReviewStatus
from core.utils import load_jsonl, save_jsonl


def show_invalid_record(record: dict, index: int, total: int):
    """展示无效记录详情"""
    print(f"\n{'='*60}")
    print(f"记录 {index}/{total} | ID: {record.get('id', 'unknown')}")
    print(f"{'='*60}")
    
    print(f"\n[失效原因] {record.get('invalid_reason', 'unknown')}")
    
    print(f"\n[原始SQL]")
    sql = record.get('sql', 'N/A')
    print(f"  {sql[:200]}{'...' if len(sql) > 200 else ''}")
    
    print(f"\n[Query] {record.get('query', 'N/A')[:100]}")
    
    api_schema = record.get('api_schema', {})
    print(f"\n[API信息]")
    print(f"  Name: {api_schema.get('name', 'N/A')}")
    print(f"  Desc: {api_schema.get('description', 'N/A')[:80]}")
    print(f"  Type: {api_schema.get('query_type', 'N/A')}")
    
    print(f"\n[边界示例] {len(record.get('boundary_examples', []))} 个")
    for ex in record.get('boundary_examples', [])[:3]:
        print(f"  - {ex.get('query', 'N/A')[:60]}")


def interactive_fix(record: dict) -> dict:
    """交互式修复记录"""
    print(f"\n{'-'*60}")
    print("开始修复（直接回车保留原值）")
    print(f"{'-'*60}")
    
    # 修复Query
    current_query = record.get('query', '')
    print(f"\n当前Query: {current_query[:100]}")
    new_query = input("新Query > ").strip()
    if new_query:
        record['query'] = new_query
    
    # 修复API Name
    current_name = record.get('api_schema', {}).get('name', '')
    print(f"\n当前API Name: {current_name}")
    new_name = input("新API Name > ").strip()
    if new_name:
        record['api_schema']['name'] = new_name
    
    # 修复Description
    current_desc = record.get('api_schema', {}).get('description', '')
    print(f"\n当前Description: {current_desc[:100]}")
    new_desc = input("新Description > ").strip()
    if new_desc:
        record['api_schema']['description'] = new_desc
    
    # 修复SQL
    current_sql = record.get('sql', '')
    print(f"\n当前SQL: {current_sql[:100]}...")
    print("输入新SQL（多行，输入END结束）：")
    lines = []
    while True:
        line = input()
        if line.strip().upper() == 'END':
            break
        lines.append(line)
    new_sql = '\n'.join(lines).strip()
    if new_sql:
        record['sql'] = new_sql
    
    # 标记修复状态
    record['fixed'] = True
    record['fixed_at'] = __import__('datetime').datetime.now().isoformat()
    
    return record


def batch_auto_fix(records: list[dict], strategy: str = "simple") -> list[dict]:
    """批量自动修复策略"""
    fixed = []
    
    for record in records:
        reason = record.get('invalid_reason', '')
        
        # 简单修复策略：补充描述、规范化名称
        if strategy == "simple":
            # 补充描述
            api = record.get('api_schema', {})
            if not api.get('description') or len(api.get('description', '')) < 20:
                query_type = record.get('query_type', 'unknown')
                table = record.get('table', 'unknown')
                api['description'] = f"【自动修复】查询{table}的{query_type}类型数据"
                record['api_schema'] = api
            
            # 规范化名称
            name = api.get('name', '')
            if name:
                # 移除非法字符
                safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
                api['name'] = safe_name[:50]
                record['api_schema'] = api
            
            record['auto_fixed'] = True
        
        fixed.append(record)
    
    return fixed


def export_for_manual_review(records: list[dict], output_path: str):
    """导出为人工审核格式"""
    review_tasks = []
    
    for idx, record in enumerate(records):
        task = {
            "task_id": f"invalid_{idx:04d}",
            "task_type": "invalid_recovery",
            "status": "pending",
            "source_query": record.get('query', ''),
            "source_api_name": record.get('api_schema', {}).get('name'),
            "proposed_schema": record.get('api_schema'),
            "original_sql": record.get('sql'),
            "invalid_reason": record.get('invalid_reason'),
            "metadata": {
                "table": record.get('table'),
                "query_type": record.get('query_type'),
                "shard_id": record.get('shard_id')
            }
        }
        review_tasks.append(task)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for task in review_tasks:
            f.write(json.dumps(task, ensure_ascii=False) + '\n')
    
    print(f"已导出 {len(review_tasks)} 条审核任务到: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="无效记录修复工具")
    parser.add_argument("--invalid-path", required=True, help="Invalid数据集路径")
    parser.add_argument("--valid-path", required=True, help="Valid数据集路径（修复后追加）")
    parser.add_argument("--mode", choices=["interactive", "auto", "export"], 
                       default="interactive", help="修复模式")
    parser.add_argument("--max-records", type=int, default=10, 
                       help="最大处理记录数")
    parser.add_argument("--export-path", default="./review_queue.jsonl",
                       help="导出审核任务路径")
    
    args = parser.parse_args()
    
    # 加载数据
    print(f"Loading invalid records from: {args.invalid_path}")
    invalid_records = load_jsonl(args.invalid_path)
    print(f"Loaded {len(invalid_records)} invalid records")
    
    valid_records = load_jsonl(args.valid_path)
    print(f"Loaded {len(valid_records)} valid records")
    
    # 限制处理数量
    to_process = invalid_records[:args.max_records]
    
    if args.mode == "interactive":
        fixed_records = []
        
        for idx, record in enumerate(to_process, 1):
            show_invalid_record(record, idx, len(to_process))
            
            action = input("\n操作: [f]修复 [s]跳过 [q]退出 > ").strip().lower()
            
            if action == 'q':
                break
            elif action == 's':
                continue
            elif action == 'f':
                fixed = interactive_fix(record)
                fixed_records.append(fixed)
                
                # 实时保存
                save_jsonl(fixed_records, args.valid_path, append=False)
                print(f"✓ 已保存 {len(fixed_records)} 条修复记录")
        
        print(f"\n{'='*60}")
        print(f"修复完成: {len(fixed_records)}/{len(to_process)}")
        print(f"有效数据已更新: {args.valid_path}")
        
    elif args.mode == "auto":
        fixed_records = batch_auto_fix(to_process, strategy="simple")
        save_jsonl(fixed_records, args.valid_path, append=False)
        print(f"自动修复完成: {len(fixed_records)} 条")
        
    elif args.mode == "export":
        export_for_manual_review(to_process, args.export_path)
        print(f"已导出到审核队列: {args.export_path}")


if __name__ == "__main__":
    main()