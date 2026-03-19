#!/usr/bin/env python3
"""
事后反馈：基于线上案例进行Schema扩展

步骤：
1. 收集边界案例
2. 查询扩写
3. 生成Schema候选
4. 提交审核
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.database import db_manager
from feedback.case_collector import CaseCollector
from feedback.query_augment import QueryAugmenter
from feedback.schema_expander import SchemaExpander
from review.submitter import ReviewSubmitter
from runtime.registry import APIRegistry


def main():
    parser = argparse.ArgumentParser(description="NL2AutoAPI Post Feedback")
    parser.add_argument("--valid-path", required=True, help="Valid dataset path")
    parser.add_argument("--feedback-path", default="./feedback_cases.jsonl")
    parser.add_argument("--min-samples", type=int, default=5,
                       help="每个案例扩写数量")
    parser.add_argument("--strategies", nargs="+", 
                       default=["semantic", "structural"],
                       choices=["semantic", "structural", "contextual"])
    parser.add_argument("--auto-submit", action="store_true",
                       help="自动提交审核")
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print("🚀 NL2AutoAPI 事后反馈处理")
    print(f"{'='*60}")
    
    # 初始化组件
    registry = APIRegistry(args.valid_path)
    collector = CaseCollector(args.feedback_path)
    augmenter = QueryAugmenter()
    review_queue_path = os.path.join(os.path.dirname(args.valid_path) or ".", "review_queue.jsonl")
    submitter = ReviewSubmitter(review_queue_path)
    expander = SchemaExpander(submitter=submitter)
    
    # 数据库连接
    try:
        db_manager.connect()
        print("Database connected")
    except Exception as e:
        print(f"Database connection failed: {e}")
        return
    
    # 1. 加载已有案例
    print(f"\n{'='*60}")
    print("步骤1: 加载历史案例")
    print(f"{'='*60}")
    
    # 这里简化处理，实际应该从feedback_path加载
    # 为了演示，我们使用valid_path中的query作为种子
    
    # 2. 处理边界案例
    print(f"\n{'='*60}")
    print("步骤2: 识别边界案例并扩写")
    print(f"{'='*60}")
    
    from core.utils import load_jsonl
    records = load_jsonl(args.valid_path)
    
    # 采样一些有代表性的query进行扩写
    import random
    sample_records = random.sample(records, min(5, len(records)))
    
    total_generated = 0
    
    for record in sample_records:
        original_query = record.get("query", "")
        base_api_data = record.get("api_schema", {})
        
        if not original_query or not base_api_data:
            continue
        
        print(f"\n处理: {original_query[:50]}...")
        
        # 构建基础API对象
        from schema.models import APISchema
        try:
            base_api = APISchema(**base_api_data)
        except Exception as e:
            print(f"  [Skip] API解析失败: {e}")
            continue
        
        # 多策略扩写
        all_variants = []
        for strategy in args.strategies:
            variants = augmenter.augment(
                original_query, 
                base_api.name.split("_")[0] if "_" in base_api.name else "unknown",
                num_variants=args.min_samples,
                strategy=strategy
            )
            all_variants.extend(variants)
            print(f"  [{strategy}] 生成 {len(variants)} 个变体")
        
        # 去重
        unique_variants = list(set(all_variants))
        print(f"  去重后: {len(unique_variants)} 个变体")
        
        if len(unique_variants) == 0:
            continue
        
        # 生成Schema候选
        generated = expander.expand_from_case(
            original_query=original_query,
            base_api=base_api,
            augmented_queries=unique_variants[:10],  # 最多处理10个
            auto_submit=args.auto_submit
        )
        
        total_generated += len(generated)
        print(f"  生成 {len(generated)} 个候选Schema")
    
    print(f"\n{'='*60}")
    print(f"✅ 处理完成")
    print(f"  生成候选Schema: {total_generated}")
    print(f"  审核队列: {submitter.submitted_count} 个任务")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()