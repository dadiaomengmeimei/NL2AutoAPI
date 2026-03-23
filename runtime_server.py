#!/usr/bin/env python3
"""
事中服务：线上运行时服务

提供：
1. API查询路由
2. 实时纠错任务生成
3. 执行结果验证
"""

import argparse
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.config import pipeline_config
from core.database import db_manager
from runtime.registry import APIRegistry
from runtime.router import RuntimeRouter
from review.submitter import ReviewSubmitter
from validation.llm_judge import LLMJudge


def interactive_mode(router: RuntimeRouter):
    """交互式模式"""
    print("\n" + "="*60)
    print("NL2AutoAPI 运行时服务 - 交互式模式")
    print("输入 'quit' 或 'exit' 退出")
    print("="*60 + "\n")
    
    while True:
        query = input("\n请输入查询 > ").strip()
        if query.lower() in ("quit", "exit", "q"):
            break
        if not query:
            continue
        
        start_time = time.time()
        result = router.route(query)
        latency = (time.time() - start_time) * 1000
        
        print(f"\n{'='*60}")
        print(f"结果 (耗时: {latency:.1f}ms):")
        print(f"  状态: {result.status}")
        print(f"  API: {result.api_name or 'N/A'}")
        print(f"  参数: {result.params or {}}")
        print(f"  行数: {result.row_count}")
        
        if result.status == "success":
            if result.columns:
                print(f"  列: {result.columns}")
            if result.data:
                print(f"  数据 (前3行):")
                for row in result.data[:3]:
                    print(f"    {row}")
        else:
            print(f"  错误: {result.error}")
            if result.correction_needed:
                print(f"  [!] 已生成纠错审核任务")
        
        if result.verification:
            print(f"  验证: {result.verification.type} - {result.verification.reason[:50]}...")
        print(f"{'='*60}")


def test_mode(router: RuntimeRouter, valid_path: str, num_tests: int = 10):
    """测试模式：从valid集采样测试"""
    from core.utils import load_jsonl
    import random
    
    records = load_jsonl(valid_path)
    if not records:
        print("No valid records found")
        return
    
    test_records = random.sample(records, min(num_tests, len(records)))
    
    print(f"\n{'='*60}")
    print(f"运行测试: {len(test_records)} 条")
    print(f"{'='*60}")
    
    success = 0
    api_hit = 0
    need_correction = 0
    
    for idx, record in enumerate(test_records, 1):
        query = record.get("query", "")
        expected_api = record.get("api_schema", {}).get("name", "")
        
        print(f"\n--- 测试 {idx}/{len(test_records)} ---")
        print(f"Query: {query[:60]}...")
        print(f"Expected: {expected_api}")
        
        result = router.route(query)
        
        if result.status == "success":
            success += 1
            flag = "✅"
        else:
            flag = "❌"
        
        if result.api_name == expected_api:
            api_hit += 1
            hit_flag = "✓"
        else:
            hit_flag = "✗"
        
        if result.correction_needed:
            need_correction += 1
        
        print(f"{flag} status={result.status} | API命中={hit_flag} (got={result.api_name}) | 需纠错={result.correction_needed}")
    
    total = len(test_records)
    print(f"\n{'='*60}")
    print(f"测试完成: {total} 条")
    print(f"  执行成功率: {success}/{total} ({100*success//total}%)")
    print(f"  API召回命中率: {api_hit}/{total} ({100*api_hit//total}%)")
    print(f"  需纠错比例: {need_correction}/{total} ({100*need_correction//total}%)")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="NL2AutoAPI Runtime Server")
    parser.add_argument("--valid-path", required=True, help="Valid dataset path")
    parser.add_argument("--mode", choices=["interactive", "test", "online"], default="interactive")
    parser.add_argument("--num-tests", type=int, default=10, help="Test mode sample size")
    parser.add_argument("--review-queue", default="", help="审核队列路径，默认落在 valid.jsonl 同目录")
    parser.add_argument("--table-name", default="my_table", help="Table name for online mode")
    parser.add_argument("--table-desc", default="", help="Table description for online query generation")
    parser.add_argument("--num-queries", type=int, default=20, help="Online mode query count")
    parser.add_argument("--batch-size", type=int, default=5, help="Online mode batch size")
    parser.add_argument("--max-rounds", type=int, default=3, help="Online mode retry rounds")
    
    args = parser.parse_args()

    if not args.review_queue:
        args.review_queue = os.path.join(os.path.dirname(args.valid_path) or ".", "review_queue.jsonl")
    
    # 初始化
    print(f"Loading API registry from: {args.valid_path}")
    registry = APIRegistry(args.valid_path)
    print(f"Loaded {len(registry.apis)} APIs")
    
    submitter = ReviewSubmitter(args.review_queue)

    # Load config and sync global db_config / llm_config / pipeline_config
    from core.config_loader import get_config_loader
    _loader = get_config_loader()
    _cfg = _loader.load()
    _loader.update_all_configs()
    table_top_k = getattr(_cfg.runtime, 'table_top_k', 3)
    api_top_k = getattr(_cfg.runtime, 'api_top_k', 5)

    router = RuntimeRouter(
        registry, submitter, enable_verify=True,
        table_top_k=table_top_k, api_top_k=api_top_k,
    )
    
    # 数据库连接测试
    conn = db_manager.connect()
    if conn is not None:
        from core.config import db_config
        print(f"Database connected: {db_config.host}:{db_config.port}/{db_config.database}")
    else:
        # 连接失败也允许继续，进入无DB模式（某些场景仅做候选+校验不跑真实SQL）
        print("Database connection failed, running in no-DB mode. 请确认环境变量 DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME 是否设置正确。")
    
    # 运行模式
    if args.mode == "interactive":
        interactive_mode(router)
    elif args.mode == "test":
        test_mode(router, args.valid_path, args.num_tests)
    else:  # online
        from runtime.online_runtime import run_runtime_loop

        table_desc = args.table_desc
        if not table_desc:
            # 从schema中的comment默认提取
            if os.path.exists(args.valid_path):
                from core.utils import load_jsonl
                records = load_jsonl(args.valid_path)
                if records:
                    first = records[0]
                    table_desc = first.get("api_schema", {}).get("table", "")

        if not table_desc:
            raise ValueError("online模式需要--table-desc或valid-path中有效描述")

        run_runtime_loop(
            router=router,
            table_name=args.table_name,
            table_desc=table_desc,
            num_queries=args.num_queries,
            batch_size=args.batch_size,
            max_rounds=args.max_rounds,
            output_dir=os.path.dirname(args.valid_path) or ".",
        )


if __name__ == "__main__":
    main()