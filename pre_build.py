#!/usr/bin/env python3
"""
事前构建：预生成API Schema

步骤：
1. 规则生成（基础覆盖）
2. LLM生成（复杂查询）
3. 验证与过滤
4. 导出
"""

import argparse
import sys
import os
import json

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.config import pipeline_config, PipelineConfig
from core.config import db_config
from core.config import llm_config
from core.config_loader import get_config_loader
from core.database import db_manager
from schema.loader import SchemaLoader
from schema.db_schema_builder import build_schema_from_db
from generation.rule_based import run_advanced_rule_pipeline  # 需要创建
from generation.pipeline import GenerationPipeline
from tools.export_schemas import SchemaExporter
from validation.query_gate import QueryCommonSenseGate


def _safe_name(text: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in (text or ""))


def _auto_schema_filename(db_name: str, tables_arg: str) -> str:
    db = _safe_name(db_name or "default_db")
    tables = [t.strip() for t in (tables_arg or "").split(",") if t.strip()]
    safe_tables = [_safe_name(t) for t in tables if _safe_name(t)]

    if not safe_tables:
        suffix = "all_tables"
    elif len(safe_tables) == 1:
        suffix = safe_tables[0]
    else:
        head = "_".join(safe_tables[:2])
        suffix = f"multi_{len(safe_tables)}_{head}"

    return f"schema_from_db_{db}_{suffix}.json"


def main():
    parser = argparse.ArgumentParser(description="NL2AutoAPI Pre-Build Pipeline")
    parser.add_argument("--config", default="./config.yaml", help="配置文件路径")
    parser.add_argument("--schema", help="Schema JSON文件路径")
    parser.add_argument("--tables", default="", help="按逗号分隔的表名，仅预热这些表")
    parser.add_argument("--schema-out", default="", help="自动从DB构建schema时的落盘路径")
    parser.add_argument("--output-dir", default="./output")
    parser.add_argument("--iterations", type=int, default=100, 
                       help="LLM生成迭代次数")
    parser.add_argument("--rule-rounds", type=int, default=1, help="Layer-A规则生成轮次")
    parser.add_argument("--require-db", action="store_true", help="严格要求数据库可连接")
    parser.add_argument("--require-llm", action="store_true", help="严格要求LLM API Key可用")
    parser.add_argument("--skip-rule", action="store_true", 
                       help="跳过规则生成")
    parser.add_argument("--skip-llm", action="store_true",
                       help="跳过LLM生成")
    parser.add_argument("--disable-query-gate", action="store_true", help="关闭预热query常识过滤")
    parser.add_argument("--export-format", default="json",
                       choices=["json", "openapi", "mcp", "all"])
    
    args = parser.parse_args()

    config_loader = get_config_loader(args.config)
    config = config_loader.load()
    config_loader.update_all_configs()
    
    # 更新配置
    pipeline_config.output_dir = args.output_dir or config.build.output_dir
    pipeline_config.iterations = args.iterations
    pipeline_config.valid_path = os.path.join(pipeline_config.output_dir, "dataset_valid.jsonl")
    pipeline_config.invalid_path = os.path.join(pipeline_config.output_dir, "dataset_invalid.jsonl")
    os.makedirs(args.output_dir, exist_ok=True)

    # 获取数据库连接（schema自动构建与后续规则/执行都可能依赖）
    db_conn = db_manager.connect()
    if db_conn is None:
        print(f"\n⚠️ 无法连接数据库，已进入无DB模式，可能部分规则依赖不会执行")
        if args.require_db:
            raise RuntimeError("严格模式要求数据库可用，但当前数据库连接失败")
    else:
        print(f"\n数据库连接成功")

    if args.require_llm and not (llm_config.api_key or "").strip():
        raise RuntimeError("严格模式要求LLM可用，但未检测到 LLM_API_KEY/OPENAI_API_KEY")

    # schema来源：优先使用 --schema，否则按数据库+表名动态构建
    schema_path = args.schema
    if not schema_path:
        if db_conn is None:
            raise ValueError("未提供 --schema 且数据库不可用，无法构建预热schema")

        inferred_schema = build_schema_from_db(
            db_conn=db_conn,
            db_name=db_config.database,
            table_names=args.tables,
        )
        if args.schema_out and args.schema_out.strip():
            schema_path = args.schema_out.strip()
        else:
            auto_name = _auto_schema_filename(db_config.database, args.tables)
            schema_path = os.path.join(args.output_dir, auto_name)
        with open(schema_path, "w", encoding="utf-8") as f:
            json.dump(inferred_schema, f, ensure_ascii=False, indent=2)
        print(f"自动构建Schema完成: {schema_path}")

    pipeline_config.schema_file = schema_path
    
    # 加载schema
    print(f"\n{'='*60}")
    print("🚀 NL2AutoAPI 事前构建启动")
    print(f"{'='*60}")
    
    loader = SchemaLoader(schema_path)
    schema = loader.load()
    
    print(f"加载Schema: {len(schema.tables)} 个表")
    for name, info in schema.tables.items():
        print(f"  - {name}: {len(info.fields)} 个字段")
    
    valid_path = pipeline_config.valid_path
    invalid_path = pipeline_config.invalid_path
    gate = None if args.disable_query_gate else QueryCommonSenseGate(args.output_dir)
    
    # 步骤1: 规则生成
    if not args.skip_rule:
        print(f"\n{'='*60}")
        print("步骤1: 规则生成（基础覆盖）")
        print(f"{'='*60}")
        rounds = max(1, int(args.rule_rounds))
        for round_idx in range(rounds):
            print(f"\n[Layer-A] round {round_idx + 1}/{rounds}")
            run_advanced_rule_pipeline(
                db_conn=db_conn,
                full_schema=schema.dict(),
                valid_path=valid_path,
                invalid_path=invalid_path,
                gate=gate,
                mode="bootstrap" if round_idx == 0 else "augment",
            )
    
    # 步骤2: LLM生成
    if not args.skip_llm:
        print(f"\n{'='*60}")
        print("步骤2: LLM生成（复杂查询）")
        print(f"{'='*60}")
        
        pipeline = GenerationPipeline(
            db_conn=db_conn,
            schema=loader,
            valid_path=valid_path,
            invalid_path=invalid_path,
            gate=gate,
        )
        
        pipeline.run(iterations=args.iterations, do_round_trip=True)
    
    # 步骤3: 导出
    print(f"\n{'='*60}")
    print("步骤3: 导出Schema")
    print(f"{'='*60}")
    
    exporter = SchemaExporter(os.path.join(args.output_dir, "schemas_by_table"))
    formats = ["json", "openapi", "mcp"] if args.export_format == "all" else [args.export_format]
    
    table_names = list(schema.tables.keys()) if hasattr(schema, "tables") else []

    for fmt in formats:
        print(f"\n导出格式: {fmt}")
        exporter.export(valid_path, fmt, allowed_tables=table_names)

    # 可选清理输出目录中不需要的表目录（只保留真实表）
    for d in os.listdir(args.output_dir):
        path = os.path.join(args.output_dir, d)
        if d in ["schemas_by_table", "dataset_valid.jsonl", "dataset_invalid.jsonl"]:
            continue
        if os.path.isdir(path) and d not in table_names:
            import shutil
            shutil.rmtree(path, ignore_errors=True)

    # 输出按表的 valid/invalid 目录（output/<table>/valid.jsonl/invalid.jsonl）
    print("\n[Export] 开始生成按表 valid/invalid 数据目录")
    exporter.export_records_by_table(valid_path, invalid_path, args.output_dir, allowed_tables=table_names)

    # 清理根目录输出，只保留 output/<table>/ 结构
    for filename in ["dataset_valid.jsonl", "dataset_invalid.jsonl", "all_table_instruct.json"]:
        root_path = os.path.join(args.output_dir, filename)
        if os.path.exists(root_path):
            os.remove(root_path)

    print(f"\n{'='*60}")
    print("✅ 事前构建完成")
    print(f"  输出目录: {args.output_dir}")
    print(f"  Valid: {valid_path}")
    print(f"  Invalid: {invalid_path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()