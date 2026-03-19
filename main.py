#!/usr/bin/env python3
"""NL2AutoAPI 统一命令行入口。"""

import argparse
import os
import sys

from core.logger import setup_logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def build_parser() -> argparse.ArgumentParser:
    """构建 CLI 参数解析器。"""
    parser = argparse.ArgumentParser(description="NL2AutoAPI - 智能 API 生成、运行时路由与在线测试系统")
    parser.add_argument("--config", default="./config.yaml", help="配置文件路径")
    parser.add_argument("--log-dir", help="日志输出目录（覆盖配置文件）")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    build_parser = subparsers.add_parser("build", help="预热阶段：预生成 API 与训练样本")
    build_parser.add_argument("--config", default="./config.yaml", help="配置文件路径")
    build_parser.add_argument("--schema", help="Schema JSON 文件（覆盖配置文件）")
    build_parser.add_argument("--tables", help="按逗号分隔的表名，仅预热这些表")
    build_parser.add_argument("--schema-out", help="未提供 --schema 时自动构建 schema 的输出路径")
    build_parser.add_argument("--output-dir", help="输出目录（覆盖配置文件）")
    build_parser.add_argument("--iterations", type=int, help="迭代次数（覆盖配置文件）")
    build_parser.add_argument("--skip-rule", action="store_true", help="跳过规则生成")
    build_parser.add_argument("--skip-llm", action="store_true", help="跳过 LLM 生成")

    serve_parser = subparsers.add_parser("serve", help="runtime / online-test 阶段入口")
    serve_parser.add_argument("--config", default="./config.yaml", help="配置文件路径")
    serve_parser.add_argument("--valid-path", help="valid.jsonl 路径（覆盖配置文件）")
    serve_parser.add_argument("--review-queue", help="review_queue.jsonl 路径（覆盖配置文件）")
    serve_parser.add_argument("--mode", choices=["interactive", "test", "online"], help="运行模式（interactive/test/online）")
    serve_parser.add_argument("--num-tests", type=int, help="测试数量（覆盖配置文件）")

    feedback_parser = subparsers.add_parser("feedback", help="事后反馈：基于线上案例扩展 API")
    feedback_parser.add_argument("--config", default="./config.yaml", help="配置文件路径")
    feedback_parser.add_argument("--valid-path", help="valid.jsonl 路径（覆盖配置文件）")
    feedback_parser.add_argument("--min-samples", type=int, default=5)
    feedback_parser.add_argument("--auto-submit", action="store_true")

    review_parser = subparsers.add_parser("review", help="启动审核界面")
    review_parser.add_argument("--config", default="./config.yaml", help="配置文件路径")
    review_parser.add_argument("--port", type=int, help="端口号（覆盖配置文件）")
    review_parser.add_argument("--share", action="store_true")
    review_parser.add_argument("--invalid-path", help="invalid.jsonl 路径（覆盖配置文件）")
    review_parser.add_argument("--valid-path", help="valid.jsonl 路径（覆盖配置文件）")
    review_parser.add_argument("--review-queue", help="review_queue.jsonl 路径（覆盖配置文件）")
    review_parser.add_argument("--recorrect-path", help="recorrect.jsonl 路径（覆盖配置文件）")

    export_parser = subparsers.add_parser("export", help="导出 Schema")
    export_parser.add_argument("input", help="Input valid.jsonl")
    export_parser.add_argument("--output-dir", default="./output/schemas_by_table")
    export_parser.add_argument("--format", choices=["json", "openapi", "mcp", "all"], default="json")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    from core.config_loader import get_config_loader

    config_path = getattr(args, 'config', './config.yaml')
    loader = get_config_loader(config_path)
    config = loader.load()
    loader.update_all_configs()

    log_dir = args.log_dir if args.log_dir else config.logging.log_dir
    setup_logging(log_dir=log_dir)

    if args.command == "build":
        schema = args.schema if args.schema else config.schema.path
        config_tables = getattr(config.schema, "table_names", []) or []
        tables = args.tables if args.tables else ",".join(config_tables)
        schema_out = args.schema_out if args.schema_out else ""
        output_dir = args.output_dir if args.output_dir else config.build.output_dir
        iterations = args.iterations if args.iterations else config.build.iterations
        rule_rounds = getattr(config.build, "rule_rounds", 1)

        sys.argv = [
            "pre_build.py",
            "--output-dir", output_dir,
            "--iterations", str(iterations),
            "--rule-rounds", str(rule_rounds),
        ]
        if schema:
            sys.argv.extend(["--schema", schema])
        if tables:
            sys.argv.extend(["--tables", tables])
        if schema_out:
            sys.argv.extend(["--schema-out", schema_out])
        if args.skip_rule or config.build.skip_rule:
            sys.argv.append("--skip-rule")
        if args.skip_llm or config.build.skip_llm:
            sys.argv.append("--skip-llm")
        if getattr(config.build, "require_db", False):
            sys.argv.append("--require-db")
        if getattr(config.build, "require_llm", False):
            sys.argv.append("--require-llm")
        if not getattr(config.build, "enable_query_gate", True):
            sys.argv.append("--disable-query-gate")

        import pre_build
        pre_build.main()
        return 0

    elif args.command == "serve":
        valid_path = args.valid_path if args.valid_path else config.runtime.valid_path
        review_queue = args.review_queue if args.review_queue else config.runtime.review_queue
        mode = args.mode if args.mode else config.runtime.mode
        num_tests = args.num_tests if args.num_tests else config.runtime.num_tests

        if not valid_path:
            print("❌ 错误: 必须通过 --valid-path 或配置文件指定 runtime.valid_path")
            return 1

        sys.argv = [
            "runtime_server.py",
            "--valid-path", valid_path,
            "--review-queue", review_queue,
            "--mode", mode,
            "--num-tests", str(num_tests),
        ]

        import runtime_server
        runtime_server.main()
        return 0

    elif args.command == "feedback":
        valid_path = args.valid_path if args.valid_path else config.runtime.valid_path
        if not valid_path:
            print("❌ 错误: 必须通过 --valid-path 或配置文件指定 runtime.valid_path")
            return 1

        sys.argv = [
            "post_feedback.py",
            "--valid-path", valid_path,
            "--min-samples", str(args.min_samples),
        ]
        if args.auto_submit:
            sys.argv.append("--auto-submit")

        import post_feedback
        post_feedback.main()
        return 0

    elif args.command == "review":
        port = args.port if args.port else config.review.port
        invalid_path = args.invalid_path if args.invalid_path else config.review.invalid_path
        valid_path = args.valid_path if args.valid_path else config.review.valid_path
        review_queue_path = args.review_queue if args.review_queue else config.review.review_queue
        recorrect_path = args.recorrect_path if args.recorrect_path else config.review.recorrect_path
        
        from review.interface import ReviewInterface
        from review.i18n import set_language
        
        auth_users = getattr(config.review, 'auth_users', []) or []
        language = getattr(config.review, 'language', 'en') or 'en'
        set_language(language)
        
        interface = ReviewInterface(
            invalid_path=invalid_path,
            recorrect_path=recorrect_path,
            review_queue_path=review_queue_path,
            valid_path=valid_path,
            auth_users=auth_users,
        )
        interface.launch(server_port=port, share=args.share)
        return 0

    elif args.command == "export":
        sys.argv = [
            "tools/export_schemas.py",
            args.input,
            "--output-dir", args.output_dir,
            "--format", args.format,
        ]

        import tools.export_schemas as export_tool
        export_tool.main()
        return 0

    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())