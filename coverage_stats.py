"""
coverage_stats.py
统计分层生成的 query 数量和覆盖率
"""
import json
import os
from collections import Counter

def load_jsonl(path):
    with open(path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]

def main():
    # 默认路径，可根据实际情况调整
    valid_path = 'output/prebuild_valid.jsonl'
    invalid_path = 'output/prebuild_invalid.jsonl'
    files = [valid_path, invalid_path]
    layer_counter = Counter()
    query_type_counter = Counter()
    total = 0
    table_type_coverage = {}
    # 加载 schema
    schema_path = 'output/schema.json'  # 需提前导出
    if os.path.exists(schema_path):
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        tables = schema.get('tables', {})
    else:
        tables = {}
    # 统计覆盖
    for file in files:
        if not os.path.exists(file):
            print(f"文件不存在: {file}")
            continue
        data = load_jsonl(file)
        for item in data:
            tag = item.get('layer_tag') or item.get('source_stage') or 'Unknown'
            layer_counter[tag] += 1
            qt = item.get('query_type') or 'Unknown'
            query_type_counter[qt] += 1
            total += 1
            table = item.get('table') or 'Unknown'
            if table not in table_type_coverage:
                table_type_coverage[table] = {}
            table_type_coverage[table][qt] = '已覆盖'
    # 能力判断
    from query_type_capability import table_supports_query_type
    all_types = list(query_type_counter.keys())
    for table, info in tables.items():
        for qt in all_types:
            if qt not in table_type_coverage.get(table, {}):
                if table_supports_query_type(info, qt):
                    table_type_coverage.setdefault(table, {})[qt] = '缺失'
                else:
                    table_type_coverage.setdefault(table, {})[qt] = '不可覆盖'
    print("\n=== 分层统计 ===")
    for layer, cnt in layer_counter.items():
        print(f"{layer}: {cnt} 条 ({cnt/total:.2%})")
    print("\n=== 查询类型统计 ===")
    for qt, cnt in query_type_counter.items():
        print(f"{qt}: {cnt} 条")
    print(f"\n总计: {total} 条")
    print("\n=== 表-类型覆盖矩阵 ===")
    for table, types in table_type_coverage.items():
        print(f"表: {table}")
        for qt, status in types.items():
            print(f"  {qt}: {status}")
        print()

if __name__ == '__main__':
    main()
