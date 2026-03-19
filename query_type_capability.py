"""
query_type_capability.py
判断每个表是否支持指定 query_type
"""
def table_supports_query_type(table_info, query_type):
    fields = table_info.get('fields', {})
    if isinstance(fields, list):
        fields = {f['name']: f for f in fields}
    # 常见类型能力判断
    if query_type in ('numeric_stats', 'aggregate_no_filter'):
        # 需有数值列
        for f in fields.values():
            t = str(f.get('type', '')).upper()
            if any(x in t for x in ['INT', 'FLOAT', 'DECIMAL', 'DOUBLE']):
                return True
        return False
    elif query_type in ('group_distribution', 'group_aggregate'):
        # 需有低基数列
        for f in fields.values():
            if f.get('d_cnt', 100) <= 12:
                return True
        return False
    elif query_type in ('list_no_filter', 'exact_query', 'aggregate_with_filter'):
        # 只要有字段即可
        return bool(fields)
    else:
        return True

# 用法示例：
# from query_type_capability import table_supports_query_type
# table_supports_query_type(table_info, 'numeric_stats')
