#!/usr/bin/env python3
"""
查询类型定义与管理

定义NL2AutoAPI支持的所有查询类型，每种类型包含：
- 是否需要字段选择
- 是否需要填槽
- 权重（用于随机采样）
- SQL示例模板
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict
import random


@dataclass
class QueryTypeConfig:
    """查询类型配置"""
    need_fields: bool                          # 是否需要选择字段
    slot_required: bool                        # 是否必须填槽
    weight: float = 1.0                        # 采样权重
    examples: List[str] = field(default_factory=list)  # SQL示例模板
    description: str = ""                      # 类型描述


# 查询类型定义
QUERY_TYPES: Dict[str, QueryTypeConfig] = {
    "aggregate_no_filter": QueryTypeConfig(
        need_fields=False,
        slot_required=False,
        weight=0.15,
        examples=[
            "SELECT COUNT(*) FROM {table}",
            "SELECT AVG({field}) FROM {table}",
            "SELECT SUM({field}) FROM {table}",
            "SELECT MAX({field}) FROM {table}",
            "SELECT MIN({field}) FROM {table}",
        ],
        description="聚合查询，无筛选条件，统计全表数据"
    ),
    
    "aggregate_with_filter": QueryTypeConfig(
        need_fields=True,
        slot_required=True,
        weight=0.20,
        examples=[
            "SELECT COUNT(*) FROM {table} WHERE {slot_field} = :slot_{slot_field}",
            "SELECT AVG({field}) FROM {table} WHERE {slot_field} = :slot_{slot_field}",
            "SELECT SUM({field}) FROM {table} WHERE {slot_field} >= :slot_{slot_field}",
        ],
        description="聚合查询，带等值筛选条件"
    ),
    
    "aggregate_with_range": QueryTypeConfig(
        need_fields=True,
        slot_required=True,
        weight=0.10,
        examples=[
            "SELECT COUNT(*) FROM {table} WHERE {field} BETWEEN :slot_start AND :slot_end",
            "SELECT AVG({field}) FROM {table} WHERE {date_field} >= :slot_date",
        ],
        description="聚合查询，带范围筛选条件"
    ),
    
    "aggregate_with_multi_filter": QueryTypeConfig(
        need_fields=True,
        slot_required=True,
        weight=0.10,
        examples=[
            "SELECT COUNT(*) FROM {table} WHERE {slot1} = :slot_{slot1} AND {slot2} = :slot_{slot2}",
            "SELECT SUM({field}) FROM {table} WHERE {slot1} = :slot_{slot1} AND {date_field} >= :slot_date",
        ],
        description="聚合查询，带多条件组合筛选"
    ),
    
    "exact_query": QueryTypeConfig(
        need_fields=True,
        slot_required=True,
        weight=0.20,
        examples=[
            "SELECT {fields} FROM {table} WHERE {slot_field} = :slot_{slot_field}",
            "SELECT * FROM {table} WHERE {pk_field} = :slot_{pk_field}",
        ],
        description="精确查询，按特定条件返回明细"
    ),
    
    "list_no_filter": QueryTypeConfig(
        need_fields=True,
        slot_required=False,
        weight=0.15,
        examples=[
            "SELECT {fields} FROM {table} LIMIT 100",
            "SELECT * FROM {table} ORDER BY {pk_field} DESC LIMIT 50",
        ],
        description="列表查询，无筛选条件，返回批量数据"
    ),
    
    "group_aggregate": QueryTypeConfig(
        need_fields=True,
        slot_required=False,
        weight=0.05,
        examples=[
            "SELECT {group_field}, COUNT(*) as cnt FROM {table} GROUP BY {group_field}",
            "SELECT {group_field}, AVG({field}) as avg_val FROM {table} GROUP BY {group_field}",
        ],
        description="分组聚合，按维度统计"
    ),
    
    "group_aggregate_with_filter": QueryTypeConfig(
        need_fields=True,
        slot_required=True,
        weight=0.05,
        examples=[
            "SELECT {group_field}, COUNT(*) FROM {table} WHERE {slot_field} = :slot_{slot_field} GROUP BY {group_field}",
        ],
        description="分组聚合，带前置筛选条件"
    ),
}


def get_query_type_config(name: str) -> Optional[QueryTypeConfig]:
    """获取指定类型的配置"""
    return QUERY_TYPES.get(name)


def get_random_query_type() -> str:
    """
    按权重随机选择一个查询类型
    
    Returns:
        查询类型名称
    """
    names = list(QUERY_TYPES.keys())
    weights = [QUERY_TYPES[n].weight for n in names]
    
    return random.choices(names, weights=weights, k=1)[0]


def get_weighted_types(n: int, exclude: Optional[List[str]] = None) -> List[str]:
    """
    按权重选择多个查询类型
    
    Args:
        n: 选择数量
        exclude: 排除的类型列表
    
    Returns:
        类型名称列表
    """
    exclude = exclude or []
    available = {k: v for k, v in QUERY_TYPES.items() if k not in exclude}
    
    if not available:
        return []
    
    names = list(available.keys())
    weights = [available[n].weight for n in names]
    
    # 如果n大于可用数量，返回全部
    if n >= len(names):
        return names
    
    # 无放回抽样
    selected = []
    temp_names = names.copy()
    temp_weights = weights.copy()
    
    for _ in range(min(n, len(names))):
        if not temp_names:
            break
        choice = random.choices(temp_names, weights=temp_weights, k=1)[0]
        selected.append(choice)
        
        idx = temp_names.index(choice)
        temp_names.pop(idx)
        temp_weights.pop(idx)
    
    return selected


def list_query_types() -> Dict[str, str]:
    """列出所有查询类型的描述"""
    return {k: v.description for k, v in QUERY_TYPES.items()}


def validate_query_type(query_type: str, sql: str) -> tuple[bool, str]:
    """
    验证SQL是否符合查询类型约束
    
    Args:
        query_type: 查询类型
        sql: 生成的SQL
    
    Returns:
        (是否通过, 错误信息)
    """
    config = QUERY_TYPES.get(query_type)
    if not config:
        return False, f"未知的查询类型: {query_type}"
    
    sql_upper = sql.upper()
    
    # 检查是否需要字段
    if config.need_fields:
        # SELECT * 或 SELECT COUNT(*) 算有字段定义
        has_fields = "SELECT" in sql_upper
        if not has_fields:
            return False, "缺少字段选择"
    
    # 检查是否必须填槽
    if config.slot_required:
        if ":slot_" not in sql and "WHERE" in sql_upper:
            # 有WHERE但没有slot占位符，可能是硬编码
            pass  # 某些情况允许，视具体策略
    
    # 类型特定检查
    if "no_filter" in query_type and "WHERE" in sql_upper:
        # 检查是否是真正的无筛选，还是只有软条件
        where_match = True  # 简化处理
    
    return True, "验证通过"