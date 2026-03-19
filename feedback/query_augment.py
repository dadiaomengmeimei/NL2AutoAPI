#!/usr/bin/env python3
"""
查询扩写模块：将审核后的Query变体扩展为新数据

核心功能：
1. 从人工修正的Query中提取变体模式
2. 生成语义等价的Query表达方式
3. 触发schema_expander完成数据扩展
"""

from typing import List, Optional
from enum import Enum

from core.utils import call_llm, parse_llm_json


class AugmentStrategy(Enum):
    """扩写策略枚举"""
    SEMANTIC = "semantic"           # 语义等价变体
    STRUCTURAL = "structural"       # 结构调整
    CONTEXTUAL = "contextual"       # 上下文扩展


class QueryAugmenter:
    """
    Query扩写器
    
    输入: 原始Query + 人工修正后的Query
    输出: 语义等价的Query变体列表
    """
    
    STRATEGY_PROMPTS = {
        AugmentStrategy.SEMANTIC: """
基于以下原始Query和人工修正版本，生成{num_variants}个语义等价的表达方式。

原始Query: {original}
修正版本: {corrected}
相关表: {table_hint}

要求:
1. 保持核心语义完全一致
2. 使用不同的词汇和句式
3. 覆盖不同的用户表达习惯
4. 每个变体用不同风格（口语/书面/简略/详细）

以JSON格式输出: {{"variants": ["变体1", "变体2", ...]}}
""",
        AugmentStrategy.STRUCTURAL: """
重构以下Query的表达方式，生成{num_variants}个结构不同的等价Query。

原始: {original}
表: {table_hint}

重构维度:
1. 主动句 ↔ 被动句
2. 肯定句 ↔ 双重否定
3. 直接问句 ↔ 条件假设
4. 完整句 ↔ 省略句

输出: {{"variants": [...]}}
""",
        AugmentStrategy.CONTEXTUAL: """
为Query添加上下文信息，生成{num_variants}个带场景的变体。

原始Query: {original}
表: {table_hint}

场景示例:
- 报表场景: "导出...报表"、"生成...统计"
- 查询场景: "查看...信息"、"搜索...数据"
- 分析场景: "对比...差异"、"分析...趋势"

输出: {{"variants": [...]}}
"""
    }
    
    def __init__(self):
        self.strategy = AugmentStrategy.SEMANTIC
    
    def augment(
        self,
        original_query: str,
        table_hint: str,
        num_variants: int = 3,
        strategy: AugmentStrategy = AugmentStrategy.SEMANTIC,
        corrected_query: Optional[str] = None
    ) -> List[str]:
        """
        扩写Query生成变体
        
        Args:
            original_query: 原始Query
            table_hint: 相关表名提示
            num_variants: 生成变体数量
            strategy: 扩写策略
            corrected_query: 人工修正版本（可选）
        
        Returns:
            Query变体列表
        """
        prompt_template = self.STRATEGY_PROMPTS[strategy]
        prompt = prompt_template.format(
            original=original_query,
            corrected=corrected_query or original_query,
            table_hint=table_hint,
            num_variants=num_variants
        )
        
        try:
            response = call_llm(prompt, temperature=0.8)
            result = parse_llm_json(response)
            
            variants = result.get("variants", [])
            # 去重并限制数量
            unique_variants = list(dict.fromkeys(variants))[:num_variants]
            return unique_variants
            
        except Exception as e:
            print(f"[QueryAugment] 扩写失败: {e}")
            # 保底策略：简单变体
            return self._fallback_variants(original_query, num_variants)
    
    def _fallback_variants(self, query: str, n: int) -> List[str]:
        """保底变体生成"""
        # 简单的句式变换
        templates = [
            "请查询{query}",
            "帮我找一下{query}",
            "看看{query}",
            "获取{query}",
            "统计{query}",
            "{query}是多少",
        ]
        
        # 去除常见动词后填充
        core = query.replace("查询", "").replace("统计", "").replace("获取", "").strip()
        
        variants = []
        for i, tmpl in enumerate(templates):
            if len(variants) >= n:
                break
            if "{query}" in tmpl:
                variants.append(tmpl.format(query=core))
            else:
                variants.append(tmpl + core)
        
        return variants[:n]
    
    def batch_augment(
        self,
        queries: List[dict],
        num_variants: int = 3
    ) -> List[dict]:
        """
        批量扩写
        
        Args:
            queries: [{"query": "...", "table": "...", "corrected": "..."}, ...]
        
        Returns:
            带变体的结果列表
        """
        results = []
        
        for item in queries:
            variants = self.augment(
                original_query=item["query"],
                table_hint=item.get("table", ""),
                num_variants=num_variants,
                corrected_query=item.get("corrected")
            )
            
            results.append({
                "original": item["query"],
                "table": item.get("table"),
                "variants": variants,
                "total_variants": len(variants)
            })
        
        return results