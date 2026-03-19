#!/usr/bin/env python3
"""
API注册中心

加载生成的APISchema，建立索引支持高效召回
"""

from typing import Optional, List, Dict, Tuple  # 修复：使用大写的List, Dict, Tuple
import json
from pathlib import Path

from schema.models import APISchema
from core.utils import load_jsonl
from core.logger import get_logger

logger = get_logger()


class APIRegistry:
    """
    API注册表
    
    功能：
    1. 加载所有生成的API Schema
    2. 建立表名索引
    3. 支持按表名快速检索候选API
    4. 维护全局API池供召回使用
    """
    
    def __init__(self, data_path: Optional[str] = None):
        """
        初始化注册表
        
        Args:
            data_path: API Schema数据文件路径（JSONL格式）
        """
        self.apis: List[APISchema] = []  # 修复：使用List
        self.table_index: Dict[str, List[APISchema]] = {}  # 修复：使用Dict, List
        self.name_index: Dict[str, APISchema] = {}  # 修复：使用Dict
        
        if data_path:
            self.load_from_file(data_path)
    
    def load_from_file(self, path: str):
        """
        从JSONL文件加载API Schema
        
        Args:
            path: 文件路径
        """
        path = Path(path)
        if not path.exists():
            logger.warning("文件不存在 %s", path)
            return
        
        records = load_jsonl(str(path))
        for record in records:
            try:
                raw_api = record.get("api_schema") if isinstance(record, dict) and "api_schema" in record else record
                if not isinstance(raw_api, dict):
                    continue

                # 补全兼容字段
                raw_api.setdefault("name", raw_api.get("name") or raw_api.get("query", ""))
                raw_api.setdefault("description", raw_api.get("description", ""))
                raw_api.setdefault("bound_sql", raw_api.get("bound_sql", ""))
                raw_api.setdefault("table", raw_api.get("table", record.get("table", "")))

                # 兼容旧 slot_mapping 形式
                if isinstance(raw_api.get("slot_mapping"), list):
                    raw_api["slot_mapping"] = {s: s for s in raw_api.get("slot_mapping", [])}

                api = APISchema(**raw_api)
                self.register(api)
            except Exception as e:
                logger.error("加载API失败: %s", e)
                continue
        
        logger.info("已加载 %d 个API", len(self.apis))
        logger.info("覆盖 %d 个表", len(self.table_index))
    
    def register(self, api: APISchema):
        """
        注册单个API
        
        Args:
            api: API Schema对象
        """
        self.apis.append(api)
        self.name_index[api.name] = api
        
        # 建立表索引
        table = api.table
        if table not in self.table_index:
            self.table_index[table] = []
        self.table_index[table].append(api)
    
    def get_api_by_name(self, name: str) -> Optional[APISchema]:
        """按名称获取API"""
        return self.name_index.get(name)
    
    def get_apis_by_table(self, table: str) -> List[APISchema]:  # 修复：使用List
        """按表名获取所有相关API"""
        return self.table_index.get(table, [])
    
    # 兼容旧接口。
    def get_by_table(self, table: str) -> List[APISchema]:
        """按表名获取所有相关API（兼容旧调用 get_by_table）"""
        return self.get_apis_by_table(table)

    def get(self, name: str) -> Optional[APISchema]:
        """按API名称获取API（兼容旧调用 get）"""
        return self.get_api_by_name(name)
    
    def get_candidate_tables(self, query: str, top_k: int = 5) -> List[str]:  # 修复：使用List
        """
        根据Query关键词提取候选表
        
        简单实现：基于关键词匹配，实际可用NER/分类模型
        
        Args:
            query: 用户查询
        
        Returns:
            候选表名列表（按匹配度排序）
        """
        query_lower = query.lower()
        
        # 表名匹配分数
        scores: Dict[str, float] = {}  # 修复：使用Dict
        
        for table in self.table_index.keys():
            score = 0.0
            
            # 直接包含表名或别名
            table_parts = table.replace('_', ' ').split()
            for part in table_parts:
                if part in query_lower:
                    score += 1.0
            
            # 检查API描述中的关键词
            for api in self.table_index[table]:
                desc = api.description.lower()
                # 简单Jaccard相似度
                query_words = set(query_lower.split())
                desc_words = set(desc.split())
                if query_words & desc_words:
                    score += 0.5 * len(query_words & desc_words) / len(query_words | desc_words)
            
            if score > 0:
                scores[table] = score
        
        # 按分数排序
        sorted_tables = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        if not sorted_tables:
            # 1) 如果query在表名/描述中命中0个，退化为“全量表+前top_k”，保证不会返回空
            # 2) 这个位置是用户感受最直接的候选表列表
            logger.warning("候选表匹配为空，返回默认表列表 top_k=%s", top_k)
            return list(self.table_index.keys())[:top_k]

        return [t[0] for t in sorted_tables[:top_k]]  # Top-5
    
    def search_apis(self, keyword: str) -> List[APISchema]:  # 修复：使用List
        """
        关键词搜索API
        
        Args:
            keyword: 搜索关键词
        
        Returns:
            匹配的API列表
        """
        keyword_lower = keyword.lower()
        results = []
        
        for api in self.apis:
            # 搜索名称和描述
            if (keyword_lower in api.name.lower() or 
                keyword_lower in api.description.lower()):
                results.append(api)
        
        return results
    
    def get_all_tables(self) -> List[str]:  # 修复：使用List
        """获取所有表名"""
        return list(self.table_index.keys())

    def get_shards(self, shard_size: int = 10) -> List[List[APISchema]]:
        """按shard_size将全部API分片，便于较大数据量召回"""
        all_apis = self.apis
        shards = [all_apis[i:i+shard_size] for i in range(0, len(all_apis), shard_size)]
        return shards
    
    def get_stats(self) -> Dict[str, any]:  # 修复：使用Dict
        """获取注册表统计信息"""
        return {
            "total_apis": len(self.apis),
            "total_tables": len(self.table_index),
            "apis_per_table": {
                table: len(apis) 
                for table, apis in self.table_index.items()
            }
        }