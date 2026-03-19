"""
表能力约束管理系统
1. 表描述生成（从字段合成）
2. 表级别能力范围说明
3. API级别不支持能力指令
"""
import os
import json
from typing import Optional, Dict, List, Any
from datetime import datetime
from core.llm import call_llm_json
from core.utils import save_jsonl, load_jsonl
from core.logger import get_logger

logger = get_logger()


class TableDescriptionGenerator:
    """表描述生成器 - 从字段集合合成自然语言描述"""
    
    def generate_from_fields(self, table_name: str, fields: List[Dict[str, str]]) -> str:
        """
        从表字段集合生成表的描述
        
        Args:
            table_name: 表名
            fields: 字段列表，每个字段包含 name/type/comment
        
        Returns:
            自然语言表描述
        """
        fields_str = "\n".join([
            f"- {f.get('name')} ({f.get('type', 'VARCHAR')}): {f.get('comment', '')}"
            for f in fields
        ])
        
        prompt = f"""
根据以下数据库表的字段信息，生成一个简洁、信息丰富的表业务描述。

表名: {table_name}
字段:
{fields_str}

要求:
1. 用中文描述，2-3句话
2. 清晰说明表的主要用途和包含的关键数据
3. 突出表支持的主要查询维度
4. 返回JSON格式: {{"description": "..."}}
"""
        
        result = call_llm_json(prompt)
        if isinstance(result, dict) and "description" in result:
            desc = result["description"].strip()
            logger.info(f"生成表描述 [{table_name}]: {desc}")
            return desc
        
        # 降级：用字段列表作为描述
        return f"{table_name}表，包含字段：{', '.join([f['name'] for f in fields[:5]])}等"


class CapabilityInstructManager:
    """能力约束指令管理器"""
    
    def __init__(self, output_dir: str = "./output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # 两级约束的路径
        self.all_table_instruct_path = os.path.join(output_dir, "all_table_instruct.json")
        self.table_instruct_cache: Dict[str, str] = {}  # table_name -> instruct
    
    def generate_instruct(
        self,
        query: str,
        table_name: str,
        table_desc: str,
        available_fields: List[str],
        existing_api_names: List[str] = None
    ) -> str:
        """
        根据query和表能力生成"不支持指令"
        
        Args:
            query: 用户query
            table_name: 表名
            table_desc: 表描述
            available_fields: 表支持的字段列表
            existing_api_names: 已有的API名列表（用于参考现有能力边界）
        
        Returns:
            不支持能力的指令说明
        """
        apis_ref = "\n".join(existing_api_names[-5:]) if existing_api_names else "无"
        
        prompt = f"""
分析用户query与表能力的挤兑。给出该query为什么不能被表支持的原因。

用户Query: {query}
表名: {table_name}
表描述: {table_desc}
表支持的字段: {', '.join(available_fields[:10])}
现有API示例: {apis_ref}

分析要求:
1. 判断query的需求类型（e.g., 超出字段范围、需要跨表关联、时间粒度不支持、复杂计算等）
2. 说明表的限制原因
3. 给出温和的前端提示文案
4. 返回JSON: {{
    "reason_type": "字段不支持|跨表关联需求|复杂聚合|其他",
    "table_limitation": "表的具体限制说明",
    "user_friendly_message": "推荐告诉用户的话术"
}}
"""
        
        result = call_llm_json(prompt)
        if isinstance(result, dict):
            instruct = {
                "query": query,
                "table": table_name,
                "reason_type": result.get("reason_type", "其他"),
                "table_limitation": result.get("table_limitation", ""),
                "user_friendly_message": result.get("user_friendly_message", "当前未找到合适的数据源"),
                "created_at": datetime.now().isoformat()
            }
            logger.info(f"生成instruct [{table_name}]: {instruct['reason_type']}")
            return instruct
        
        return {
            "query": query,
            "table": table_name,
            "reason_type": "系统错误",
            "table_limitation": "LLM生成失败",
            "user_friendly_message": "当前未找到合适的数据源",
            "created_at": datetime.now().isoformat()
        }
    
    def save_all_table_instruct(self, instruct_dict: Dict[str, Any]):
        """
        保存所有表的能力范畴说明（all_table_instruct.json）
        
        Args:
            instruct_dict: 格式为 {
                "description": "系统说明",
                "tables": {
                    "base_staff": {
                        "capabilities": ["exact_query", "group_by", ...],
                        "limitations": ["跨表join", "复杂窗口函数", ...],
                        "supported_fields": [...],
                        "table_desc": "..."
                    },
                    ...
                }
            }
        """
        with open(self.all_table_instruct_path, 'w', encoding='utf-8') as f:
            json.dump(instruct_dict, f, ensure_ascii=False, indent=2)
        logger.info(f"保存all_table_instruct到: {self.all_table_instruct_path}")
    
    def load_all_table_instruct(self) -> Dict[str, Any]:
        """加载所有表的能力范畴说明"""
        if os.path.exists(self.all_table_instruct_path):
            with open(self.all_table_instruct_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"description": "未生成", "tables": {}}
    
    def get_table_instruct(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取指定表的能力说明"""
        all_instruct = self.load_all_table_instruct()
        return all_instruct.get("tables", {}).get(table_name)


class SynthesizedTableDesc:
    """合成表描述工具 - 整合字段→desc→能力→约束"""
    
    def __init__(self, output_dir: str = "./output"):
        self.desc_generator = TableDescriptionGenerator()
        self.instruct_manager = CapabilityInstructManager(output_dir)
    
    def synthesize(
        self,
        table_name: str,
        fields: List[Dict[str, str]],
        existing_apis: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        综合生成表的完整描述和能力约束
        
        Args:
            table_name: 表名
            fields: 字段列表
            existing_apis: 已有的API list（用于推断表能力）
        
        Returns:
            包含desc/capabilities/limitations的字典
        """
        # 1. 生成表描述
        table_desc = self.desc_generator.generate_from_fields(table_name, fields)
        
        # 2. 从existing_apis推断能力
        capabilities = set()
        supported_fields = set()
        
        if existing_apis:
            for api in existing_apis:
                # 从API名推断查询类型
                if "exact_query" in api.get("name", ""):
                    capabilities.add("精准查询")
                elif "group_" in api.get("name", ""):
                    capabilities.add("分组聚合")
                elif "count" in api.get("name", ""):
                    capabilities.add("计数统计")
                
                # 从SQL提取字段
                sql = api.get("bound_sql", "")
                for field in fields:
                    if field["name"] in sql:
                        supported_fields.add(field["name"])
        
        return {
            "table": table_name,
            "description": table_desc,
            "supported_fields": list(supported_fields),
            "capabilities": list(capabilities),
            "limitations": [
                "仅支持单表查询（不支持跨表Join）",
                "不支持自定义复杂窗口函数",
                "不支持机器学习或高级分析函数"
            ],
            "generated_at": datetime.now().isoformat()
        }


# 温和话术库
FRIENDLY_MESSAGES = {
    "field_not_supported": "抱歉，当前表格中不包含您查询的'{field}'字段。建议您选择其他查询维度。",
    "cross_table_join": "该查询涉及多个表的关联，目前暂不支持。建议分别查询相关信息。",
    "complex_aggregation": "您的查询包含复杂计算，目前我们支持的是基础的统计分析。建议简化查询条件。",
    "no_matching_source": "当前未找到合适的数据源。您可以尝试：1. 调整查询条件；2. 选择其他查询维度；3. 联系数据团队。",
    "time_granularity": "目前数据仅支持按{granularity}查询，不支持更细粒度的时间维度。",
}
