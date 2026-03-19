"""
线上验证与纠错模块

处理验证失败的情况，生成纠错信息和审核任务
"""
from typing import Optional

from core.llm import call_llm_json
from core.database import db_manager
from core.utils import fill_sql_with_values
from schema.models import APISchema, RuntimeResult, VerificationResult, ReviewTask
from generation.api_generator import APIGenerator


class OnlineVerifier:
    """线上验证器"""
    
    def __init__(self, registry, api_generator: Optional[APIGenerator] = None, table_top_k: int = 3):
        self.registry = registry
        self.api_generator = api_generator or APIGenerator()
        self.table_top_k = table_top_k
        self.correction_history: list[dict] = []  # 纠错历史
    
    def verify_and_correct(
        self,
        query: str,
        selected_api: APISchema,
        params: dict,
        exec_result: RuntimeResult,
        verification: VerificationResult
    ) -> Optional[ReviewTask]:
        """
        验证失败时，分析原因并生成纠错建议
        
        Args:
            query: 原始查询
            selected_api: 选中的API
            params: 填槽参数
            exec_result: 执行结果
            verification: 验证结果
        
        Returns:
            审核任务或None（如果无需纠错）
        """
        if verification.type == "CORRECT":
            return None
        
        # 分析失败原因
        failure_analysis = self._analyze_failure(
            query, selected_api, params, exec_result, verification
        )
        
        # 根据原因生成不同的审核任务
        if failure_analysis["type"] == "wrong_api":
            # API选择错误，需要区分相似API
            return self._generate_api_distinction_task(
                query, selected_api, failure_analysis
            )
        elif failure_analysis["type"] == "missing_api":
            # 缺少合适的API，需要生成新API
            return self._generate_new_api_task(
                query, failure_analysis
            )
        elif failure_analysis["type"] == "slot_error":
            # 填槽错误
            return self._generate_slot_correction_task(
                query, selected_api, params, failure_analysis
            )
        else:
            # 其他错误，生成通用纠错任务
            return self._generate_generic_correction_task(
                query, selected_api, exec_result, verification
            )
    
    def _analyze_failure(
        self,
        query: str,
        selected_api: APISchema,
        params: dict,
        exec_result: RuntimeResult,
        verification: VerificationResult
    ) -> dict:
        """分析失败原因"""
        prompt = f"""
分析以下验证失败的原因：

用户查询: {query}
选中的API: {selected_api.name}
API描述: {selected_api.description}
API的SQL: {selected_api.bound_sql}
填槽参数: {params}
执行结果: {exec_result.dict() if hasattr(exec_result, 'dict') else exec_result}
验证结果: {verification.type} - {verification.reason}

请分析失败原因类型：
1. wrong_api - 选错了API，应该选其他API
2. missing_api - 没有合适的API，需要新建
3. slot_error - 填槽错误，参数没提对
4. sql_error - SQL执行错误
5. other - 其他原因

输出JSON:
{{
    "type": "原因类型",
    "detail": "详细说明",
    "suggestion": "改进建议"
}}
"""
        result = call_llm_json(prompt)
        return result or {"type": "other", "detail": "未知原因", "suggestion": "人工审核"}
    
    def _generate_api_distinction_task(
        self,
        query: str,
        wrong_api: APISchema,
        analysis: dict
    ) -> ReviewTask:
        """
        生成API区分任务
        
        当召回阶段选错API时，需要生成帮助模型区分的指令
        """
        # 获取候选表（API集合）
        candidate_tables = self.registry.get_candidate_tables(query, top_k=self.table_top_k)
        
        # 获取每个表的API列表
        table_apis = {}
        for table in candidate_tables:
            apis = self.registry.get_by_table(table)
            table_apis[table] = [
                {"name": a.name, "description": a.description}
                for a in apis[:5]  # 每个表最多5个
            ]
        
        # 生成区分指令
        distinction_prompt = f"""
用户查询: {query}
错误选中的API: {wrong_api.name} ({wrong_api.description})

候选表及其API：
{__import__('json').dumps(table_apis, ensure_ascii=False, indent=2)}

请生成清晰的区分指令，帮助模型下次能正确选择API。

输出JSON:
{{
    "correct_table": "正确的表名",
    "correct_api": "正确的API名称",
    "distinction_instruction": "详细的区分说明，说明为什么选这个而不是其他的",
    "api_schema_improvement": "对正确API的description改进建议"
}}
"""
        distinction = call_llm_json(distinction_prompt)
        
        # 获取正确的API（如果已知）
        correct_api = None
        if distinction:
            correct_api_name = distinction.get("correct_api")
            if correct_api_name:
                correct_api = self.registry.get(correct_api_name)
        
        task_id = f"correction_{__import__('time').time():.0f}"
        
        return ReviewTask(
            task_id=task_id,
            task_type="runtime_correction",
            source_query=query,
            source_api_name=wrong_api.name,
            priority=2,  # 高优先级
            query=query,
            current_api=wrong_api,
            candidate_apis=[self.registry.get(n) for n in 
                          [distinction.get("correct_api")] if n] if distinction else [],
            description="API选择纠错：需要生成区分指令",
            auto_verify_result=VerificationResult(
                type="INCORRECT",
                reason=analysis.get("detail", "API选择错误")
            ),
            # 附加信息
            generated_query=distinction.get("distinction_instruction") if distinction else None,
        )
    
    def _generate_new_api_task(
        self,
        query: str,
        analysis: dict
    ) -> ReviewTask:
        """生成新API任务"""
        # 推测目标表
        candidate_tables = self.registry.get_candidate_tables(query, top_k=self.table_top_k)
        target_table = candidate_tables[0] if candidate_tables else "unknown"
        sql_prompt = f"""
根据用户查询，生成正确的SQL：

用户查询: {query}
目标表: {target_table}
该表相关API的SQL示例:
{[a.bound_sql for a in self.registry.get_by_table(target_table)[:3]]}

请生成能正确回答查询的SQL，使用slot参数表示可变部分。

输出JSON:
{{
    "sql": "生成的SQL",
    "table_name": "确认的目标表名",
    "reasoning": "为什么这样生成"
}}
"""
        sql_result = call_llm_json(sql_prompt)
        
        if not sql_result:
            # 无法生成SQL
            return ReviewTask(
                task_id=f"newapi_failed_{__import__('time').time():.0f}",
                task_type="runtime_correction",
                source_query=query,
                source_api_name=None,
                priority=1,
                query=query,
                description=f"新API生成失败: {analysis.get('detail')}",
            )
        
        # 生成API Schema
        suggested_sql = sql_result.get("sql", "")
        table_name = sql_result.get("table_name", target_table)
        
        new_api = self.api_generator.generate_from_runtime(
            query=query,
            sql=suggested_sql,
            table_name=table_name,
            description=analysis.get("suggestion", "")
        )
        
        task_id = f"newapi_{__import__('time').time():.0f}"
        
        return ReviewTask(
            task_id=task_id,
            task_type="runtime_correction",
            source_query=query,
            source_api_name=None,
            priority=3,  # 最高优先级，新功能
            query=query,
            generated_schema=new_api,
            generated_sql=suggested_sql,
            description=f"新建API需求: {analysis.get('detail')}",
        )
    
    def _generate_slot_correction_task(
        self,
        query: str,
        api: APISchema,
        params: dict,
        analysis: dict
    ) -> ReviewTask:
        """生成填槽纠错任务"""
        task_id = f"slot_{__import__('time').time():.0f}"
        
        return ReviewTask(
            task_id=task_id,
            task_type="runtime_correction",
            source_query=query,
            source_api_name=api.name if api else None,
            priority=2,
            query=query,
            current_api=api,
            description=f"填槽纠错: {analysis.get('detail')}",
            auto_verify_result=VerificationResult(
                type="PARTIAL",
                reason=f"填槽错误: {params}"
            ),
        )
    
    def _generate_generic_correction_task(
        self,
        query: str,
        api: APISchema,
        exec_result: RuntimeResult,
        verification: VerificationResult
    ) -> ReviewTask:
        """生成通用纠错任务"""
        task_id = f"generic_{__import__('time').time():.0f}"
        
        return ReviewTask(
            task_id=task_id,
            task_type="runtime_correction",
            source_query=query,
            source_api_name=api.name if api else None,
            priority=1,
            query=query,
            current_api=api,
            execution_result=exec_result,
            description=f"通用纠错: {verification.reason}",
            auto_verify_result=verification,
        )