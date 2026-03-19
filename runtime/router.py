"""
运行时路由：完整的query→api→sql执行链路
"""

from typing import Optional

from core.database import db_manager, execute_sql
from core.utils import fill_sql_with_values
from core.logger import get_logger
from schema.models import RuntimeResult, APISchema, VerificationResult

logger = get_logger()
from runtime.registry import APIRegistry
from runtime.recall import APIRecaller
from runtime.slot_filling import SlotFiller
from runtime.online_verify import OnlineVerifier
from validation.llm_judge import LLMJudge
from review.submitter import ReviewSubmitter


class RuntimeRouter:
    """
    运行时路由器
    
    完整链路: query → recall → select → fill slots → execute → verify
    """
    
    def __init__(
        self,
        registry: APIRegistry,
        submitter: Optional[ReviewSubmitter] = None,
        enable_verify: bool = True,
        table_top_k: int = 3,
        api_top_k: int = 5,
    ):
        self.registry = registry
        self.table_top_k = table_top_k
        self.api_top_k = api_top_k
        self.recaller = APIRecaller(registry, top_k=api_top_k)
        self.slot_filler = SlotFiller()
        self.llm_judge = LLMJudge()
        self.online_verifier = OnlineVerifier(registry, table_top_k=table_top_k)
        self.submitter = submitter or ReviewSubmitter()
        self.enable_verify = enable_verify
    
    def route(self, query: str, top_k: int = 5) -> RuntimeResult:
        """
        路由执行完整链路
        
        Args:
            query: 用户查询
            top_k: 召回候选数量
        
        Returns:
            运行时结果
        """
        logger.info("query: %s", query[:60])
        
        # 1. 候选表召回（用于后续纠错）
        candidate_tables = self.registry.get_candidate_tables(query, top_k=self.table_top_k)
        logger.info("候选表: %s", candidate_tables)
        
        # 1.5 检查表召回是否为空
        if not candidate_tables:
            user_message = (
                "抱歉，当前未找到合适的数据源。您的查询可能涉及以下情况：\n"
                "1. 查询字段超出当前数据库范围\n"
                "2. 所需的数据维度暂不支持\n"
                "建议：请尝试调整查询条件或联系数据团队补充数据源。"
            )
            logger.warning("No candidate tables found for query")
            return RuntimeResult(
                status="error",
                error=user_message,
                correction_needed=True,
            )
        
        # 2. API召回
        candidates = self.recaller.recall(query, table_hint=candidate_tables[0] if candidate_tables else None)
        if not candidates:
            user_message = (
                "抱歉，当前未找到合适的数据源。\n"
                f"数据库 {candidate_tables[0] if candidate_tables else '未知表'} 中可能不包含您需要的字段。\n"
                "建议：\n"
                "1. 尝试用其他字段名描述您的查询\n"
                "2. 简化查询条件（例如：只查询某一个维度）\n"
                "3. 联系数据团队了解数据源范围"
            )
            logger.warning("No API candidates found")
            return RuntimeResult(
                status="error",
                error=user_message,
                correction_needed=True
            )
        
        logger.info("召回 %d 个候选API", len(candidates))
        
        # 3. 精选最佳API
        best_api = self.recaller.select_best(query, candidates)
        if not best_api:
            return RuntimeResult(
                status="error",
                error="精选 API 失败",
                correction_needed=True
            )
        
        logger.info("选中 API: %s", best_api.name)
        logger.info("description: %s", best_api.description[:60])
        
        # 3.5 检查能力约束指令（instruct）
        if best_api.instruct:
            # 该API被标记为不支持此类查询
            instruct = best_api.instruct
            user_message = instruct.get("user_friendly_message", "当前未找到合适的数据源")
            logger.warning("API marked with limitation: %s", instruct.get("reason_type"))
            
            return RuntimeResult(
                status="error",
                error=user_message,
                correction_needed=True,
                api_name=best_api.name,
            )
        
        # 4. 填槽
        params = self.slot_filler.fill(query, best_api)
        logger.info("提取参数: %s", params)
        
        # 5. 参数验证
        valid, missing = self.slot_filler.validate(params, best_api)
        if not valid:
            error_msg = f"缺少必填参数: {missing}"
            logger.warning(error_msg)
            
            # 提交纠错任务
            task = self.online_verifier._generate_slot_correction_task(
                query, best_api, params,
                {"type": "slot_error", "detail": error_msg, "missing": missing}
            )
            self.submitter.submit(task)
            
            return RuntimeResult(
                status="error",
                error=error_msg,
                api_name=best_api.name,
                params=params,
                correction_needed=True
            )
        
        # 6. 执行SQL
        exec_sql = fill_sql_with_values(best_api.bound_sql, params)
        logger.info("执行SQL: %s", exec_sql[:80])
        
        exec_result = execute_sql(None, exec_sql)  # 使用全局db_manager
        
        if exec_result["status"] != "success":
            return RuntimeResult(
                status="error",
                error=f"SQL执行失败: {exec_result.get('error')}",
                api_name=best_api.name,
                params=params,
                invoked_sql=exec_sql,
                correction_needed=True
            )
        
        # 7. 结果验证
        result = RuntimeResult(
            status="success",
            api_name=best_api.name,
            params=params,
            invoked_sql=exec_sql,
            data=exec_result.get("data"),
            columns=exec_result.get("columns"),
            row_count=exec_result.get("row_count", 0),
            exec_result=exec_result,
        )
        
        if self.enable_verify:
            verification = self.llm_judge.judge(
                query, exec_sql,
                {"data": result.data, "columns": result.columns, "row_count": result.row_count}
            )
            result.verification = verification
            
            logger.info("验证结果: %s (%s)", verification.type, verification.reason[:60])
            
            # 验证不通过，触发纠错
            if not self.llm_judge.is_acceptable(verification):
                result.correction_needed = True
                result.status = "partial" if verification.type == "PARTIAL" else "error"
                
                # 生成纠错任务
                task = self.online_verifier.verify_and_correct(
                    query, best_api, params, result, verification
                )
                if task:
                    self.submitter.submit(task)
                
                return result
        
        logger.info("✓ 成功 (rows=%d)", result.row_count)
        return result