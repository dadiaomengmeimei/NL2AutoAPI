import json
from datetime import datetime
from typing import Optional

from schema.models import RuntimeResult, APISchema


class CaseCollector:
    """案例收集器"""
    
    def __init__(self, output_path: str = "./feedback_cases.jsonl"):
        self.output_path = output_path
        self.cases: list[dict] = []
    
    def collect(
        self,
        query: str,
        api: Optional[APISchema],
        params: dict,
        result: RuntimeResult,
        latency_ms: float = 0,
        user_feedback: Optional[str] = None,
        context: Optional[dict] = None
    ):
        """
        收集一个运行案例
        
        Args:
            query: 用户查询
            api: 使用的API
            params: 填槽参数
            result: 执行结果
            latency_ms: 响应延迟
            user_feedback: 用户反馈（可选）
            context: 额外上下文
        """
        case = {
            "timestamp": datetime.now().isoformat(),
            "query": query,
            "api_name": api.name if api else None,
            "api_description": api.description if api else None,
            "params": params,
            "status": result.status,
            "row_count": result.row_count,
            "latency_ms": latency_ms,
            "user_feedback": user_feedback,
            "context": context or {},
        }
        
        # 记录错误信息
        if result.error:
            case["error"] = result.error
        
        # 记录验证结果
        if result.verification:
            case["verification"] = {
                "type": result.verification.type,
                "reason": result.verification.reason,
            }
        
        self.cases.append(case)
        
        # 实时写入
        with open(self.output_path, "a", encoding="utf8") as f:
            f.write(json.dumps(case, ensure_ascii=False) + "\n")
    
    def get_boundary_cases(self, min_confidence: float = 0.5) -> list[dict]:
        """
        获取边界案例（需要扩写的）
        
        包括：
        - 验证为PARTIAL的
        - 验证为INCORRECT的
        - 用户反馈负面的
        - 执行失败的
        
        Returns:
            边界案例列表
        """
        boundary = []
        for case in self.cases:
            # 执行失败
            if case.get("status") != "success":
                boundary.append(case)
                continue
            
            # 验证置信度低
            verification = case.get("verification", {})
            if verification.get("type") in ("PARTIAL", "INCORRECT"):
                boundary.append(case)
                continue
            
            # 用户反馈负面
            if case.get("user_feedback") in ("bad", "incorrect", "unhelpful"):
                boundary.append(case)
                continue
        
        return boundary