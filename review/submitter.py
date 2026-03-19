"""
审核任务提交器
"""
import json
import os
from datetime import datetime
from typing import Optional

from schema.models import ReviewTask
from core.config import pipeline_config


class ReviewSubmitter:
    """审核任务提交器"""
    
    def __init__(self, review_queue_path: Optional[str] = None, submitted_path: str = "./submitted_schemas.jsonl"):
        if not review_queue_path:
            base_dir = os.path.dirname(pipeline_config.valid_path) or "./output"
            review_queue_path = os.path.join(base_dir, "review_queue.jsonl")
        self.review_queue_path = review_queue_path
        self.submitted_path = submitted_path
        self.submitted_count = 0
    
    def submit(self, task: ReviewTask, reviewer: Optional[str] = None) -> str:
        """
        提交审核任务
        
        Args:
            task: 审核任务
            reviewer: 指定审核人，None表示系统分配
        
        Returns:
            任务ID
        """
        # 确保目录存在
        os.makedirs(os.path.dirname(self.review_queue_path) or ".", exist_ok=True)
        
        # 添加时间戳和审核人
        task_data = task.dict()
        task_data["submit_time"] = datetime.now().isoformat()
        task_data["assigned_reviewer"] = reviewer
        
        # 追加到队列
        with open(self.review_queue_path, "a", encoding="utf8") as f:
            f.write(json.dumps(task_data, ensure_ascii=False) + "\n")
        
        self.submitted_count += 1
        print(f"  [Review] 提交审核任务: {task.task_id} (类型: {task.task_type})")
        
        return task.task_id
    
    def submit_runtime_correction(
        self,
        query: str,
        correct_api: Optional[dict],
        wrong_api: Optional[dict] = None,
        distinction_instruction: Optional[str] = None,
        candidate_tables: Optional[list] = None,
        invoked_sql: Optional[str] = None,
        params: Optional[dict] = None,
    ) -> str:
        """
        提交运行时纠错任务（简化接口）
        
        Args:
            query: 原始查询
            correct_api: 正确的API信息
            wrong_api: 错误选中的API
            distinction_instruction: 区分指令
            candidate_tables: 候选表列表
            invoked_sql: 填槽后实际执行的SQL（审核时展示用）
            params: 填槽参数
        
        Returns:
            任务ID
        """
        task_id = f"rt_{datetime.now().timestamp():.0f}"
        
        task_data = {
            "task_id": task_id,
            "task_type": "runtime_correction",
            "priority": 2,
            "query": query,
            "wrong_api": wrong_api,
            "correct_api": correct_api,
            "candidate_tables": candidate_tables or [],
            "distinction_instruction": distinction_instruction,
            "invoked_sql": invoked_sql or "",
            "params": params or {},
            "submit_time": datetime.now().isoformat(),
            "status": "pending"
        }
        
        with open(self.review_queue_path, "a", encoding="utf8") as f:
            f.write(json.dumps(task_data, ensure_ascii=False) + "\n")
        
        print(f"  [Review] 提交运行时纠错任务: {task_id}")
        return task_id
    
    def submit_schema_expansion(
        self,
        original_query: str,
        expanded_queries: list[str],
        base_api: dict,
        generated_schemas: list[dict]
    ) -> str:
        """
        提交Schema扩展任务（事后）
        
        Args:
            original_query: 原始查询
            expanded_queries: 扩写的查询列表
            base_api: 基础API
            generated_schemas: 生成的Schema列表
        
        Returns:
            任务ID
        """
        task_id = f"exp_{datetime.now().timestamp():.0f}"
        
        task_data = {
            "task_id": task_id,
            "task_type": "schema_expansion",
            "priority": 1,
            "original_query": original_query,
            "expanded_queries": expanded_queries,
            "base_api": base_api,
            "generated_schemas": generated_schemas,
            "submit_time": datetime.now().isoformat(),
            "status": "pending"
        }
        
        with open(self.review_queue_path, "a", encoding="utf8") as f:
            f.write(json.dumps(task_data, ensure_ascii=False) + "\n")
        
        print(f"  [Review] 提交Schema扩展任务: {task_id} ({len(generated_schemas)}个候选)")
        return task_id