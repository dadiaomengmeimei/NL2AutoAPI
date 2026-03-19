#!/usr/bin/env python3
"""
审核层单元测试
测试: submitter, models
"""

import sys
import json
import tempfile
import os
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from review.models import ReviewTask, ReviewTaskType, ReviewStatus
from review.submitter import ReviewSubmitter


def log_section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def log_subsection(title: str):
    print(f"\n  📌 {title}")
    print(f"  {'-'*50}")


def log_result(test_name: str, success: bool, details: str = ""):
    status = "✅ PASS" if success else "❌ FAIL"
    print(f"    {status} | {test_name}")
    if details:
        print(f"           {details}")


def test_review_models():
    """测试审核模型"""
    log_section("TEST: review/models")
    
    log_subsection("ReviewTask创建")
    try:
        task = ReviewTask(
            task_id="task_001",
            task_type=ReviewTaskType.RUNTIME_CORRECTION,
            source_query="查询北京研发部员工",
            candidate_tables=["base_staff", "base_department"],
            wrong_api={"name": "get_staff_by_city", "description": "按城市查询"},
            correct_api={"name": "get_staff_by_dept", "description": "按部门查询"},
            distinction_instruction="区分部门和城市的查询"
        )
        
        print(f"    Task ID: {task.task_id}")
        print(f"    Type: {task.task_type.value}")
        print(f"    Status: {task.status.value}")
        print(f"    Created: {task.created_at}")
        print(f"    Candidate tables: {task.candidate_tables}")
        print(f"    Has distinction: {task.distinction_instruction is not None}")
        
        # 验证状态转换
        assert task.status == ReviewStatus.PENDING
        
        log_result("ReviewTask creation", True)
    except Exception as e:
        log_result("ReviewTask creation", False, str(e))
    
    log_subsection("任务类型枚举")
    try:
        types = list(ReviewTaskType)
        print(f"    任务类型: {[t.value for t in types]}")
        
        # 验证所有类型
        expected = ["runtime_correction", "schema_expansion", "invalid_recovery"]
        actual = [t.value for t in types]
        
        log_result("TaskType enum", set(expected) == set(actual))
    except Exception as e:
        log_result("TaskType enum", False, str(e))
    
    log_subsection("状态流转")
    try:
        task = ReviewTask(
            task_id="task_status_test",
            task_type=ReviewTaskType.SCHEMA_EXPANSION,
            source_query="测试"
        )
        
        print(f"    初始状态: {task.status.value}")
        
        # 模拟状态更新
        task.status = ReviewStatus.APPROVED
        task.reviewer = "admin"
        task.review_comment = "测试通过"
        task.updated_at = datetime.now().isoformat()
        
        print(f"    更新后: {task.status.value}")
        print(f"    审核人: {task.reviewer}")
        print(f"    更新时间: {task.updated_at}")
        
        log_result("Status transition", task.status == ReviewStatus.APPROVED)
    except Exception as e:
        log_result("Status transition", False, str(e))


def test_review_submitter():
    """测试审核提交器"""
    log_section("TEST: review/submitter")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        queue_path = os.path.join(tmpdir, "review_queue.jsonl")
        submitted_path = os.path.join(tmpdir, "submitted_schemas.jsonl")
        
        submitter = ReviewSubmitter(queue_path, submitted_path)
        
        log_subsection("提交运行时纠错任务")
        try:
            task = ReviewTask(
                task_id="rt_001",
                task_type=ReviewTaskType.RUNTIME_CORRECTION,
                source_query="查询深圳员工",
                candidate_tables=["base_staff"],
                wrong_api={"name": "wrong", "description": "错误API"},
                correct_api={"name": "correct", "description": "正确API"},
                distinction_instruction="区分城市和部门"
            )
            
            task_id = submitter.submit(task)
            print(f"    提交任务ID: {task_id}")
            print(f"    提交器计数: {submitter.submitted_count}")
            
            # 验证文件写入
            assert os.path.exists(queue_path)
            with open(queue_path, 'r') as f:
                saved = json.loads(f.readline())
                print(f"    文件内容预览: {saved['task_id']}, {saved['task_type']}")
            
            log_result("submit runtime correction", task_id == "rt_001")
        except Exception as e:
            log_result("submit runtime correction", False, str(e))
        
        log_subsection("提交Schema扩展任务")
        try:
            from schema.models import APISchema
            
            new_api = APISchema(
                name="get_staff_by_age",
                description="按年龄查询员工",
                inputSchema={"type": "object", "properties": {"age": {"type": "integer"}}},
                outputSchema={"type": "array", "items": {"type": "object"}},
                bound_sql="SELECT * FROM staff WHERE age = :slot_age",
                slot_mapping={"age": "age"},
                query_type="exact_query",
                table="staff",
                examples=[]
            )
            
            task_id = submitter.submit_schema_expansion(
                original_query="查询30岁的员工",
                augmented_queries=["30岁员工", "年龄30岁的人"],
                generated_schema=new_api,
                metadata={"strategy": "semantic"}
            )
            
            print(f"    扩展任务ID: {task_id}")
            print(f"    任务类型: schema_expansion")
            
            # 读取验证
            with open(queue_path, 'r') as f:
                lines = f.readlines()
                print(f"    队列总任务数: {len(lines)}")
                last_task = json.loads(lines[-1])
                print(f"    最后任务类型: {last_task['task_type']}")
            
            log_result("submit schema expansion", last_task['task_type'] == "schema_expansion")
        except Exception as e:
            log_result("submit schema expansion", False, str(e))
            import traceback
            traceback.print_exc()
        
        log_subsection("批量提交")
        try:
            tasks = [
                ReviewTask(
                    task_id=f"batch_{i}",
                    task_type=ReviewTaskType.INVALID_RECOVERY,
                    source_query=f"修复任务{i}",
                    proposed_schema={"name": f"api_{i}"}
                )
                for i in range(3)
            ]
            
            # 直接写入模拟批量
            with open(queue_path, 'a') as f:
                for task in tasks:
                    f.write(json.dumps(task.model_dump(), default=str) + '\n')
            
            # 重新加载验证
            loaded = submitter.load_pending_tasks()
            print(f"    待处理任务数: {len(loaded)}")
            for t in loaded[-3:]:
                print(f"      - {t['task_id']}: {t['task_type']}")
            
            log_result("batch submit", len(loaded) >= 3)
        except Exception as e:
            log_result("batch submit", False, str(e))
        
        log_subsection("任务批准流程")
        try:
            # 模拟批准
            approved_schema = APISchema(
                name="approved_api",
                description="已审核通过",
                inputSchema={"type": "object", "properties": {}},
                outputSchema={"type": "array", "items": {"type": "object"}},
                bound_sql="SELECT 1",
                slot_mapping={},
                query_type="aggregate_no_filter",
                table="test",
                examples=[]
            )
            
            submitter.mark_approved("rt_001", approved_schema, reviewer="admin", comment="测试通过")
            
            # 验证写入
            if os.path.exists(submitted_path):
                with open(submitted_path, 'r') as f:
                    approved = json.loads(f.readline())
                    print(f"    批准记录: {approved.get('task_id')}")
                    print(f"    审核人: {approved.get('reviewer')}")
                    print(f"    状态: {approved.get('status')}")
                
                log_result("approve task", approved.get('status') == 'approved')
            else:
                log_result("approve task", False, "approved file not created")
        except Exception as e:
            log_result("approve task", False, str(e))


def run_all_tests():
    print(f"\n{'#'*60}")
    print(f"#{'':^58}#")
    print(f"#{'REVIEW MODULE UNIT TESTS':^58}#")
    print(f"#{'':^58}#")
    print(f"{'#'*60}")
    
    test_review_models()
    test_review_submitter()
    
    print(f"\n{'#'*60}")
    print(f"#{'REVIEW TESTS COMPLETED':^58}#")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    run_all_tests()