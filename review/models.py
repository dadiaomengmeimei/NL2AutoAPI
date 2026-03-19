"""
审核数据模型
"""

from typing import Optional, Any
from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime


class ReviewTaskType(str, Enum):
    """审核任务类型"""
    RUNTIME_CORRECTION = "runtime_correction"    # 运行时纠错
    SCHEMA_EXPANSION = "schema_expansion"          # 事后扩写
    INVALID_RECOVERY = "invalid_recovery"          # 无效修复


class ReviewStatus(str, Enum):
    """审核状态"""
    PENDING = "pending"           # 待审核
    APPROVED = "approved"         # 已批准
    REJECTED = "rejected"         # 已拒绝
    MODIFIED = "modified"         # 修改后批准


class ReviewTask(BaseModel):
    """审核任务模型"""
    
    # 基本信息
    task_id: str = Field(..., description="任务唯一ID")
    task_type: ReviewTaskType = Field(..., description="任务类型")
    status: ReviewStatus = Field(default=ReviewStatus.PENDING)
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    updated_at: Optional[str] = None
    
    # 来源信息
    source_query: str = Field(..., description="原始用户查询")
    source_api_name: Optional[str] = Field(None, description="来源API名称")
    
    # 运行时纠错专用
    candidate_tables: list[str] = Field(default_factory=list, description="候选表列表")
    wrong_api: Optional[dict] = Field(None, description="错误选中的API")
    correct_api: Optional[dict] = Field(None, description="正确的API")
    distinction_instruction: Optional[str] = Field(None, description="区分指令")
    
    # Schema内容（新API或修正后的API）
    proposed_schema: Optional[dict] = Field(None, description="提议的Schema")
    
    # 审核信息
    reviewer: Optional[str] = Field(None, description="审核人")
    review_comment: Optional[str] = Field(None, description="审核意见")
    final_schema: Optional[dict] = Field(None, description="最终确定的Schema")
    
    # 扩展信息
    metadata: dict = Field(default_factory=dict, description="扩展元数据")
    
    class Config:
        extra = "allow"


class BatchReviewResult(BaseModel):
    """批量审核结果"""
    total: int
    approved: int
    rejected: int
    modified: int
    pending: int
    details: list[dict]