#!/usr/bin/env python3
"""
Schema模型定义

定义数据表结构、API Schema、生成记录等核心数据模型
"""

from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from enum import Enum


class FieldType(str, Enum):
    """字段类型枚举"""
    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"


class FieldInfo(BaseModel):
    """表字段信息"""
    name: str = Field(..., description="字段名")
    type: str = Field(..., description="数据类型，如INT, VARCHAR等")
    length: Optional[int] = Field(None, description="长度限制")
    precision: Optional[int] = Field(None, description="数值精度")
    scale: Optional[int] = Field(None, description="数值小数位")
    is_primary: bool = Field(default=False, description="是否主键")
    is_nullable: bool = Field(default=True, description="是否可空")
    is_index: bool = Field(default=False, description="是否有索引")
    comment: Optional[str] = Field(None, description="字段注释")
    default: Optional[Any] = Field(None, description="默认值")
    
    # 扩展信息
    distinct_count: Optional[int] = Field(None, description="不同值数量（采样）")
    sample_values: Optional[List[str]] = Field(None, description="采样值示例")


class TableSchema(BaseModel):
    """表结构定义"""
    name: str = Field(..., description="表名")
    comment: Optional[str] = Field(None, description="表注释")
    fields: List[FieldInfo] = Field(default_factory=list, description="字段列表")
    primary_key: Optional[List[str]] = Field(None, description="主键字段组合")
    indexes: Optional[List[Dict[str, Any]]] = Field(None, description="索引定义")
    
    def get_field(self, name: str) -> Optional[FieldInfo]:
        """按名称获取字段"""
        for f in self.fields:
            if f.name == name:
                return f
        return None
    
    def get_primary_key(self) -> Optional[FieldInfo]:
        """获取主键字段（单字段主键）"""
        for f in self.fields:
            if f.is_primary:
                return f
        return None
    
    def get_indexed_fields(self) -> List[FieldInfo]:
        """获取有索引的字段"""
        return [f for f in self.fields if f.is_index]


class DatabaseSchema(BaseModel):
    """数据库Schema"""
    database: str = Field(..., description="数据库名")
    tables: Dict[str, TableSchema] = Field(default_factory=dict, description="表映射: 表名->TableSchema")
    
    def get_table(self, name: str) -> Optional[TableSchema]:
        """按名称获取表"""
        return self.tables.get(name)

    def get_table_names(self) -> list[str]:
        """获取表名列表"""
        return list(self.tables.keys())


# ==================== API Schema ====================

class JSONSchema(BaseModel):
    """JSON Schema定义（简化版）"""
    type: str = "object"
    properties: Optional[Dict[str, Any]] = None
    required: Optional[List[str]] = None
    items: Optional[Dict[str, Any]] = None  # 用于array类型
    description: Optional[str] = None


class APISchema(BaseModel):
    """
    API Schema定义
    
    包含完整的API契约信息，可直接用于：
    1. 生成OpenAPI文档
    2. 运行时参数校验
    3. SQL绑定执行
    """
    
    # 基本信息
    name: str = Field(..., description="API名称，如 get_user_by_id")
    description: str = Field(..., description="API功能描述")
    
    # JSON Schema定义
    inputSchema: Dict[str, Any] = Field(
        default_factory=dict,
        description="输入参数Schema"
    )
    outputSchema: Dict[str, Any] = Field(
        default_factory=dict,
        description="输出结果Schema"
    )
    
    # SQL绑定
    bound_sql: str = Field(..., description="绑定的SQL模板，含:slot_占位符")
    slot_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description="slot名到SQL参数的映射，如 {'city': 'city_name'}"
    )
    
    # 元信息
    query_type: str = Field(..., description="查询类型，如 exact_query")
    table: str = Field(..., description="主操作表")
    related_tables: Optional[List[str]] = Field(None, description="关联表")
    
    # 示例（用于 few-shot 和测试）
    examples: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="使用示例，每个包含query和params"
    )
    
    # 扩展信息
    author: Optional[str] = Field(None, description="生成者/审核者")
    confidence: Optional[float] = Field(None, description="生成置信度")
    created_at: Optional[str] = Field(None, description="创建时间")
    version: str = Field(default="1.0", description="版本号")
    
    # 能力约束指令
    instruct: Optional[Dict[str, Any]] = Field(
        None,
        description="表该API不支持的能力说明，包括reason_type/table_limitation/user_friendly_message"
    )
    
    class Config:
        extra = "allow"  # 允许扩展字段


# ==================== 生成记录 ====================

class GenerationRecord(BaseModel):
    """单次生成记录，用于追踪和调试"""
    
    # 输入
    table: str
    query_type: str
    selected_fields: Optional[List[str]] = None
    
    # 中间产物
    generated_sql: Optional[str] = None
    sql_valid: Optional[bool] = None
    sql_issues: Optional[List[str]] = None
    
    generated_queries: Optional[List[str]] = None
    
    # 最终结果
    final_api: Optional[APISchema] = None
    
    # 验证结果
    validation_passed: Optional[bool] = None
    validation_type: Optional[str] = None  # correct / partial / incorrect
    validation_reason: Optional[str] = None
    
    # 迭代信息
    iteration: int = 0
    retry_count: int = 0
    
    # 时间戳
    created_at: str = Field(default_factory=lambda: __import__('datetime').datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于日志/存储）"""
        return self.model_dump(exclude_none=True)


class RecallCandidate(BaseModel):
    """API候选结构"""
    name: str
    description: Optional[str] = None


class VerificationType(str, Enum):
    """验证类型"""
    CORRECT = "CORRECT"
    PARTIAL = "PARTIAL"
    INCORRECT = "INCORRECT"


class VerificationResult(BaseModel):
    """验证结果模型"""
    type: VerificationType
    reason: str
    confidence: float = 0.0


# ==================== 审核相关 ====================

class ReviewTaskType(str, Enum):  # 添加这个缺失的枚举
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


class RuntimeResult(BaseModel):
    """运行时路由结果"""
    status: str
    error: Optional[str] = None
    api_name: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    invoked_sql: Optional[str] = None
    data: Optional[List[list]] = None
    columns: Optional[List[str]] = None
    row_count: Optional[int] = None
    exec_result: Optional[Dict[str, Any]] = None
    verification: Optional[VerificationResult] = None
    correction_needed: bool = False
    source: Optional[str] = None


class ReviewTask(BaseModel):
    """审核任务模型"""
    
    # 基本信息
    task_id: str = Field(..., description="任务唯一ID")
    task_type: ReviewTaskType = Field(..., description="任务类型")
    status: ReviewStatus = Field(default=ReviewStatus.PENDING)
    created_at: str = Field(default_factory=lambda: __import__('datetime').datetime.now().isoformat())
    updated_at: Optional[str] = None
    
    # 来源信息
    source_query: str = Field(..., description="原始用户查询")
    source_api_name: Optional[str] = Field(None, description="来源API名称")
    
    # 运行时纠错专用
    candidate_tables: List[str] = Field(default_factory=list, description="候选表列表")
    wrong_api: Optional[Dict] = Field(None, description="错误选中的API")
    correct_api: Optional[Dict] = Field(None, description="正确的API")
    distinction_instruction: Optional[str] = Field(None, description="区分指令")
    
    # Schema内容（新API或修正后的API）
    proposed_schema: Optional[Dict] = Field(None, description="提议的Schema")
    
    # 审核信息
    reviewer: Optional[str] = Field(None, description="审核人")
    review_comment: Optional[str] = Field(None, description="审核意见")
    final_schema: Optional[Dict] = Field(None, description="最终确定的Schema")
    
    # 扩展信息
    metadata: Dict = Field(default_factory=dict, description="扩展元数据")
    
    class Config:
        extra = "allow"


class BatchReviewResult(BaseModel):
    """批量审核结果"""
    total: int
    approved: int
    rejected: int
    modified: int
    pending: int
    details: List[Dict]


# ==================== 工具函数 ====================

def create_api_schema_from_sql(
    name: str,
    description: str,
    sql: str,
    table: str,
    slot_mapping: Dict[str, str],
    query_type: str
) -> APISchema:
    """
    从SQL快速创建API Schema（简化版）
    
    Args:
        name: API名称
        description: 描述
        sql: SQL模板
        table: 主表
        slot_mapping: 参数映射
        query_type: 查询类型
    
    Returns:
        APISchema对象
    """
    # 提取slot定义
    input_props = {}
    required_slots = []
    
    for slot_name in slot_mapping.keys():
        input_props[slot_name] = {
            "type": "string",
            "description": f"参数: {slot_name}"
        }
        required_slots.append(slot_name)
    
    return APISchema(
        name=name,
        description=description,
        inputSchema={
            "type": "object",
            "properties": input_props,
            "required": required_slots
        },
        outputSchema={
            "type": "array",
            "items": {"type": "object"}
        },
        bound_sql=sql,
        slot_mapping=slot_mapping,
        query_type=query_type,
        table=table,
        examples=[]
    )