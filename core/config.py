"""核心配置模块。"""

import os
from dataclasses import dataclass, field


def _env_int(name: str, default: int) -> int:
    """从环境变量读取整数配置。"""
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    """从环境变量读取浮点配置。"""
    return float(os.getenv(name, str(default)))


@dataclass
class PipelineConfig:
    """流水线配置。"""
    iterations: int = 100
    output_dir: str = "./output"

    # 生成配置
    sql_temperature: float = 0.7
    query_temperature: float = 0.7

    # 验证配置
    do_round_trip: bool = True
    max_retries: int = 3

    # 审核配置
    auto_approve: bool = False
    review_queue_path: str = "./output/base_staff/review_queue.jsonl"

    # 扩展配置
    enable_feedback: bool = True
    min_confidence: float = 0.8

    # 数据路径
    valid_path: str = "./output/base_staff/valid.jsonl"
    invalid_path: str = "./output/base_staff/invalid.jsonl"
    schema_file: str = ""


@dataclass
class DBConfig:
    """数据库配置。"""
    host: str = field(default_factory=lambda: os.getenv("DB_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: _env_int("DB_PORT", 3306))
    user: str = field(default_factory=lambda: os.getenv("DB_USER", "root"))
    password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", ""))
    database: str = field(default_factory=lambda: os.getenv("DB_NAME", "test"))
    charset: str = field(default_factory=lambda: os.getenv("DB_CHARSET", "utf8mb4"))


# 全局配置实例
pipeline_config = PipelineConfig()

# 全局数据库配置
db_config = DBConfig()

def get_db_config() -> dict:
    """获取数据库配置。"""
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": _env_int("DB_PORT", 3306),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", "test"),
        "charset": os.getenv("DB_CHARSET", "utf8mb4"),
    }


@dataclass
class LLMConfig:
    """LLM 调用配置。"""
    model: str = field(default_factory=lambda: os.getenv("LLM_MODEL", "gpt-4o-mini"))
    api_key: str = field(default_factory=lambda: os.getenv("LLM_API_KEY", os.getenv("OPENAI_API_KEY", "")))
    base_url: str = field(default_factory=lambda: os.getenv("LLM_BASE_URL", os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")))
    max_tokens: int = field(default_factory=lambda: _env_int("LLM_MAX_TOKENS", 2048))
    timeout: int = field(default_factory=lambda: _env_int("LLM_TIMEOUT", 60))
    max_retries: int = field(default_factory=lambda: _env_int("LLM_MAX_RETRIES", 3))
    retry_delay: float = field(default_factory=lambda: _env_float("LLM_RETRY_DELAY", 1.0))
    temperature: float = field(default_factory=lambda: _env_float("LLM_TEMPERATURE", 0.01))
    top_p: float = field(default_factory=lambda: _env_float("LLM_TOP_P", 0.8))
    extra_body: dict = field(default_factory=dict)


# 全局LLM配置实例
llm_config = LLMConfig()


def get_llm_config() -> dict:
    """获取 LLM 配置。"""
    return {
        "model": llm_config.model,
        "api_key": llm_config.api_key,
        "base_url": llm_config.base_url,
        "max_tokens": llm_config.max_tokens,
        "timeout": llm_config.timeout,
        "max_retries": llm_config.max_retries,
        "retry_delay": llm_config.retry_delay,
        "temperature": llm_config.temperature,
        "top_p": llm_config.top_p,
        "extra_body": llm_config.extra_body,
    }