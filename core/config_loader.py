"""
配置文件加载器

支持从 YAML 文件加载配置，并支持环境变量覆盖
优先级：命令行参数 > 环境变量 > 配置文件 > 默认值
"""

import os
import yaml
from typing import Optional, Dict, Any
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class DBConfigFromFile:
    """数据库配置"""
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "test"
    charset: str = "utf8mb4"


@dataclass
class LLMConfigFromFile:
    """LLM配置"""
    model: str = "gpt-4o-mini"
    api_key: str = ""
    base_url: str = "https://api.openai.com/v1"
    max_tokens: int = 2048
    timeout: int = 60
    max_retries: int = 3
    retry_delay: float = 1.0
    temperature: float = 0.01
    top_p: float = 0.8


@dataclass
class SchemaConfigFromFile:
    """Schema配置"""
    path: str = ""
    table_names: list[str] = field(default_factory=list)


@dataclass
class BuildConfigFromFile:
    """构建配置"""
    output_dir: str = "./output"
    iterations: int = 100
    rule_rounds: int = 1
    skip_rule: bool = False
    skip_llm: bool = False
    require_db: bool = True
    require_llm: bool = True
    enable_query_gate: bool = True
    export_format: str = "all"


@dataclass
class RuntimeConfigFromFile:
    """运行时配置"""
    valid_path: str = "./output/base_staff/valid.jsonl"
    review_queue: str = "./output/base_staff/review_queue.jsonl"
    mode: str = "interactive"
    num_tests: int = 10
    num_queries: int = 20
    batch_size: int = 5
    max_rounds: int = 3
    table_top_k: int = 3   # TopK candidate tables to recall per query
    api_top_k: int = 5     # TopK candidate APIs to recall per table shard


@dataclass
class ReviewConfigFromFile:
    """审核界面配置"""
    port: int = 7860
    share: bool = False
    invalid_path: str = "./output/base_staff/invalid.jsonl"
    valid_path: str = "./output/base_staff/valid.jsonl"
    recorrect_path: str = "./output/base_staff/recorrect.jsonl"
    review_queue: str = "./output/base_staff/review_queue.jsonl"
    auth_users: list[str] = field(default_factory=list)  # whitelist usernames; empty = no auth
    language: str = "en"  # UI language: "en" or "zh"


@dataclass
class LoggingConfigFromFile:
    """日志配置"""
    log_dir: str = "./logs"
    level: str = "INFO"


@dataclass
class AppConfig:
    """应用总配置"""
    database: DBConfigFromFile = field(default_factory=DBConfigFromFile)
    llm: LLMConfigFromFile = field(default_factory=LLMConfigFromFile)
    schema: SchemaConfigFromFile = field(default_factory=SchemaConfigFromFile)
    build: BuildConfigFromFile = field(default_factory=BuildConfigFromFile)
    runtime: RuntimeConfigFromFile = field(default_factory=RuntimeConfigFromFile)
    review: ReviewConfigFromFile = field(default_factory=ReviewConfigFromFile)
    logging: LoggingConfigFromFile = field(default_factory=LoggingConfigFromFile)


class ConfigLoader:
    """配置加载器"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置加载器
        
        Args:
            config_path: 配置文件路径，默认为项目根目录的 config.yaml
        """
        if config_path is None:
            # 默认使用项目根目录的 config.yaml
            project_root = Path(__file__).parent.parent
            config_path = project_root / "config.yaml"
        
        self.config_path = Path(config_path)
        self._config: Optional[AppConfig] = None
    
    def load(self) -> AppConfig:
        """加载配置"""
        if self._config is not None:
            return self._config
        
        # 1. 从文件加载基础配置
        config_dict = {}
        if self.config_path.exists():
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_dict = yaml.safe_load(f) or {}
            print(f"✓ 加载配置文件: {self.config_path}")
        else:
            print(f"⚠️  配置文件不存在: {self.config_path}，使用默认配置")
        
        # 2. 应用环境变量覆盖
        config_dict = self._apply_env_overrides(config_dict)
        
        # 3. 构建配置对象
        self._config = self._build_config(config_dict)
        
        return self._config
    
    def _apply_env_overrides(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """应用环境变量覆盖"""
        # 数据库配置
        db = config_dict.setdefault("database", {})
        db["host"] = os.getenv("DB_HOST", db.get("host", "localhost"))
        db["port"] = int(os.getenv("DB_PORT", db.get("port", 3306)))
        db["user"] = os.getenv("DB_USER", db.get("user", "root"))
        db["password"] = os.getenv("DB_PASSWORD", db.get("password", ""))
        db["database"] = os.getenv("DB_NAME", db.get("database", "test"))
        db["charset"] = os.getenv("DB_CHARSET", db.get("charset", "utf8mb4"))
        
        # LLM配置
        llm = config_dict.setdefault("llm", {})
        llm["model"] = os.getenv("LLM_MODEL", llm.get("model", "gpt-3.5-turbo"))
        llm["api_key"] = os.getenv("LLM_API_KEY", os.getenv("OPENAI_API_KEY", llm.get("api_key", "")))
        llm["base_url"] = os.getenv("LLM_BASE_URL", os.getenv("OPENAI_BASE_URL", llm.get("base_url", "https://api.openai.com/v1")))
        llm["max_tokens"] = int(os.getenv("LLM_MAX_TOKENS", llm.get("max_tokens", 2048)))
        llm["timeout"] = int(os.getenv("LLM_TIMEOUT", llm.get("timeout", 60)))
        llm["max_retries"] = int(os.getenv("LLM_MAX_RETRIES", llm.get("max_retries", 3)))
        llm["retry_delay"] = float(os.getenv("LLM_RETRY_DELAY", llm.get("retry_delay", 1.0)))
        llm["temperature"] = float(os.getenv("LLM_TEMPERATURE", llm.get("temperature", 0.01)))
        llm["top_p"] = float(os.getenv("LLM_TOP_P", llm.get("top_p", 0.8)))
        
        return config_dict
    
    def _build_config(self, config_dict: Dict[str, Any]) -> AppConfig:
        """构建配置对象"""
        return AppConfig(
            database=DBConfigFromFile(**config_dict.get("database", {})),
            llm=LLMConfigFromFile(**config_dict.get("llm", {})),
            schema=SchemaConfigFromFile(**config_dict.get("schema", {})),
            build=BuildConfigFromFile(**config_dict.get("build", {})),
            runtime=RuntimeConfigFromFile(**config_dict.get("runtime", {})),
            review=ReviewConfigFromFile(**config_dict.get("review", {})),
            logging=LoggingConfigFromFile(**config_dict.get("logging", {})),
        )
    
    def update_db_config(self):
        """更新全局数据库配置"""
        from core.config import db_config
        
        config = self.load()
        db_config.host = config.database.host
        db_config.port = config.database.port
        db_config.user = config.database.user
        db_config.password = config.database.password
        db_config.database = config.database.database
        db_config.charset = config.database.charset
    
    def update_llm_config(self):
        """更新全局LLM配置"""
        from core.config import llm_config
        
        config = self.load()
        llm_config.model = config.llm.model
        llm_config.api_key = config.llm.api_key
        llm_config.base_url = config.llm.base_url
        llm_config.max_tokens = config.llm.max_tokens
        llm_config.timeout = config.llm.timeout
        llm_config.max_retries = config.llm.max_retries
        llm_config.retry_delay = config.llm.retry_delay
        llm_config.temperature = config.llm.temperature
        llm_config.top_p = config.llm.top_p
    
    def update_pipeline_config(self):
        """更新全局流水线配置"""
        from core.config import pipeline_config
        
        config = self.load()
        pipeline_config.output_dir = config.build.output_dir
        pipeline_config.iterations = config.build.iterations
        pipeline_config.valid_path = os.path.join(config.build.output_dir, "dataset_valid.jsonl")
        pipeline_config.invalid_path = os.path.join(config.build.output_dir, "dataset_invalid.jsonl")
        pipeline_config.review_queue_path = config.runtime.review_queue
        pipeline_config.schema_file = config.schema.path
    
    def update_all_configs(self):
        """更新所有全局配置"""
        self.update_db_config()
        self.update_llm_config()
        self.update_pipeline_config()


# 全局配置加载器实例
_global_loader: Optional[ConfigLoader] = None
_global_loader_path: Optional[str] = None


def get_config_loader(config_path: Optional[str] = None) -> ConfigLoader:
    """获取全局配置加载器"""
    global _global_loader
    global _global_loader_path

    normalized_path = None
    if config_path is not None:
        normalized_path = str(Path(config_path).expanduser().resolve())

    if _global_loader is None or (_global_loader_path is not None and normalized_path is not None and _global_loader_path != normalized_path):
        _global_loader = ConfigLoader(config_path)
        _global_loader_path = str(_global_loader.config_path.expanduser().resolve())
    elif _global_loader_path is None and _global_loader is not None:
        _global_loader_path = str(_global_loader.config_path.expanduser().resolve())
    return _global_loader


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """加载配置（快捷方法）"""
    loader = get_config_loader(config_path)
    return loader.load()
