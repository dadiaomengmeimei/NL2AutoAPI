"""统一日志入口"""
import logging
import os
from pathlib import Path


def setup_logging(log_dir: str = "./logs", log_file: str = "nl2autoapi.log", level: int = logging.INFO):
    """初始化日志配置（文件 + 控制台）"""
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("nl2autoapi")
    logger.setLevel(level)

    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)-8s [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(fmt)
        logger.addHandler(console_handler)

        file_handler = logging.FileHandler(os.path.join(log_dir, log_file), encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    return logger


def get_logger():
    logger = logging.getLogger("nl2autoapi")
    if not logger.handlers:
        setup_logging()
    return logger
