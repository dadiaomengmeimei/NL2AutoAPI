"""
数据库连接管理
"""
import pymysql
from contextlib import contextmanager
from typing import Optional, Generator
from .config import db_config
from .logger import get_logger

logger = get_logger()


class DatabaseManager:
    """数据库连接管理器"""
    
    _instance: Optional["DatabaseManager"] = None
    _connection: Optional[pymysql.Connection] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def connect(self) -> Optional[pymysql.Connection]:
        """建立数据库连接"""
        if self._connection is None or not self._connection.open:
            try:
                self._connection = pymysql.connect(
                    host=db_config.host,
                    port=db_config.port,
                    database=db_config.database,
                    user=db_config.user,
                    password=db_config.password,
                    charset=db_config.charset,
                    cursorclass=pymysql.cursors.Cursor,
                    autocommit=True,
                )
                logger.info("数据库连接成功：%s:%s/%s", db_config.host, db_config.port, db_config.database)
            except Exception as e:
                logger.warning("无法连接数据库: %s，将以无DB模式继续。", e)
                self._connection = None

        return self._connection
    
    def close(self):
        """关闭连接"""
        if self._connection and self._connection.open:
            self._connection.close()
            self._connection = None
    
    @contextmanager
    def cursor(self) -> Generator:
        """获取游标的上下文管理器"""
        conn = self.connect()
        if conn is None:
            logger.warning("当前没有数据库连接，无法获取游标")
            raise RuntimeError("没有数据库连接")

        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
    
    def execute(self, sql: str) -> dict:
        """执行SQL并返回结构化结果"""
        if self._connection is None or not self._connection.open:
            # Auto-connect if not connected yet
            self.connect()

        if self._connection is None:
            logger.warning("没有数据库连接，无法执行SQL: %s", sql)
            return {
                "status": "error",
                "error": "没有数据库连接，请检查数据库配置",
                "error_type": "ConnectionError",
            }

        try:
            with self.cursor() as cursor:
                cursor.execute(sql)
                
                if cursor.description:
                    cols = [c[0] for c in cursor.description]
                    rows = cursor.fetchall()
                    return {
                        "status": "success",
                        "columns": cols,
                        "data": [list(r) for r in rows[:5]],
                        "row_count": len(rows),
                        "all_rows": [list(r) for r in rows],  # 完整数据
                    }
                else:
                    return {
                        "status": "success",
                        "columns": [],
                        "data": [],
                        "row_count": cursor.rowcount,
                    }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
            }


# 全局数据库管理器
db_manager = DatabaseManager()


def get_db_connection() -> pymysql.Connection:
    """获取数据库连接（兼容旧代码）"""
    return db_manager.connect()


def execute_sql(db_conn, sql: str) -> dict:
    """
    执行SQL（兼容旧代码接口）
    如果db_conn为None，使用全局管理器
    """
    if db_conn is None:
        return db_manager.execute(sql)
    
    # 使用传入的连接
    try:
        cursor = db_conn.cursor()
        cursor.execute(sql)
        
        if cursor.description:
            cols = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            result = {
                "status": "success",
                "columns": cols,
                "data": [list(r) for r in rows[:5]],
                "row_count": len(rows),
            }
        else:
            result = {
                "status": "success",
                "columns": [],
                "data": [],
                "row_count": cursor.rowcount,
            }
        cursor.close()
        return result
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }