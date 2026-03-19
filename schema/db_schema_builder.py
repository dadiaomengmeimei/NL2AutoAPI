"""从数据库构建预热所需 schema（按表过滤）。"""

from __future__ import annotations

from typing import Any


def _split_tables(raw_tables: str | list[str] | None) -> list[str]:
    if raw_tables is None:
        return []
    if isinstance(raw_tables, list):
        return [t.strip() for t in raw_tables if isinstance(t, str) and t.strip()]
    return [t.strip() for t in str(raw_tables).split(",") if t.strip()]


def build_schema_from_db(db_conn, db_name: str, table_names: str | list[str] | None = None) -> dict[str, Any]:
    """基于 information_schema 构建 SchemaLoader 兼容结构。"""
    if db_conn is None:
        raise RuntimeError("数据库连接不可用，无法从DB构建schema")

    selected_tables = _split_tables(table_names)
    schema = {
        "db_id": db_name,
        "tables": {}
    }

    with db_conn.cursor() as cursor:
        if selected_tables:
            placeholders = ",".join(["%s"] * len(selected_tables))
            cursor.execute(
                f"""
                SELECT TABLE_NAME, TABLE_COMMENT
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME IN ({placeholders})
                ORDER BY TABLE_NAME
                """,
                [db_name, *selected_tables],
            )
        else:
            cursor.execute(
                """
                SELECT TABLE_NAME, TABLE_COMMENT
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME
                """,
                [db_name],
            )

        table_rows = cursor.fetchall()

        for table_name, table_comment in table_rows:
            cursor.execute(
                """
                SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_COMMENT
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                [db_name, table_name],
            )
            col_rows = cursor.fetchall()

            fields = {}
            for col_name, col_type, is_nullable, col_key, col_comment in col_rows:
                fields[col_name] = {
                    "name": col_name,
                    "type": str(col_type).upper(),
                    "is_nullable": str(is_nullable).upper() == "YES",
                    "is_primary": str(col_key).upper() == "PRI",
                    "comment": col_comment or "",
                }

            schema["tables"][table_name] = {
                "comment": table_comment or "",
                "fields": fields,
            }

    return schema
