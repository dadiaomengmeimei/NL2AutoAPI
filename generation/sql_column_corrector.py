"""SQL 列名与占位符纠错（基于 schema 字段名）。"""

from __future__ import annotations

import re
from typing import Iterable

SQL_KEYWORDS = {
    "select", "from", "where", "group", "by", "order", "limit", "and", "or", "not",
    "between", "in", "is", "null", "like", "as", "on", "join", "left", "right", "inner",
    "outer", "count", "avg", "sum", "min", "max", "length", "distinct", "having", "asc", "desc"
}


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _field_map(schema_fields: dict | Iterable[str]) -> dict[str, str]:
    if isinstance(schema_fields, dict):
        names = list(schema_fields.keys())
    else:
        names = []
        for item in list(schema_fields):
            if isinstance(item, str):
                names.append(item)
            elif isinstance(item, dict):
                field_name = item.get("name")
                if isinstance(field_name, str) and field_name:
                    names.append(field_name)
            else:
                field_name = getattr(item, "name", None)
                if isinstance(field_name, str) and field_name:
                    names.append(field_name)
    mapping: dict[str, str] = {}
    for name in names:
        key = _norm(name)
        if not key:
            continue
        # 仅保留唯一映射，避免误替换
        if key in mapping and mapping[key] != name:
            mapping[key] = ""
        else:
            mapping[key] = name
    return {k: v for k, v in mapping.items() if v}


def correct_sql_columns(sql: str, table_name: str, schema_fields: dict | Iterable[str]) -> str:
    if not sql:
        return sql

    fmap = _field_map(schema_fields)
    if not fmap:
        return sql

    out = sql

    # 1) 修正占位符 :xxx / :slot_xxx
    def repl_slot(m: re.Match) -> str:
        raw = m.group(1)
        core = raw[5:] if raw.startswith("slot_") else raw
        mapped = fmap.get(_norm(core))
        if not mapped:
            return m.group(0)
        new_slot = f"slot_{mapped}" if raw.startswith("slot_") else mapped
        return f":{new_slot}"

    out = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", repl_slot, out)

    # 2) 修正列名 token（避免替换关键字、表名）
    token_re = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")
    pieces = []
    last = 0
    low_table = (table_name or "").lower()

    for m in token_re.finditer(out):
        token = m.group(1)
        low = token.lower()

        # 跳过关键字、表名、已是字段名、函数名语境
        if low in SQL_KEYWORDS or low == low_table:
            continue

        # 跳过 :slot 中的 token
        if m.start() > 0 and out[m.start() - 1] == ":":
            continue

        mapped = fmap.get(_norm(token))
        if not mapped or mapped == token:
            continue

        pieces.append(out[last:m.start()])
        pieces.append(mapped)
        last = m.end()

    if pieces:
        pieces.append(out[last:])
        out = "".join(pieces)

    return out
