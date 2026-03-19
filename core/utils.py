"""
通用工具函数
"""
import json
import os
import re
from datetime import datetime
from typing import Any

from core.logger import get_logger

logger = get_logger()


def _with_record_time(obj: Any) -> Any:
    if isinstance(obj, dict):
        if "record_time" not in obj:
            obj = {**obj, "record_time": datetime.now().isoformat()}
    return obj


def save_jsonl(path: str, obj: dict, mode: str = "a"):
    """
    追加写入JSONL文件
    
    Args:
        path: 文件路径
        obj: 要写入的对象
        mode: 打开模式，默认追加"a"
    """
    # 确保目录存在
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    
    payload = _with_record_time(obj)
    with open(path, mode, encoding="utf8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def _normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", "", (sql or "")).strip().lower()


def _normalize_query(query: str) -> str:
    q = (query or "").strip().lower()
    q = re.sub(r"[\s\u3000]+", "", q)
    q = re.sub(r"[，。！？,.!?；;：:'\"“”‘’（）()\[\]{}<>《》]", "", q)
    return q


def _query_char_ngrams(text: str, n: int = 2) -> set[str]:
    if not text:
        return set()
    if len(text) <= n:
        return {text}
    return {text[i:i + n] for i in range(len(text) - n + 1)}


def _normalize_query_semantic(query: str) -> str:
    q = _normalize_query(query)
    # 去掉常见口语填充，保留业务主干语义
    fillers = [
        "请问", "请帮我", "帮我", "我想知道", "我想查一下", "我想看看", "能不能", "能给我看看", "能帮我看看",
        "一下", "一下子", "现在", "当前", "咱们", "公司", "员工表里", "员工表中", "给我", "看看", "统计一下"
    ]
    for token in fillers:
        q = q.replace(token, "")
    return q


def _is_similar_query(query_a: str, query_b: str, threshold: float = 0.82) -> bool:
    a = _normalize_query_semantic(query_a)
    b = _normalize_query_semantic(query_b)
    if not a or not b:
        return False
    if a == b:
        return True
    if a in b or b in a:
        shorter = min(len(a), len(b))
        longer = max(len(a), len(b))
        if longer > 0 and shorter / longer >= 0.78:
            return True

    grams_a = _query_char_ngrams(a)
    grams_b = _query_char_ngrams(b)
    if not grams_a or not grams_b:
        return False
    overlap = len(grams_a & grams_b)
    union = len(grams_a | grams_b)
    score = overlap / union if union else 0.0
    return score >= threshold


def _extract_record_sql(record: dict) -> str:
    if not isinstance(record, dict):
        return ""
    sql = record.get("sql")
    if isinstance(sql, str) and sql.strip():
        return sql
    api_schema = record.get("api_schema")
    if isinstance(api_schema, dict):
        bound_sql = api_schema.get("bound_sql")
        if isinstance(bound_sql, str) and bound_sql.strip():
            return bound_sql
    return ""


def _extract_record_query(record: dict) -> str:
    if not isinstance(record, dict):
        return ""
    query = record.get("query")
    if isinstance(query, str) and query.strip():
        return query
    api_schema = record.get("api_schema")
    if isinstance(api_schema, dict):
        q = api_schema.get("query")
        if isinstance(q, str) and q.strip():
            return q
    return ""


def save_jsonl_upsert_sql(path: str, obj: dict) -> bool:
    """
    Upsert write to JSONL: if a record with the same SQL or similar query exists,
    replace it with the new record; otherwise append.

    Returns:
        True: successfully written (inserted or replaced)
        False: file write error
    """
    incoming_sql = _normalize_sql(_extract_record_sql(obj))
    incoming_query = _normalize_query(_extract_record_query(obj))
    raw_incoming_query = _extract_record_query(obj)

    if not incoming_sql and not incoming_query:
        save_jsonl(path, obj)
        return True

    replaced = False
    updated_records = []

    if os.path.exists(path):
        with open(path, "r", encoding="utf8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning(f"跳过第{line_num}行解析错误: JSON解析失败")
                    updated_records.append(line)
                    continue

                existing_sql = _normalize_sql(_extract_record_sql(record))
                existing_query = _normalize_query(_extract_record_query(record))
                raw_existing_query = _extract_record_query(record)

                is_dup = False
                if incoming_sql and existing_sql and existing_sql == incoming_sql:
                    is_dup = True
                elif incoming_query and existing_query and existing_query == incoming_query:
                    is_dup = True
                elif incoming_query and existing_query and _is_similar_query(raw_incoming_query, raw_existing_query):
                    is_dup = True

                if is_dup and not replaced:
                    # Replace old record with new one
                    payload = _with_record_time(obj)
                    updated_records.append(json.dumps(payload, ensure_ascii=False, default=str))
                    replaced = True
                elif is_dup and replaced:
                    # Additional duplicate, skip it
                    continue
                else:
                    updated_records.append(line)

    if replaced:
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        with open(path, "w", encoding="utf8") as f:
            for rec_line in updated_records:
                f.write(rec_line.rstrip("\n") + "\n")
        return True
    else:
        save_jsonl(path, obj)
        return True


def save_jsonl_dedup_sql(path: str, obj: dict, allow_same_sql_duplicates: bool = False) -> bool:
    """
    按SQL去重写入JSONL。

    Returns:
        True: 成功写入
        False: 发现同SQL记录，跳过写入
    """
    incoming_sql = _normalize_sql(_extract_record_sql(obj))
    incoming_query = _normalize_query(_extract_record_query(obj))
    if not incoming_sql:
        if not incoming_query:
            save_jsonl(path, obj)
            return True

    if os.path.exists(path):
        with open(path, "r", encoding="utf8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning(f"跳过第{line_num}行解析错误: JSON解析失败")
                    continue

                existing_sql = _normalize_sql(_extract_record_sql(record))
                existing_query = _normalize_query(_extract_record_query(record))
                if incoming_sql and existing_sql and existing_sql == incoming_sql and not allow_same_sql_duplicates:
                    return False
                if incoming_query and existing_query and existing_query == incoming_query:
                    return False
                if incoming_query and existing_query:
                    raw_existing_query = _extract_record_query(record)
                    raw_incoming_query = _extract_record_query(obj)
                    if _is_similar_query(raw_incoming_query, raw_existing_query):
                        return False

    save_jsonl(path, obj)
    return True


def load_jsonl(path: str) -> list[dict]:
    """
    加载JSONL文件
    
    Args:
        path: 文件路径
    
    Returns:
        记录列表，文件不存在返回空列表
    """
    if not os.path.exists(path):
        return []
    
    records = []
    with open(path, "r", encoding="utf8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                logger.warning(f"跳过第{line_num}行解析错误: {e}")
                continue
    
    return records


def overwrite_jsonl(path: str, records: list[dict]):
    """覆盖写入JSONL文件"""
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with open(path, "w", encoding="utf8") as f:
        for obj in records:
            payload = _with_record_time(obj)
            f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def extract_slots(sql: str) -> list[str]:
    """
    提取SQL中所有 :param_name 形式的slot占位符
    
    Args:
        sql: SQL语句
    
    Returns:
        保序去重的slot名称列表
    """
    return list(dict.fromkeys(re.findall(r":(\w+)", sql)))


def fill_sql_with_values(sql: str, slot_values: dict) -> str:
    """
    用实际值填充SQL中的slot占位符
    
    Args:
        sql: 含slot的SQL
        slot_values: slot名到值的映射
    
    Returns:
        填充后的SQL
    """
    result = sql
    for slot, val in slot_values.items():
        replacement = f"'{val}'" if isinstance(val, str) else str(val)
        result = result.replace(f":{slot}", replacement)
    return result


def parse_llm_json(text: str):
    """解析LLM返回JSON，支持markdown代码块和普通文本。"""
    if not isinstance(text, str):
        return None

    s = text.strip()
    # Remove surrounding ``` blocks
    if s.startswith('```') and s.endswith('```'):
        # strip exactly one layer of backticks
        s = s.strip('`').strip()

    # remove explicit json fences
    s = re.sub(r'^```json\s*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'```$', '', s).strip()

    try:
        return json.loads(s)
    except json.JSONDecodeError:
        # if it is not a JSON object, do a naive attempt with matched braces
        return None


def call_llm(prompt: str) -> str:
    """代理到core.llm的call_llm。"""
    try:
        from .llm import call_llm as _call_llm
        return _call_llm(prompt)
    except ImportError:
        raise


def generate_api_name(table: str, slots: list[str], query_type: str, desc_hint: str | None = None) -> str:
    """根据table/slots/query_type生成API名称"""
    table_part = sanitize_filename(table).lower().replace('-', '_')
    query_part = query_type or 'query'

    suffix = ''
    if query_part.startswith('aggregate'):
        suffix = 'count'
    elif query_part.startswith('list'):
        suffix = 'list'
    elif query_part.startswith('exact'):
        suffix = 'get'
    else:
        suffix = 'get'

    if slots:
        slot_part = '_'.join([sanitize_filename(s).lower() for s in slots])
        name = f"{table_part}_{suffix}_by_{slot_part}"
    else:
        name = f"{table_part}_{suffix}"

    if desc_hint:
        hint = sanitize_filename(desc_hint).lower().replace('__', '_')
        if hint:
            name = f"{name}_{hint[:30]}"

    name = re.sub(r'_+', '_', name).strip('_')

    if not name:
        name = f"{table_part}_api"

    if not name[0].isalpha():
        name = f"api_{name}"

    return name


def get_safe_filename(name: str) -> str:
    """安全文件名，移除特殊字符并限制长度。"""
    safe = sanitize_filename(name)
    if len(safe) > 100:
        safe = safe[:100]
    return safe


def sanitize_api_name(name: str) -> str:
    """安全API名称，类似safe_filename但保留"_""" 
    api_name = sanitize_filename(name)
    api_name = api_name.replace('-', '_').replace('__', '_')
    api_name = re.sub(r'[^a-zA-Z0-9_]', '_', api_name)
    if len(api_name) > 100:
        api_name = api_name[:100]
    if api_name and not api_name[0].isalpha():
        api_name = f"api_{api_name}"
    return api_name


def sanitize_filename(name: str) -> str:
    """
    将字符串转换为安全的文件名
    
    Args:
        name: 原始名称
    
    Returns:
        安全的文件名
    """
    # 替换不安全字符
    safe = re.sub(r'[<>:"/\\|?*]', '_', name)
    # 限制长度
    return safe[:100]


def _default_value(col_type: str):
    """根据列类型生成默认值"""
    t = col_type.upper()
    if "INT" in t:
        return 1
    if "DATE" in t:
        return "2023-01-01"
    if "FLOAT" in t or "DOUBLE" in t or "DECIMAL" in t:
        return 1.0
    if "BOOL" in t or "TINYINT(1)" in t:
        return 1
    return "test"