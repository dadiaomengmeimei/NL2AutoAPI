#!/usr/bin/env python3
"""审计 Layer-A query 语义质量与重复簇。"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path


def normalize_query(query: str) -> str:
    q = (query or "").strip().lower()
    q = re.sub(r"[\s\u3000]+", "", q)
    q = re.sub(r"[，。！？,.!?；;：:'\"“”‘’（）()\[\]{}<>《》]", "", q)
    fillers = [
        "请问", "请帮我", "帮我", "我想知道", "我想查一下", "我想看看", "能不能", "能给我看看", "能帮我看看",
        "一下", "现在", "当前", "咱们", "公司", "给我", "看看", "统计一下"
    ]
    for token in fillers:
        q = q.replace(token, "")
    return q


def has_constraint_hint(query: str) -> bool:
    hints = [
        "指定", "某", "某个", "某类", "某位", "该", "这个", "这位", "哪位", "邮箱", "员工ID", "id",
        "部门", "业务", "地区", "地点", "状态", "类型", "岗位", "职级", "姓名", "名字", "路径", "编号", "条件"
    ]
    return any(h in (query or "") for h in hints)


def looks_like_total_count(query: str) -> bool:
    patterns = ["一共有多少", "总共有多少", "多少员工", "总人数", "整体的人数", "公司现在有多少"]
    return any(p in (query or "") for p in patterns)


def semantic_issue(query: str, sql: str, query_type: str) -> str | None:
    sql_norm = f" {(sql or '').lower()} "
    has_where_slot = " where " in sql_norm and ":" in sql_norm
    has_group_by = " group by " in sql_norm
    has_count = "count(" in sql_norm
    exact_equal_lookup = "select *" in sql_norm and has_where_slot and " like " not in sql_norm

    if has_where_slot and has_count and not has_group_by:
        if looks_like_total_count(query) and not has_constraint_hint(query):
            return "问句像全量统计但SQL带筛选"
    if has_group_by and looks_like_total_count(query):
        return "问句像总人数但SQL是分组统计"
    if exact_equal_lookup or query_type == "exact_query":
        fuzzy_markers = ["开头", "有哪些", "都有哪些", "名单", "列表", "怎么叫", "昵称", "写法"]
        if any(m in (query or "") for m in fuzzy_markers):
            return "问句像模糊列表但SQL是精确匹配"
    return None


def main() -> int:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("./output/base_staff/valid.jsonl")
    if not path.exists():
        print(f"文件不存在: {path}")
        return 1

    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if obj.get("layer_tag") == "Layer-A":
            rows.append(obj)

    print(f"Layer-A rows: {len(rows)}")

    clusters = defaultdict(list)
    issues = []
    for r in rows:
        q = r.get("query", "")
        api = r.get("api_schema", {}) or {}
        sql = api.get("bound_sql", "")
        key = (r.get("query_type", ""), normalize_query(q))
        clusters[key].append(q)
        issue = semantic_issue(q, sql, r.get("query_type", ""))
        if issue:
            issues.append((issue, q, sql))

    dup_clusters = [(k, v) for k, v in clusters.items() if len(v) > 1]
    print(f"Near-duplicate clusters: {len(dup_clusters)}")
    for key, vals in sorted(dup_clusters, key=lambda x: -len(x[1]))[:10]:
        print(f"\n[DUP] {key} size={len(vals)}")
        for q in vals[:5]:
            print(" -", q)

    print(f"\nSemantic issues: {len(issues)}")
    for issue, q, sql in issues[:20]:
        print(f"\n[ISSUE] {issue}")
        print("Q  :", q)
        print("SQL:", sql)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
