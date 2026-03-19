"""归一化 query gate 原因，聚类成少量标准类别。"""

from __future__ import annotations

import json
import os
from collections import Counter


def categorize(reason: str) -> str:
    r = (reason or "").strip()
    if not r:
        return "其他"

    if any(k in r for k in ["字符长度", "长度分布", "机器统计偏好"]):
        return "技术统计偏好"
    if any(k in r for k in ["前N条", "前100", "机械限定"]):
        return "非自然取样目标"
    if any(k in r for k in ["技术模板", "技术需求模板", "像技术"]):
        return "技术模板问法"
    if any(k in r for k in ["拼接", "堆叠", "指标"]):
        return "指标拼接过载"
    if any(k in r for k in ["无具体", "缺乏场景", "无业务场景"]):
        return "缺少具体业务约束"
    if any(k in r for k in ["业务目标", "场景"]):
        return "业务目标不清"
    return "其他"


def normalize(input_path: str, output_path: str):
    if not os.path.exists(input_path):
        raise FileNotFoundError(input_path)

    with open(input_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError("query_gate_rules.json 应为 dict")

    merged = Counter()
    details = {}

    for reason, cnt in raw.items():
        cat = categorize(reason)
        merged[cat] += int(cnt)
        details.setdefault(cat, []).append({"reason": reason, "count": int(cnt)})

    for cat in details:
        details[cat].sort(key=lambda x: x["count"], reverse=True)

    payload = {
        "total_rejected": int(sum(merged.values())),
        "category_counts": dict(merged),
        "category_details": details,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return payload


def main():
    base = "./output"
    input_path = os.path.join(base, "query_gate_rules.json")
    output_path = os.path.join(base, "query_gate_rules_normalized.json")
    payload = normalize(input_path, output_path)
    print(f"已输出: {output_path}")
    print(f"total_rejected={payload['total_rejected']}")
    for k, v in sorted(payload["category_counts"].items(), key=lambda x: x[1], reverse=True):
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
