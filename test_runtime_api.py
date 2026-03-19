#!/usr/bin/env python3
"""
Test script for the two runtime API endpoints:
  1. POST /runtime/resolve            — resolve only (no generate fallback)
  2. POST /runtime/resolve-with-generate — resolve + generate fallback

Usage:
    python test_runtime_api.py [--host HOST] [--port PORT]
"""

import argparse
import json
import sys
import time
import requests


# ── Test queries ────────────────────────────────────────────────
TEST_QUERIES = [
    # --- Simple exact queries (should hit existing APIs) ---
    "帮我查一下张三的入职日期",
    "查询工号为10086的员工信息",
    "帮我查一下李四的办公地点",
    "帮我看看王五的部门是什么",
    "帮我看看员工编号为20001的正式姓名和显示姓名",

    # --- Aggregate queries ---
    "统计一下在职员工有多少人",
    "统计每个部门的在职员工人数",
    "帮我看看2024年入职的员工有多少人",

    # --- Edge cases / potentially new patterns (may need generate) ---
    "查一下深圳办公区有哪些员工",
    "有没有叫赵六的员工？帮我查一下他的工号和入职时间",
]


def call_runtime(base_url: str, endpoint: str, query: str, timeout: int = 60) -> dict:
    """Call a runtime endpoint and return the parsed response."""
    url = f"{base_url}{endpoint}"
    payload = {"query": query}
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        return resp.json()
    except requests.exceptions.ConnectionError:
        return {"error": f"Connection refused: {url}"}
    except requests.exceptions.Timeout:
        return {"error": f"Timeout after {timeout}s"}
    except Exception as e:
        return {"error": str(e)}


def fmt_result(r: dict) -> str:
    """Format a runtime response for display."""
    status = r.get("status", "?")
    route = r.get("route", "?")
    api_name = r.get("api_name") or "-"
    filled_sql = r.get("filled_sql") or "-"
    error = r.get("error") or ""
    review_task = r.get("review_task_id") or ""

    lines = [
        f"  status={status}  route={route}",
        f"  api={api_name}",
        f"  sql={filled_sql[:120]}{'...' if len(filled_sql) > 120 else ''}",
    ]
    if error:
        lines.append(f"  error={error}")
    if review_task:
        lines.append(f"  review_task={review_task}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Test runtime API endpoints")
    parser.add_argument("--host", default="127.0.0.1", help="API host")
    parser.add_argument("--port", type=int, default=7862, help="API port")
    parser.add_argument("--timeout", type=int, default=60, help="Request timeout")
    parser.add_argument("--queries", nargs="*", help="Custom queries (overrides built-in list)")
    args = parser.parse_args()

    base_url = f"http://{args.host}:{args.port}"
    queries = args.queries if args.queries else TEST_QUERIES

    # Health check
    print(f"🔍 Checking health at {base_url}/health ...")
    try:
        health = requests.get(f"{base_url}/health", timeout=5).json()
        print(f"   ✅ {health.get('status')} — {health.get('service')}\n")
    except Exception as e:
        print(f"   ❌ Health check failed: {e}")
        print("   Please ensure the review server is running (python main.py review)")
        sys.exit(1)

    endpoints = [
        ("/runtime/resolve", "Resolve Only (no generate)"),
        ("/runtime/resolve-with-generate", "Resolve + Generate Fallback"),
    ]

    results = {}
    for endpoint, label in endpoints:
        print(f"\n{'='*70}")
        print(f"📡 Endpoint: {label}")
        print(f"   {base_url}{endpoint}")
        print(f"{'='*70}")

        ep_results = []
        for i, q in enumerate(queries, 1):
            print(f"\n[{i}/{len(queries)}] Query: {q}")
            t0 = time.time()
            r = call_runtime(base_url, endpoint, q, timeout=args.timeout)
            elapsed = time.time() - t0
            print(fmt_result(r))
            print(f"  ⏱️  {elapsed:.2f}s")
            ep_results.append({
                "query": q,
                "status": r.get("status"),
                "route": r.get("route"),
                "api_name": r.get("api_name"),
                "elapsed": round(elapsed, 2),
                "error": r.get("error"),
            })

        results[endpoint] = ep_results

    # Summary
    print(f"\n\n{'='*70}")
    print("📊 Summary")
    print(f"{'='*70}")
    for endpoint, label in endpoints:
        ep = results[endpoint]
        total = len(ep)
        success = sum(1 for r in ep if r["status"] == "success")
        failed = total - success
        avg_time = sum(r["elapsed"] for r in ep) / total if total else 0
        print(f"\n  {label}:")
        print(f"    Total: {total}  Success: {success}  Failed: {failed}  Avg: {avg_time:.2f}s")

    # Save detailed results
    out_path = "test_runtime_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n💾 Detailed results saved to: {out_path}")


if __name__ == "__main__":
    main()
