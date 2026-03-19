# NL2AutoAPI Review Platform — Usage Guide

## Table of Contents
- [1. Overview](#1-overview)
- [2. Quick Start](#2-quick-start)
- [3. Tab Reference](#3-tab-reference)
  - [3.1 Schema Browser & Auto-Fix](#31-schema-browser--auto-fix)
  - [3.2 Dataset](#32-dataset)
  - [3.3 Validation](#33-validation)
  - [3.4 Runtime Query](#34-runtime-query)
  - [3.5 Review Queue](#35-review-queue)
  - [3.6 Version History](#36-version-history)
  - [3.7 Statistics](#37-statistics)
- [4. Core Mechanisms](#4-core-mechanisms)
  - [4.1 Two-Layer Pre-build (Layer A + Layer B)](#41-two-layer-pre-build-layer-a--layer-b)
  - [4.2 LLM-as-a-Judge Auto-Review](#42-llm-as-a-judge-auto-review)
  - [4.3 Schema Feedback & Auto-Update (Diff-Driven)](#43-schema-feedback--auto-update-diff-driven)
  - [4.4 Cascade Updates](#44-cascade-updates)
  - [4.5 User-Edit Protection (`user_edited`)](#45-user-edit-protection-user_edited)
  - [4.6 Version Management & Binlog](#46-version-management--binlog)
  - [4.7 Concurrency Protection](#47-concurrency-protection)
- [5. Data Files](#5-data-files)
- [6. Configuration](#6-configuration)
- [7. FAQ](#7-faq)

---

## 1. Overview

NL2AutoAPI Review is an integrated platform for data production, human review, auto-correction, and runtime verification. Its goal is to map natural-language queries to precise API Schema + SQL pairs, and iteratively improve data quality through human-machine collaboration.

**Core data flow:**

```
Natural Language Query
    ↓
Layer A: Schema Understanding (Auto-Fix field descriptions)
    ↓
Layer B: Pre-build (generate candidate API + SQL pairs)
    ↓
Human Review (Approve / Edit / Reject)
    ↓
valid.jsonl (approved dataset)
    ↓
Runtime Verification (Route → Slot Fill → Execute → Verify)
    ↓
Schema Feedback (auto-analyze SQL/Query diffs → suggest field updates)
    ↓
Cascade Update (propagate description changes to related APIs)
```

## 2. Quick Start

```bash
cd nl2autoapi
python main.py review --port 7860
```

Open `http://localhost:7860` in your browser.

**Prerequisites:**
- `config.yaml` with correct database and LLM configuration
- Schema JSON file generated (e.g. `output/base_staff/schema.json`)

**Language setting:** Set `review.language` in `config.yaml` to `en` (default) or `zh`.

**Whitelist auth:** Set `review.auth_users` to a list of usernames. Leave empty to disable auth.

---

## 3. Tab Reference

### 3.1 Schema Browser & Auto-Fix

> **Purpose:** Manage table Schema definitions and auto-fix field descriptions.

| Feature | Description |
|---|---|
| **Generate Schema from DB** | Connect to database, auto-generate Schema JSON (fields, types, comments) |
| **Schema Editor** | Edit Schema JSON directly, manually adjust field descriptions |
| **Field Management** | Delete individual fields or smart-prune system/internal fields |
| **Global Auto-Fix** | LLM reviews all fields and improves descriptions in bulk |
| **Single Field Auto-Fix** | Target a specific field for multi-round improvement |
| **Pre-build** | Generate initial Query-SQL dataset from Schema |

**Typical workflow:**
1. Click "Generate Schema from DB"
2. Click "Smart Prune" to remove system fields (created_at, etl_flag, etc.)
3. Click "Global Auto-Fix" to let LLM improve all field descriptions
4. Manually review the results
5. Click "Run Pre-build" to generate candidate data

### 3.2 Dataset

> **Purpose:** Browse and edit approved valid records.

| Feature | Description |
|---|---|
| **Browse records** | Paginated view of all records in `valid.jsonl` |
| **Edit API** | Modify query, API name, description, SQL, or full API schema JSON |
| **Delete record** | Remove a record from the dataset |
| **Schema Feedback** | On save, auto-analyze SQL/Query changes and suggest field description updates |

**Key interaction:** After editing SQL or Query and clicking "Save Changes", the system auto-analyzes the diff and may display schema update suggestions.

### 3.3 Validation

> **Purpose:** Review Pre-build generated candidates one by one.

| Feature | Description |
|---|---|
| **✨ Concretize Query** | Replace abstract queries ("query employee info") with concrete instances ("query John's info") |
| **🤖 Auto-Generate SQL** | LLM drafts API name, description, SQL from the query |
| **🧠 Auto-Review** | LLM reviews current SQL and auto-fills API metadata |
| **✅ Approve** | Accept into `valid.jsonl` |
| **⏭️ Skip** | Skip for now |
| **❌ Reject** | Reject, save to `recorrect.jsonl` |
| **Schema Feedback** | On approval, auto-analyze SQL/Query diff |

**Typical workflow:**
1. System shows a candidate record with query and original SQL
2. If query is too abstract → "Concretize Query"
3. If SQL needs changes → edit manually or "Auto-Generate SQL"
4. Confirm → "Approve"
5. If schema feedback appears → review and "Apply Suggestions" or "Dismiss"

### 3.4 Runtime Query

> **Purpose:** Test end-to-end query execution in real time.

| Feature | Description |
|---|---|
| **Run Query** | Execute full Runtime pipeline: Route → Slot Fill → Execute → Verify |
| **Manual SQL mode** | Manually specify API Schema + SQL, test parameterization |
| **Fill from Record** | Auto-fill form from a previous execution record |
| **Import to Valid** | Import tested results directly into valid dataset |

**Runtime execution pipeline:**
```
Query → Recall (candidate API search) → Select Best
     → Slot Fill → Execute SQL → Verify (semantic check)
     → If failed: RAG Generate (use existing API context to generate new API)
     → If still failed: submit correction task to Review Queue
```

### 3.5 Review Queue

> **Purpose:** Process runtime-generated correction tasks.

| Feature | Description |
|---|---|
| **Task details** | Shows query, mismatched API, candidate tables, distinction instruction |
| **SQL (template)** | The `bound_sql` with `:slot` placeholders, editable |
| **🔍 Invoked SQL** | Read-only view of the actually-executed SQL (slots filled with real values) |
| **Review actions** | Approve / Reject / Next / Concretize / Auto-Generate / Auto-Review |
| **Schema Feedback** | On approval, analyzes the diff between invoked SQL and new SQL, plus Query changes |

**About Invoked SQL:**
- The editable SQL is the **template** (`bound_sql`) with `:name`, `:city` placeholders
- "🔍 Invoked SQL" shows the **actual SQL** executed at runtime with real parameter values
- Use invoked SQL as reference to understand what went wrong
- **Templates are stored on approval** (auto-parameterized), not the filled version

### 3.6 Version History

> **Purpose:** View all data modification binlogs and restore to any point in time.

| Feature | Description |
|---|---|
| **Summary** | Operation statistics per dataset |
| **Binlog detail** | View last 50 operations for any dataset |
| **Restore** | Restore a dataset to a specific ISO timestamp |
| **Supported datasets** | valid, invalid, schema, boundary, recorrect, review_queue |

### 3.7 Statistics

> **Purpose:** Quick overview of queue sizes and task type distribution.

---

## 4. Core Mechanisms

### 4.1 Two-Layer Pre-build (Layer A + Layer B)

The pre-build is not a single monolithic step. It follows a **two-layer architecture**:

**Layer A — Schema Understanding & Refinement:**
```
Database table → Pull structure → LLM auto-fix field descriptions
                                    ↓
                        Exploration loop (per field):
                        Generate test query → Generate SQL
                        → Execute against DB → Verify result
                        → If wrong: improve description → repeat
                                    ↓
                        High-quality schema.json
```

**Layer B — Query-SQL Pair Generation:**
```
Refined schema.json → LLM generates diverse queries per field
                    → For each query: generate SQL + API
                    → Validate SQL against real DB
                    → Auto-filter bad candidates
                    → Output: valid.jsonl + invalid.jsonl
```

**Why two layers?**
- Layer A ensures the LLM **truly understands** each field before generating SQL
- Layer B produces more accurate SQL thanks to refined descriptions
- **Layer A can be re-run independently** — improving a description doesn't require full re-generation
- After Layer A updates, the system uses **cascade updates** instead of full rebuild

**Full Pre-build is blocked** once `valid.jsonl` has data (especially user-edited records). The recommended path is diff-driven cascade updates instead.

### 4.2 LLM-as-a-Judge Auto-Review

The system uses **LLM as a Judge** at multiple stages:

| Stage | What LLM Judges | Action |
|---|---|---|
| **Pre-build validation** | Is the SQL syntactically correct and returning meaningful results? | Auto-filter bad candidates |
| **Auto-Generate SQL** (🤖) | Generate API name, description, inputSchema, SQL from query | Fill form fields |
| **Auto-Review** (🧠) | Given current SQL, is the API metadata correct? | Auto-correct metadata |
| **Schema feedback** | When human edits SQL/Query: classify reason → `column_ambiguity` / `sql_logic_error` / `value_mismatch` | Trigger schema updates |
| **Cascade judgment** | For each affected API: should description/SQL/query be updated? | Three-dimension update |
| **Runtime verification** | Does the SQL result semantically match the original query? | Approve or escalate |

This replaces rigid heuristics with flexible, natural-language classification.

### 4.3 Schema Feedback & Auto-Update (Diff-Driven)

When you edit SQL and/or Query in **any** tab (Dataset, Validation, Review Queue), the system automatically:

```
Compare old_sql vs new_sql + old_query vs new_query
                    ↓
LLM classifies modification reason (all changes analyzed together):
                    ↓
┌──────────────────────────────────────────────────┐
│ column_ambiguity                                 │
│ → Field description was unclear, wrong column    │
│   selected. Query rewrite to disambiguate also   │
│   counts (e.g. "name" → "formal name")          │
│ → Suggest Schema field description update        │
│                                                  │
│ sql_logic_error                                  │
│ → WHERE/JOIN/aggregation logic wrong, but        │
│   correct columns used. Query rewrite for logic  │
│   correction also counts (e.g. "latest" →       │
│   "latest 3")                                    │
│ → Log only, no Schema change                     │
│                                                  │
│ value_mismatch                                   │
│ → Enum value / data format misunderstanding      │
│ → Log only, no Schema change                     │
└──────────────────────────────────────────────────┘
                    ↓ (column_ambiguity only)
Generate field description update suggestions (with confidence)
                    ↓
Display in UI: suggestion table with old/new descriptions
                    ↓
User choice:
  ├── ✅ Apply Suggestions → Update Schema + trigger cascade
  └── ↩️ Dismiss → Ignore
```

**Key properties:**
- **Never auto-applies** — user must click "Apply"
- Confidence < 0.5 suggestions are filtered out
- Both SQL and Query changes are analyzed together for better classification

### 4.4 Cascade Updates

After applying schema suggestions, the system automatically:

```
Schema field description updated
        ↓
Scan valid.jsonl for APIs referencing the updated field
        ↓
Skip records where user_edited = true
        ↓
For each remaining API, LLM evaluates 3 dimensions:
  ├── description: Is the API description outdated?
  ├── bound_sql: Should a different column be used?
  └── query: Does the query wording need adjustment?
        ↓
Batch update + binlog
```

**Example:**
- You update `name_formal` description to "Employee legal name (distinct from display nickname `name_display`)"
- System finds 5 APIs using `name_formal` in their SQL
- 2 of them are `user_edited` → skipped
- For the remaining 3, LLM determines:
  - API #1: description needs update (was "query employee name" → "query employee legal name")
  - API #2: SQL should use `name_display` instead → SQL updated
  - API #3: no changes needed

### 4.5 User-Edit Protection (`user_edited`)

When you manually edit and save a record in the Dataset tab, it gets marked `user_edited=true`. Protected records:
- Are **never modified** by cascade updates
- Are **never modified** by schema feedback auto-apply
- Can **only** be changed by the user editing them again manually

This ensures human review work is never silently overwritten by automation.

### 4.6 Version Management & Binlog

Every write operation is automatically logged:

| Dataset | Tracked operations |
|---|---|
| `valid.jsonl` | Approve, edit, delete, import, cascade update |
| `invalid.jsonl` | Pre-build generation |
| `schema.json` | Field description update, auto-fix |
| `recorrect.jsonl` | Rejection |
| `review_queue.jsonl` | Task status changes |

Each binlog entry contains: `ts`, `op` (insert/update/delete), `record`, `old_record`, `meta`.

Point-in-time restore is available in the Version History tab.

### 4.7 Concurrency Protection

All side-effect buttons use `threading.Lock`:

| Lock | Protects |
|---|---|
| `_lock_prebuild` | Pre-build generation |
| `_lock_autofix` | Global/field auto-fix |
| `_lock_invalid_action` | Validation: approve/skip/reject |
| `_lock_task_action` | Review queue: approve/reject/next |
| `_lock_valid_write` | Dataset: save/delete |

---

## 5. Data Files

| File | Description |
|---|---|
| `output/<table>/valid.jsonl` | Approved dataset (core asset) |
| `output/<table>/invalid.jsonl` | Pre-build candidates (pending review) |
| `output/<table>/schema.json` | Table Schema (foundation for LLM understanding) |
| `output/<table>/review_queue.jsonl` | Runtime correction tasks |
| `output/<table>/recorrect.jsonl` | Rejected records |
| `output/<table>/binlog_*.jsonl` | Version management logs |

**valid.jsonl record format:**
```json
{
  "query": "Query John's hire date",
  "api_schema": {
    "name": "query_hire_date",
    "description": "Query hire date for a specific employee",
    "inputSchema": {"type": "object", "properties": {"name_formal": {"type": "string"}}},
    "bound_sql": "SELECT hire_date FROM base_staff WHERE name_formal = :name_formal",
    "table": "base_staff"
  },
  "reviewed_at": "2026-03-18T10:30:00",
  "reviewer": "admin",
  "user_edited": true,
  "source": "manual_review"
}
```

---

## 6. Configuration

```yaml
review:
  port: 7860
  language: en           # UI language: "en" or "zh" (default: en)
  auth_users: []         # Whitelist usernames; empty = no auth
  valid_path: ./output/base_staff/valid.jsonl
  review_queue: ./output/base_staff/review_queue.jsonl
```

---

## 7. FAQ

### Q: Pre-build data quality is poor?
**A:** Run "Global Auto-Fix" first to improve Schema field descriptions. Better descriptions → better SQL generation.

### Q: Why does the Review Queue SQL have `:slot` placeholders?
**A:** That's the **template SQL** (`bound_sql`). Check the "🔍 Invoked SQL" field below for the actual executed SQL. Templates are stored on approval.

### Q: Should I re-run Pre-build after editing Schema descriptions?
**A:** **No.** Full Pre-build is blocked once data exists (especially user-edited records). Instead: edit Schema → modify SQL in any tab → system auto-triggers cascade updates → click "Apply Suggestions". This preserves all your manual review work.

### Q: Does cascade update modify SQL?
**A:** **Yes.** It evaluates three dimensions independently: `description`, `bound_sql`, and `query`. But records marked `user_edited=true` are never modified — only you can edit those again.

### Q: What is `user_edited`?
**A:** When you manually edit and save a record in the Dataset tab, it's marked `user_edited=true`. These records are protected from all automation (cascade updates, schema feedback). Only manual re-editing can change them.

### Q: Schema feedback shows low confidence. Should I apply?
**A:** Suggestions with confidence < 50% are auto-filtered. For those shown, confidence > 70% is usually safe. "Dismiss" has zero side effects. All changes are binlogged and can be rolled back.
