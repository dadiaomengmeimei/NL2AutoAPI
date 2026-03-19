# NL2AutoAPI

NL2AutoAPI is an engineering system for structured-data Q&A: it distills table schemas, statistics, and historical samples into reusable **API assets**, then routes natural-language queries to stable SQL execution pipelines.

**Target scenarios:**
- Single-table statistical Q&A
- Lightweight multi-table systems where single-table capability dominates
- Teams that need LLM-generated results to be **auditable, replayable, and iteratively improvable**

---

## Core Value

Unlike letting an LLM generate SQL on-the-fly for every request, NL2AutoAPI emphasizes **asset-ification and closed-loop iteration**:

- **Build APIs first, serve online second** — not regenerating SQL from scratch each time.
- **Auditable** — generated results enter `valid.jsonl` / `invalid.jsonl` / `review_queue.jsonl`.
- **Iterable** — online failure cases flow back and expand API coverage.
- **Deployable** — export to JSON / OpenAPI / MCP downstream formats.
- **Clear capability boundary** — explicitly defines what the system can and cannot do.

---

## 🔑 Key Design Decisions

### 1. Schema Construction: Agent-Style Exploration Loop (Auto-Fix)

Database comments are often sparse or misleading (e.g., three fields all labeled "name"). Yet field descriptions are the foundation of the entire system. Traditional approaches ask the LLM to guess meanings from comments — one shot, fixed forever.

NL2AutoAPI takes a fundamentally different approach — **simulating an Agent interacting with a real environment (the database), learning through trial-and-error** to understand what each field truly means:

```
┌─────────────────────────────────────────────────────────┐
│  For each exploration round:                            │
│                                                         │
│  1. Generate NL queries (biased towards high-usage      │
│     fields that need the most refinement)               │
│           ↓                                             │
│  2. For each query: LLM generates SQL                   │
│           ↓                                             │
│  3. Execute SQL against real database                   │
│           ↓                                             │
│  4. LLM validates: does the result match the intent?    │
│     ┌─── CORRECT → field descriptions are good ✅       │
│     └─── INCORRECT/PARTIAL                              │
│           ↓                                             │
│  5. Extract columns from the failed SQL                 │
│           ↓                                             │
│  6. For each involved field, LLM refines description    │
│     using:                                              │
│     • Random sampled values from DB                     │
│     • Neighboring field context                         │
│     • Error context from the failed attempt             │
│           ↓                                             │
│  7. Updated descriptions feed into next round           │
└─────────────────────────────────────────────────────────┘
```

**This follows the same paradigm as RL Agent-Environment interaction**: Observe → Act → Get Feedback → Update Policy → Repeat. Here the "environment" is the real database, the "policy" is field descriptions, and the "feedback" is SQL execution results.

Instead of asking the LLM "what does this field mean?", the system:
- **Tries using the field** in a real query
- **Observes the result** from the real database
- **Diagnoses why it failed** (wrong column? ambiguous description?)
- **Refines with concrete evidence** — sampled values, error context, neighboring field comparison
- **Repeats** until descriptions are accurate enough

**Example:** A field named `name` with comment "姓名" (name). After the exploration loop:
1. LLM generates query "find Zhang San's info" → SQL uses `WHERE name = 'Zhang San'` → executes → returns nothing
2. System samples `name` column → discovers employee IDs like "E001"
3. Samples `name_formal` → discovers real names like "Zhang San"
4. LLM refines: `name` → "Employee ID code (not the person's actual name; see `name_formal` for legal name)"
5. Next round: queries using `name_formal` succeed ✅

Auto-Fix is triggered from the Schema tab in the Review UI (Global Auto-Fix / Single Field Auto-Fix). It is **independent of Pre-build** and can be re-run at any time without affecting existing data.

### 2. Two-Layer Pre-build Architecture (Layer A → Layer B)

With high-quality Schema in hand, the pre-build stage generates API assets in **two layers with fundamentally different generation strategies**:

```
┌──────────────────────────────────────────────────────────────┐
│ Layer A — Data-Distribution-Driven Structured Generation     │
│           (backbone, ~80% of output)                         │
│                                                              │
│  1. Data Profiling: sample each column, compute cardinality, │
│     uniqueness ratio, and sample values                      │
│  2. Auto-classify based on distribution features:            │
│     • Low cardinality (2~12) → GROUP BY aggregation          │
│     • Mid cardinality + non-numeric → filter stats (COUNT    │
│       WHERE)                                                 │
│     • High uniqueness (>0.8) + identity field → exact lookup │
│       (WHERE =)                                              │
│     • Numeric + high cardinality → numeric stats             │
│       (AVG/MAX/MIN)                                          │
│     • Date/time type → range query (BETWEEN)                 │
│     • Multi-dimension combos → cross-filter aggregation      │
│  3. SQL assembled from rule templates (deterministic, no     │
│     hallucination)                                           │
│  4. LLM only polishes: turns structured descriptions into    │
│     natural language queries                                 │
│  5. Fill slots with sampled values → validate on real DB     │
│  Output: valid.jsonl (layer_tag: "Layer-A")                  │
└────────────────────────────┬─────────────────────────────────┘
                             ↓
┌──────────────────────────────────────────────────────────────┐
│ Layer B — Value-Driven LLM Free Generation                   │
│           (gap-filling, ~20% of output)                      │
│                                                              │
│  1. Randomly select a query type by weight                   │
│  2. Randomly sample 2~4 field subsets                        │
│  3. LLM freely generates SQL (no rule templates)             │
│  4. LLM converts SQL back to natural language query          │
│  5. Sample real values from DB for slot filling               │
│  6. Execution validation + intent verification + round-trip  │
│     check                                                    │
│  Output: valid.jsonl (layer_tag: "Layer-B")                  │
└──────────────────────────────────────────────────────────────┘
```

**Why two layers?**
- Layer A is **"let the data distribution tell me what to generate"** — profile data features first, then decide query types; highly deterministic
- Layer B is **"pick a type first, validate with real values"** — LLM generates freely, filling gaps that Layer A cannot cover
- **Schema changes don't require a full rebuild** — the system uses **diff-driven cascade updates** to propagate changes

### 3. LLM-as-a-Judge Auto-Review

The system employs **LLM as a Judge** at multiple stages:

| Stage | What the LLM judges | Impact |
|---|---|---|
| **Pre-build validation** | Is the generated SQL syntactically correct? Does it return meaningful results? | Auto-filter bad candidates |
| **Auto-Review (per record)** | Given the query and SQL, is the API name/description/inputSchema correct? | Auto-fill or correct API metadata |
| **Schema feedback analysis** | When a human edits SQL or Query, **classify the modification reason** into `column_ambiguity`, `sql_logic_error`, or `value_mismatch` | Trigger targeted schema updates |
| **Cascade update judgment** | For each affected API: should the description/SQL/query be updated? | Three-dimensional update per API |
| **Runtime verification** | After executing the SQL, does the result semantically match the original query? | Auto-approve or escalate to review queue |

The "LLM-as-a-Judge" pattern replaces manual heuristics with a flexible, natural-language classifier that adapts to new patterns without code changes.

### 4. Diff-Driven Auto & Cascade Modifications

Every human edit triggers an **automatic diff analysis**:

```
Human edits SQL and/or Query
         ↓
Compare old_sql vs new_sql + old_query vs new_query
         ↓
LLM classifies modification reason (3 categories)
         ↓
If "column_ambiguity" detected:
  ├── Suggest field description update (with confidence score)
  ├── User clicks "Apply" → Schema updated
  └── Cascade: scan all APIs referencing the updated field
         ↓
For each affected API (skipping user_edited records):
  LLM evaluates 3 dimensions independently:
  ├── Description: is it outdated?
  ├── SQL: is the wrong column used?
  └── Query: does the wording need adjustment?
         ↓
Batch update + binlog (all changes are versioned)
```

**Key properties:**
- **Never overwrites user edits** — records marked `user_edited=true` are protected
- **Three-dimensional judgment** — description, SQL, and query are evaluated independently
- **Confidence filtering** — only suggestions with confidence ≥ 0.5 are shown
- **Full auditability** — every change is logged in binlog with before/after snapshots
- **No full rebuild needed** — unlike re-running Pre-build, cascade updates are surgical and targeted

---

## System Architecture

### Phase 1: Pre-build

```bash
python main.py build --config ./config.yaml
```

- Generate API assets from schema + field stats + sample data
- **Layer A**: Data-distribution profiling (cardinality, uniqueness) → rule-template SQL + LLM-polished queries
- **Layer B**: LLM freely generates SQL + queries → real-value slot filling from DB → gap coverage
- Output: `valid.jsonl`, `invalid.jsonl`
- Core modules: `pre_build.py`, `generation/`, `schema/`, `validation/`

### Phase 2: Runtime

```bash
python main.py serve --config ./config.yaml --mode interactive
```

- Route user queries to best-matching API via **TopK table recall + API shard recall + LLM select**
- Slot-fill → Execute SQL → Verify result
- Failed requests auto-generate correction tasks
- Core modules: `runtime/router.py`, `runtime/registry.py`, `runtime/recall.py`, `runtime/slot_filling.py`

### Phase 3: Review

```bash
python main.py review --config ./config.yaml
```

- 7-tab Gradio interface for human review
- Auto-review, schema feedback, cascade updates
- Version management with point-in-time restore
- Core modules: `review/interface.py`, `review/i18n.py`, `core/schema_feedback.py`

---

## Project Structure

```
nl2autoapi/
├── core/          # Config, logging, DB, LLM, utilities
├── schema/        # Schema reading and data models
├── generation/    # Pre-build query/SQL/API generation
├── validation/    # Build-time and runtime validation
├── runtime/       # Routing, recall, online testing
├── review/        # Human review UI, i18n, task submission
│   ├── interface.py   # Main Gradio interface (7 tabs)
│   └── i18n.py        # Internationalization (en/zh)
├── feedback/      # History-based expansion
├── tools/         # Export and utility scripts
├── main.py        # Unified CLI entry point
└── config.yaml    # Configuration
```

---

## Quick Start

### 1. Set environment variables

```bash
export DB_HOST=127.0.0.1  DB_PORT=3306  DB_USER=root  DB_PASSWORD=xxx  DB_NAME=demo_db
export LLM_MODEL=gpt-4o-mini  LLM_API_KEY=xxx  LLM_BASE_URL=https://api.openai.com/v1
```

### 2. Configure

Edit `config.yaml` — key fields: `schema.path`, `build.output_dir`, `review.language` (`en` or `zh`).

### 3. Pre-build

```bash
python main.py build --config ./config.yaml
```

### 4. Launch review UI

```bash
python main.py review --config ./config.yaml --port 7860
```

### 5. Runtime serve

```bash
python main.py serve --config ./config.yaml --mode interactive
```

---

## Configuration

All configuration is centralized in `config.yaml`:

```yaml
review:
  port: 7860
  language: en          # UI language: en | zh
  auth_users: []        # Whitelist; empty = no auth
  valid_path: ./output/base_staff/valid.jsonl
  review_queue: ./output/base_staff/review_queue.jsonl
```

Priority: **CLI args > Environment variables > config.yaml > Defaults**

---

## Requirements

- Python 3.10+
- MySQL / MariaDB compatible database
- OpenAI-compatible chat API
- Key dependencies: `PyYAML`, `pymysql`, `pydantic`, `gradio`, `requests`
