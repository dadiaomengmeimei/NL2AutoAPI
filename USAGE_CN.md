# NL2AutoAPI Review 平台使用文档

## 目录
- [1. 概述](#1-概述)
- [2. 快速启动](#2-快速启动)
- [3. 各页签功能说明](#3-各页签功能说明)
  - [3.1 Schema 浏览 & 自动修正](#31-schema-浏览--自动修正)
  - [3.2 数据集](#32-数据集)
  - [3.3 数据校验](#33-数据校验)
  - [3.4 Runtime 查询](#34-runtime-查询)
  - [3.5 审核队列](#35-审核队列)
  - [3.6 版本管理](#36-版本管理)
- [4. 核心机制](#4-核心机制)
  - [4.1 自动审核（Auto-Review）](#41-自动审核auto-review)
  - [4.2 Schema 反馈 & 自动更新](#42-schema-反馈--自动更新)
  - [4.3 级联更新](#43-级联更新)
  - [4.4 版本管理 & Binlog](#44-版本管理--binlog)
  - [4.5 防重复点击](#45-防重复点击)
- [5. 数据文件说明](#5-数据文件说明)
- [6. 常见问题](#6-常见问题)

---

## 1. 概述

NL2AutoAPI Review 是一个集数据生产、人工审核、自动修正、Runtime 验证于一体的数据集管理平台。核心目标是将自然语言查询（query）对应到精准的 API Schema + SQL，并通过多轮人机协作不断提升数据质量。

**核心数据流**：

```
用户查询（Query）
    ↓
Pre-build 自动生成候选 API + SQL
    ↓
人工审核（通过 / 修改 / 拒绝）
    ↓
valid.jsonl（已通过的数据集）
    ↓
Runtime 在线验证（自动路由 + 填槽 + 执行 + 校验）
    ↓
Schema 反馈（自动分析 SQL 修改，建议更新字段描述）
```

## 2. 快速启动

```bash
cd nl2autoapi
python main.py review --port 7860
```

打开浏览器访问 `http://localhost:7860` 即可使用。

**启动前准备**：
- 确保 `config.yaml` 已正确配置数据库连接和 LLM 接口
- 确保 Schema JSON 文件已生成（如 `output/base_staff/schema.json`）

---

## 3. 各页签功能说明

### 3.1 Schema 浏览 & 自动修正

> **用途**：管理数据库表的 Schema 定义，自动修正字段描述

| 功能 | 说明 |
|---|---|
| **从数据库生成 Schema** | 连接数据库，自动生成表的 Schema JSON（含字段名、类型、注释） |
| **Schema 编辑器** | 直接编辑 Schema JSON，手动修改字段描述 |
| **字段管理 & 自动修正** | 选择某个字段，LLM 自动改进该字段的描述（如添加枚举值、区分相似字段） |
| **全局自动修正** | 一键让 LLM 审视所有字段描述，批量改进 |
| **Pre-build 数据生成** | 基于 Schema 自动生成候选 API 和 SQL |

**典型流程**：
1. 点击「从数据库生成 Schema」
2. 点击「全局自动修正」让 LLM 改进字段描述
3. 人工审核修正结果
4. 点击「Pre-build 数据生成」生成候选数据

### 3.2 数据集

> **用途**：浏览和编辑已通过审核的 valid 数据集

| 功能 | 说明 |
|---|---|
| **浏览记录** | 按索引查看 valid.jsonl 中的每条记录 |
| **编辑 API** | 修改 query、API 名称、描述、SQL 等字段 |
| **删除记录** | 从数据集中移除某条记录 |
| **Schema 反馈** | 保存修改时，自动分析 SQL 和 Query 变更并提供字段描述更新建议 |

**重要交互**：修改 SQL 或 Query 后点击「保存修改」，系统会自动分析修改原因（详见 [4.2 Schema 反馈](#42-schema-反馈--自动更新)）。

### 3.3 数据校验

> **用途**：逐条审核 Pre-build 生成的候选数据

| 功能 | 说明 |
|---|---|
| **✨ 具体化 Query** | 将抽象查询（如「查询某员工信息」）替换为具体实例（如「查询张三的信息」） |
| **🤖 自动生成 SQL** | 根据 query 让 LLM 自动生成 API 名称、描述、SQL |
| **🧠 自动复审** | 基于当前 SQL 让 LLM 自动填充 API 名称、描述、输入参数 |
| **✅ 通过** | 审核通过，写入 valid.jsonl |
| **⏭️ 跳过** | 暂时跳过，不做处理 |
| **❌ 拒绝** | 拒绝入库，写入 recorrect.jsonl |
| **Schema 反馈** | 审核通过时，自动分析 SQL 和 Query 变更并提供字段描述更新建议 |

**典型流程**：
1. 系统显示一条候选记录的 query 和原始 SQL
2. 如果 query 太抽象 → 点击「具体化 Query」
3. 如果 SQL 需要修改 → 手动编辑或点击「自动生成 SQL」
4. 确认无误 → 点击「通过」
5. 如果弹出 Schema 反馈通知 → 审核建议后点击「应用建议」或「撤销」

### 3.4 Runtime 查询

> **用途**：实时测试 query 的端到端执行效果

| 功能 | 说明 |
|---|---|
| **运行查询** | 输入 query，走完整 Runtime 链路（路由 → 填槽 → 执行 → 验证） |
| **手工 SQL 模式** | 手动填写 API Schema + SQL，测试参数化和 round-trip 验证 |
| **扩展测试** | 自动扩展 query 变体（水平扩展 + 垂直扩展），批量测试 |
| **导入 Valid** | 将测试通过的结果直接导入 valid 数据集 |
| **去重** | 对 valid 数据集执行 SQL 去重 |

**Runtime 执行链路**：
```
Query → Recall（候选API检索）→ Select Best（选最佳API）
     → Slot Fill（填槽）→ Execute SQL → Verify（语义校验）
     → 如果失败：RAG Generate（基于已有API上下文生成新API）
     → 如果仍失败：提交审核任务到审核队列
```

### 3.5 审核队列

> **用途**：处理 Runtime 产生的纠错任务

| 功能 | 说明 |
|---|---|
| **查看任务** | 显示任务详情：query、错误 API、候选表、区分指令 |
| **SQL（模板）** | 显示 `bound_sql`（带 `:slot` 占位符的模板 SQL），可编辑 |
| **🔍 填槽后SQL** | 显示 `invoked_sql`（填入真实值后实际执行的 SQL），只读参考 |
| **✨ 具体化 Query** | 将抽象 query 替换为具体实例 |
| **🤖 自动生成 SQL** | 根据 query 生成新的 API + SQL |
| **🧠 自动复审** | 基于当前 SQL 自动审核 |
| **✅ 通过 / ❌ 拒绝 / ⏭️ 下一个** | 审核操作 |
| **Schema 反馈** | 通过时自动分析填槽后 SQL 与新 SQL、以及 Query 的差异 |

**关于填槽后 SQL**：
- 审核队列中的 SQL 是**模板 SQL**（`bound_sql`），带 `:name`, `:city` 等占位符
- 「🔍 填槽后SQL」显示的是 Runtime 执行时**实际使用的 SQL**，已填入真实参数值
- 审核时可以参考填槽后 SQL 来判断原来的 SQL 哪里有问题
- **入库时存储的是模板 SQL**（自动参数化），不是填槽后的

### 3.6 版本管理

> **用途**：查看所有数据修改的 Binlog，支持时间点恢复

| 功能 | 说明 |
|---|---|
| **版本摘要** | 查看各数据集的操作统计 |
| **Binlog 详情** | 查看某个数据集最近 50 条操作记录 |
| **支持的数据集** | valid, invalid, schema, boundary, recorrect, review_queue |

---

## 4. 核心机制

### 4.1 自动审核（Auto-Review）

系统提供多层次的自动审核能力：

**层次 1：自动生成 SQL**
- 点击「🤖 自动生成 SQL」，LLM 根据 query + Schema 自动起草 API
- 自动生成：API 名称、描述、inputSchema、bound_sql

**层次 2：自动复审**
- 点击「🧠 自动复审」，基于当前 SQL 让 LLM 审核并修正
- 适用于已有 SQL 但需要优化 API 名称和描述的场景

**层次 3：Query 具体化**
- 点击「✨ 具体化 Query」，将抽象查询（如「查询指定员工」）自动替换为具体查询
- 从数据库采样真实值填入 query 中

**层次 4：Runtime 自动验证**
- 在 Runtime 查询中，系统自动执行完整链路并验证语义正确性
- 通过的记录自动入库，失败的提交到审核队列

### 4.2 Schema 反馈 & 自动更新

当用户在**数据集编辑**、**数据校验审核通过**、**审核队列通过**时修改了 SQL 和/或 Query，系统会自动执行以下分析：

```
对比 old_sql vs new_sql + old_query vs new_query
    ↓
LLM 分析修改原因（三分类）——SQL 和 Query 变动一并分析
    ↓
┌──────────────────────────────────────────────────┐
│  column_ambiguity（字段歧义）                      │
│  → 字段描述不清晰导致选错字段                        │
│  → 自动建议更新 Schema 字段描述                      │
│                                                    │
│  sql_logic_error（SQL逻辑错误）                     │
│  → WHERE/JOIN/聚合等逻辑错误                         │
│  → 仅记录，不影响 Schema                             │
│                                                    │
│  value_mismatch（值格式不匹配）                      │
│  → 枚举值/数据格式理解错误                            │
│  → 仅记录，不影响 Schema                             │
└──────────────────────────────────────────────────┘
    ↓ （仅 column_ambiguity）
生成字段描述更新建议（含置信度）
    ↓
在 UI 上显示通知：更新建议表格
    ↓
用户选择：
  ├── ✅ 应用建议 → 更新 Schema + 级联更新 API
  └── ↩️ 撤销 → 忽略建议
```

**重要细节**：
- 默认**不会自动更新** Schema，必须用户手动点击「应用建议」
- 置信度 < 0.5 的建议自动过滤
- LLM 判断结果可以为空（`reason_type: none`），不会强行修改
- 更新后的字段描述不会与原描述出现重复内容

### 4.3 级联更新

当用户「应用建议」更新 Schema 字段描述后，系统会自动：

1. **扫描 valid.jsonl**：找出所有引用了被修改字段的 API
2. **过滤保护记录**：跳过所有标记为 `user_edited=true` 的记录（用户手动编辑过的记录受保护，只能由用户自己再次修改）
3. **LLM 判断三个维度**：每个受影响 API 的描述、SQL、查询是否需要同步更新
4. **一并更新**：修改 API 描述（description）、SQL（bound_sql）、查询（query），由 LLM 判断各维度是否需要改
5. **Binlog 记录**：所有更新都记入版本管理

```
Schema 字段描述更新
    ↓
扫描 valid.jsonl 中引用该字段的 API
    ↓
自动跳过 user_edited=true 的记录
    ↓
LLM 判断三个维度：
  ├── description：API 描述是否过时？
  ├── bound_sql：SQL 中的字段是否选错？
  └── query：查询文本措辞是否需调整？
    ↓
批量更新 + binlog
```

**示例**：
- 更新 `name_formal` 的描述为「员工法定姓名（区别于 name_display 显示昵称）」
- 自动检测所有 SQL 中包含 `name_formal` 的 API
- 如果某个 API 的 SQL 应该用 `name_display` 而不是 `name_formal`，LLM 会建议修改 SQL
- 如果 API 描述有「查询员工姓名」这样的模糊描述，LLM 会建议改为「查询员工法定姓名」
- 用户手动编辑过的记录（`user_edited=true`）不受影响

### 4.4 版本管理 & Binlog

所有写操作都自动记录 binlog，支持：
- **valid.jsonl**：通过审核、编辑保存、删除、导入、级联更新
- **invalid.jsonl**：Pre-build 生成
- **schema.json**：字段描述更新、自动修正
- **recorrect.jsonl**：拒绝入库
- **review_queue.jsonl**：任务状态更新
- **boundary.json**：能力边界更新

每条 binlog 包含：
- `ts`：时间戳
- `op`：操作类型（insert / update / delete）
- `record`：操作后的数据
- `old_record`：操作前的数据
- `meta`：额外信息（source、reviewer 等）

### 4.5 防重复点击

所有有副作用的按钮都有 `threading.Lock` 保护：
- 如果操作正在执行中，再次点击会立即返回「⚠️ 操作正在进行中，请稍候」
- 不会出现数据竞争或重复写入

| 锁 | 保护的操作 |
|---|---|
| `_lock_prebuild` | Pre-build 数据生成 |
| `_lock_autofix` | 全局/字段自动修正 |
| `_lock_invalid_action` | 数据校验：通过/跳过/拒绝 |
| `_lock_task_action` | 审核队列：通过/修改/拒绝/下一个 |
| `_lock_valid_write` | 数据集：保存/删除 |

---

## 5. 数据文件说明

| 文件 | 说明 |
|---|---|
| `output/<table>/valid.jsonl` | 已通过审核的数据集（核心文件） |
| `output/<table>/invalid.jsonl` | Pre-build 生成的候选数据（待审核） |
| `output/<table>/schema.json` | 表的 Schema 定义（字段描述是 LLM 理解的基础） |
| `output/<table>/review_queue.jsonl` | Runtime 产生的审核任务 |
| `output/<table>/runtime_valid.jsonl` | Runtime 自动通过的数据 |
| `output/<table>/runtime_invalid.jsonl` | Runtime 失败的数据 |
| `output/<table>/recorrect.jsonl` | 审核拒绝后的纠错记录 |
| `output/<table>/binlog_*.jsonl` | 版本管理日志 |

**valid.jsonl 记录格式**：
```json
{
  "query": "查询张三的入职日期",
  "api_schema": {
    "name": "query_hire_date",
    "description": "查询指定员工的入职日期",
    "inputSchema": {"type": "object", "properties": {"name_formal": {"type": "string"}}},
    "bound_sql": "SELECT hire_date FROM base_staff WHERE name_formal = :name_formal",
    "slot_mapping": {"name_formal": "name_formal"},
    "query_type": "exact_query",
    "table": "base_staff"
  },
  "reviewed_at": "2025-03-18T10:30:00",
  "reviewer": "admin",
  "source": "manual_review"
}
```

---

## 6. 常见问题

### Q: Pre-build 生成的数据质量不好？
**A**: 先运行「全局自动修正」改进 Schema 字段描述，字段描述越精确，生成的 SQL 质量越高。

### Q: 审核队列中的 SQL 看起来有 `:slot` 占位符？
**A**: 这是**模板 SQL**（`bound_sql`），用于存储可复用的查询模式。可以参考下方的「🔍 填槽后SQL」字段查看实际执行时使用的 SQL。入库时存储的是模板 SQL。

### Q: Schema 反馈弹出了建议但我不确定要不要应用？
**A**: 可以先看建议的置信度。置信度 > 70% 通常可以直接应用。点击「撤销」不会有任何影响。所有更新都记了 binlog，即使应用了也可以回滚。

### Q: 修改了 Schema 字段描述后需要重新 Pre-build 吗？
**A**: **不需要，也不应该**。全量 Pre-build 会覆盖之前手动修改过的 SQL 和 Query，导致人工审核的成果丢失。正确做法是：修改 Schema 字段描述后，在任意页签修改 SQL 时系统会自动触发级联更新——LLM 会自动判断相关 API 的描述、SQL、查询是否也需要同步调整，并在 UI 上提供建议供您确认。用户手动编辑过的记录不会被级联更新影响。

### Q: 级联更新会改 SQL 吗？
**A**: **会**。级联更新现在会同时检查 API 的 `description`、`bound_sql` 和 `query` 三个维度，由 LLM 判断每个维度是否需要更新。例如，如果字段描述澄清后发现某个 API 的 SQL 应该用另一个字段，LLM 会建议修改。但**用户手动编辑过的记录**（`user_edited=true`）不会被级联更新修改，只能由用户自己再次编辑。

### Q: 什么是 `user_edited` 标记？
**A**: 当您在「数据集」页签手动编辑并保存某条记录时，该记录会被标记为 `user_edited=true`。被标记的记录在任何自动化流程中（级联更新、Schema 反馈）都不会被修改，确保人工审核的成果不被覆盖。
