# NL2AutoAPI

NL2AutoAPI 是一个面向结构化数据问答的工程化系统：它把表结构、统计信息和历史样本沉淀成可复用的 API 资产，再把自然语言查询路由到稳定的 SQL 执行链路。

适用场景：
- 单表统计问答
- 轻量多表、但以单表能力为主的查询系统
- 需要把 LLM 能力变成“可审核、可回放、可迭代”资产的团队

---

## 核心价值

相比直接让模型临时生成 SQL，NL2AutoAPI 更强调资产化和闭环：

- **先沉淀 API，再服务线上**：不是每次都从零生成 SQL。
- **可审核**：生成结果会进入 `valid.jsonl` / `invalid.jsonl` / `review_queue.jsonl`。
- **可迭代**：线上失败 case 可以回流，继续扩充 API 覆盖。
- **可部署**：可导出 JSON / OpenAPI / MCP 等下游格式。
- **能力边界清晰**：明确系统支持什么、不支持什么，而不是让用户面对黑盒失败。

---

## 主要功能

### 1. 预热构建
- 从 schema 和样本统计出发，生成基础 API 集
- **Layer A**：基于数据分布探查（基数、唯一率），规则模板拼装 SQL + LLM 润色 query
- **Layer B**：LLM 自由生成 SQL + query，从数据库采样真实值填槽验证
- 对结果进行校验与过滤
- 导出按表组织的 API 资产

### 2. Runtime 路由
- **TopK 表级召回** → **分片 API 召回** → **LLM 精选最佳 API**
- 自动填槽并执行 SQL
- 用 LLM 或规则校验结果是否符合意图
- 对失败请求自动生成纠错任务

### 3. Online Test
- 根据表描述自动生成线上风格 query
- 跑完整 runtime 链路做回归或压测
- 自动把通过/不通过样本重新沉淀

### 4. Human Review
- 审核 invalid case 和 runtime correction task
- 修正 API / SQL / 描述后回流到有效集

### 5. Feedback Expansion
- 基于线上失败或边界 case 做扩写
- 生成新的 API 候选，扩充覆盖范围

---

## 🔑 核心设计决策

### 1. Schema 构建：Agent 式探索循环（Auto-Fix）

数据库注释通常简陋甚至错误（比如三个字段都叫"姓名"），但字段描述是整个系统的基础。传统方案是让 LLM "看注释猜含义"，一次生成就定死。

NL2AutoAPI 采用了完全不同的思路 —— **模拟一个 Agent 在真实环境（数据库）中不断尝试并反馈**，通过试错学习来理解每个字段的真正含义：

```
┌─────────────────────────────────────────────────────────┐
│  每一轮探索：                                             │
│                                                         │
│  1. 生成自然语言查询（偏向高频字段，                       │
│     优先修正最需要改进的描述）                              │
│           ↓                                             │
│  2. LLM 根据当前 Schema 生成 SQL                         │
│           ↓                                             │
│  3. 在真实数据库上执行 SQL                                │
│           ↓                                             │
│  4. LLM 验证：执行结果是否匹配查询意图？                    │
│     ┌─── CORRECT → 字段描述准确 ✅                        │
│     └─── INCORRECT / PARTIAL                            │
│           ↓                                             │
│  5. 从失败 SQL 中提取涉及的列名                            │
│           ↓                                             │
│  6. 对每个涉及字段，LLM 结合以下信息修正描述：              │
│     • 数据库中的随机采样值                                 │
│     • 邻居字段的上下文                                    │
│     • 失败尝试的错误信息                                   │
│           ↓                                             │
│  7. 更新后的描述进入下一轮探索                              │
└─────────────────────────────────────────────────────────┘
```

**这和 RL 中 Agent 与环境交互的范式一致**：观察 → 行动 → 获得反馈 → 更新策略 → 重复。只不过这里的"环境"是真实数据库，"策略"是字段描述，"反馈"是 SQL 执行结果。

系统不是问 LLM "这个字段什么意思"，而是：
- **用这个字段** 构造真实查询
- **观察结果** —— 真实数据库返回了什么
- **诊断失败原因** —— 用错了列？描述不准？
- **用具体证据修正** —— 采样值、错误上下文、邻居字段对比
- **循环重复** —— 直到描述足够准确

**举例**：字段 `name`，注释写的是"姓名"。经过探索循环：
1. LLM 生成查询"查张三的信息" → SQL `WHERE name = '张三'` → 执行 → 返回空
2. 系统采样 `name` 列 → 发现是 "E001" 这样的工号
3. 采样 `name_formal` → 发现是 "张三" 这样的真名
4. LLM 修正：`name` → "员工工号编码（非真实姓名，真实姓名见 name_formal）"
5. 下一轮查询使用 `name_formal` → 成功 ✅

Auto-Fix 在 Review 界面的 Schema 页签中触发（全局修正 / 单字段修正），它独立于 Pre-build，可以随时重跑而不影响已有数据。

### 2. 双层预热架构（Layer A → Layer B）

有了高质量 Schema 之后，预热阶段分两层生成 API 资产。**两层的本质区别在于生成策略不同**：

```
┌──────────────────────────────────────────────────────────┐
│ Layer A — 数据分布驱动的结构化生成（主干，约占 80%）         │
│                                                          │
│  1. 数据探查：对每个字段采样，计算基数、唯一率、采样值       │
│  2. 基于分布特征自动分类：                                  │
│     • 低基数 (2~12) → 分组统计 (GROUP BY)                  │
│     • 中基数 + 非数值 → 筛选统计 (COUNT WHERE)              │
│     • 高唯一率 (>0.8) + 身份字段 → 精确查询 (WHERE =)       │
│     • 数值型 + 高基数 → 数值统计 (AVG/MAX/MIN)              │
│     • 日期/时间型 → 范围查询 (BETWEEN)                      │
│     • 多维组合 → 多条件过滤 + 交叉统计                      │
│  3. SQL 基于规则模板直接拼装（确定性强，不会出错）           │
│  4. LLM 只做润色：将结构化描述转为自然语言 query              │
│  5. 采样值填槽 → 真实数据库执行验证                         │
│  产出：valid.jsonl (layer_tag: "Layer-A")                  │
└──────────────────────┬───────────────────────────────────┘
                       ↓
┌──────────────────────────────────────────────────────────┐
│ Layer B — 值驱动的 LLM 自由生成（补洞，约占 20%）           │
│                                                          │
│  1. 按查询类型权重随机选类型                               │
│  2. 随机采样 2~4 个字段子集                                │
│  3. LLM 自由生成 SQL（不再是规则模板）                      │
│  4. LLM 将 SQL 反转为自然语言 query                        │
│  5. 从数据库采样真实值填槽验证                              │
│  6. 执行验证 + 意图验证 + round-trip 检查                   │
│  产出：valid.jsonl (layer_tag: "Layer-B")                  │
└──────────────────────────────────────────────────────────┘
```

**为什么分两层？**
- Layer A 是 **"数据分布告诉我该生成什么"** —— 先探查数据特征，再决定查询类型，生成确定性强
- Layer B 是 **"先定类型，用真实值去验证"** —— LLM 自由发挥，补充 Layer A 覆盖不到的查询模式
- **Schema 改了不需要全量重建** —— 系统通过 **diff 驱动的级联更新** 传播变更

### 3. LLM-as-a-Judge 多节点自动审核

系统在 **5 个关键节点** 使用 LLM 作为裁判：

| 节点 | LLM 判断什么 | 影响 |
|---|---|---|
| **预热期校验** | SQL 语法正确吗？返回结果有意义吗？ | 自动过滤坏候选 |
| **自动复审** | 给定 Query+SQL，API 名称/描述/参数对吗？ | 自动填充或修正元数据 |
| **Schema 反馈分析** | 人修改了 SQL/Query，原因是「字段歧义」还是「逻辑错误」还是「值不匹配」？ | 触发定向 Schema 更新 |
| **级联更新判断** | 每个受影响 API 的描述/SQL/Query 需要更新吗？ | 三维度独立判断 |
| **Runtime 验证** | SQL 执行结果和原始查询语义匹配吗？ | 自动通过或提交人工审核 |

### 4. Diff 驱动的自动级联修正

每次人工编辑都会触发自动 diff 分析：

```
人修改了 SQL 和/或 Query
         ↓
比较 old_sql vs new_sql + old_query vs new_query
         ↓
LLM 分类修改原因（3 类）
         ↓
如果是 "column_ambiguity"（字段歧义）：
  ├── 建议更新字段描述（附置信度评分）
  ├── 用户点击"应用建议" → Schema 更新
  └── 级联：扫描所有引用该字段的 API
         ↓
对每个受影响 API（跳过 user_edited 记录）：
  LLM 独立评估 3 个维度：
  ├── 描述：是否过时？
  ├── SQL：是否用错了列？
  └── Query：措辞是否需要调整？
         ↓
批量更新 + binlog（所有变更可追溯）
```

**关键特性：**
- 🛡️ **永远不覆盖人工编辑** —— `user_edited=true` 的记录受保护
- 📐 **三维度独立判断** —— 描述、SQL、Query 分别评估
- 📊 **置信度过滤** —— 只展示置信度 ≥ 0.5 的建议
- 📝 **完全可审计** —— 每次变更都有 binlog，支持时间点恢复
- 🔄 **不需要全量重建** —— 级联更新是精准的外科手术

---

## 优点

- **比纯 Text-to-SQL 更稳**：优先复用已沉淀 API，而不是每次自由生成。
- **比纯规则系统更灵活**：预热和 runtime 都可以借助 LLM 处理复杂表达。
- **比一次性离线脚本更工程化**：有构建、运行、测试、审核、反馈闭环。
- **对开源友好**：入口统一，配置集中，敏感信息可通过环境变量管理。

---

## 能力边界

这个项目当前更适合以下边界内的问题：

### 当前擅长
- 单表精确查询
- 单表聚合统计
- 带有限过滤条件的分析查询
- 已知业务域内、字段语义相对明确的问答

### 当前不擅长
- 重度跨表 Join 场景
- 高度开放式分析问题
- 强依赖复杂业务口径推理的问题
- 实时事务型写操作
- 通用 BI / 通用 Agent 全场景替代

### 推荐理解方式

NL2AutoAPI 不是“万能数据库 Agent”，而是“把高频查询沉淀为 API 资产的结构化问答系统”。

---

## 系统架构

项目可以按 3 个主阶段理解。

### 1. 预热阶段（Pre-build）

入口：`python main.py build --config ./config.yaml`

作用：
- 根据 schema、字段信息和统计样本，提前生成 API 资产
- 产出 `valid.jsonl`、`invalid.jsonl`
- 导出结构化 schema 结果

核心模块：
- `pre_build.py`
- `generation/`
- `schema/`
- `validation/`
- `tools/export_schemas.py`

### 2. Runtime 阶段

入口：`python main.py serve --config ./config.yaml --mode interactive`

作用：
- 用已有 API 资产处理真实 query
- 做召回、填槽、执行、验证和纠错提交

核心模块：
- `runtime_server.py`
- `runtime/router.py`
- `runtime/registry.py`
- `runtime/recall.py`
- `runtime/slot_filling.py`
- `runtime/online_verify.py`

### 3. Online-test 阶段

入口：`python main.py serve --config ./config.yaml --mode online`

作用：
- 自动模拟线上 query
- 跑完整 runtime 链路
- 沉淀新的 runtime valid / invalid 数据

核心模块：
- `runtime/online_runtime.py`
- `runtime/online_verify.py`
- `validation/intent_verify.py`

支撑闭环：
- `review/`：人工审核
- `feedback/`：线上 case 扩写

---

## 项目结构

```text
nl2autoapi/
├── core/          # 配置、日志、数据库、LLM、通用工具
├── schema/        # schema 读取与数据模型
├── generation/    # 预热阶段 query / sql / api 生成
├── validation/    # 生成期与运行期验证
├── runtime/       # 路由、召回、在线测试
├── review/        # 人工审核界面与任务提交
├── feedback/      # 基于历史 case 的扩写与扩展
├── tools/         # 导出和辅助脚本
├── tests/         # 测试
├── main.py        # 统一 CLI 入口
└── config.yaml    # 配置入口
```

---

## 快速开始

### 1. 配置环境变量

```bash
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=root
export DB_PASSWORD=your_password
export DB_NAME=demo_db

export LLM_MODEL=gpt-4o-mini
export LLM_API_KEY=your_key
export LLM_BASE_URL=https://api.openai.com/v1
```

### 2. 修改配置

编辑 `config.yaml`，至少确认：
- `schema.path`
- `build.output_dir`
- `runtime.valid_path`

### 3. 运行预热构建

```bash
python main.py build --config ./config.yaml
```

### 4. 启动 runtime

```bash
python main.py serve --config ./config.yaml --mode interactive
```

### 5. 跑 online-test

```bash
python main.py serve --config ./config.yaml --mode online
```

### 6. 启动审核界面

```bash
python main.py review --config ./config.yaml
```

---

## 配置说明

统一使用 `config.yaml` 管理：
- 数据库连接
- LLM 模型与 endpoint
- 输出目录
- runtime 输入输出路径
- review 端口与文件路径
- 日志目录

优先级：

```text
命令行参数 > 环境变量 > config.yaml > 默认值
```

---

## 运行要求

- Python 3.10+
- MySQL / MariaDB 兼容数据库
- OpenAI 兼容聊天接口

常见依赖：
- `PyYAML`
- `requests`
- `pymysql`
- `pydantic`
- `gradio`

---

## 开源整理说明

当前版本已做以下收敛：

- 统一入口为 `main.py`
- 统一配置为 `config.yaml`
- 统一 LLM 管理为 `core/llm.py`
- 删除重复文档与过时脚本
- 删除硬编码账号、绝对路径与示例日志

---

## Roadmap

- [ ] 增加标准依赖文件（`requirements.txt` 或 `pyproject.toml`）
- [ ] 增加面向 README 的 smoke test
- [x] 联表 TopK 表格 API 召回（registry 候选表排序 + recall 分片召回 + LLM 精选）
- [ ] 强化多表 Join 能力
- [ ] 把 feedback 阶段进一步产品化
- [ ] 增加更标准的示例数据与 benchmark

---

## 开源前检查清单

- [ ] 替换 `config.yaml` 中的真实 schema 路径
- [ ] 使用环境变量注入数据库和 LLM 凭据
- [ ] 清理 `output/` 中不适合公开的数据
- [ ] 运行 `python main.py build --config ./config.yaml`
- [ ] 运行 `python main.py serve --config ./config.yaml --mode test`
- [ ] 确认 `review` 页面可正常启动
