# Pre-build 阶段 Query 分层设计（建议稿）

## 1. 目标

在预热（`main.py build`）阶段，把 query 生成从“随机产出”升级为“分层覆盖 + 补洞增强”的可控体系：

- 第一层：基于表的物理统计特征做**结构化分层生成**（主干）
- 第二层：基于字段随机组合做**查漏补缺生成**（补充）

你提出的方向是正确的：
- 第一层才是核心分层逻辑
- 第二层应该明确定位为 coverage 补洞，而不是主生成策略

---

## 2. 现状映射（基于当前代码）

### 2.1 第一层（已存在，需加固）

主要在：
- `generation/rule_based.py`
- `pre_build.py` 中 `run_advanced_rule_pipeline(...)`

已有能力：
- 通过 `profile_table_with_data` 做字段基数 / 唯一率 / 样本值探查
- 能生成一些基于统计特征的规则型 API：
  - 全表计数（`table_count`）
  - 低基数字段分布（`group_distribution`）
  - 数值统计（`numeric_stats`）
  - 高唯一率精确匹配（`exact_query`）
- 可通过 LLM 对 query/description 做语义润色

现有问题：
- 分层标准还偏粗，更多是 if/else 规则，不是层级体系
- 时间语义（同比/环比/时间窗口）没有成为明确层
- 未显式区分“业务主干层”和“补洞层”产物

### 2.2 第二层（已存在，定位需收敛）

主要在：
- `generation/pipeline.py`
- `schema/sampler.py`
- `generation/sql_generator.py`
- `generation/query_generator.py`

已有能力：
- 按 `QUERY_TYPES` 权重随机选类型
- 随机采样字段生成 SQL，再映射自然语言 query
- 经过执行 + 意图验证后沉淀 valid/invalid

现有问题：
- 随机采样与“覆盖缺口”没有闭环绑定
- 权重静态，没根据覆盖率动态调整
- Round-trip 在 `GenerationPipeline` 中是简化版，约束力度偏弱

---

## 3. 建议的分层框架（升级版）

建议把 query 分层从“2段式”升级为“2主层 + 4维标签”。

## 3.1 两个主层

### Layer-A：结构化分层主干（主产能，建议 70%~85%）

按数据物理特征和业务统计模式，先定义层，再生成 query。

建议子层：

- A1 基础聚合层
  - count/sum/avg/max/min
- A2 分组统计层
  - group by 单维 / 多维
- A3 过滤统计层
  - 单条件过滤、区间过滤、多条件叠加
- A4 时间分析层（重点增强）
  - 日/周/月窗口
  - 同比（YoY）
  - 环比（MoM/WoW）
- A5 排序与 TopN 层
  - topN / bottomN / ranking
- A6 明细定位层
  - 精确查询、唯一键定位

### Layer-B：补洞增强层（补覆盖，建议 15%~30%）

基于随机字段组合生成候选 SQL/query，但必须满足：
- 仅针对 Layer-A 未覆盖簇触发
- 不直接作为主分层依据
- 通过更严格验证后才进入 valid

---

## 3.2 四个维度标签（每条样本都要打标签）

每条样本建议打 4 类标签，便于后续评估与重采样：

- `intent_family`：聚合/分组/过滤/时间/TopN/明细
- `constraint_depth`：无过滤/单过滤/多过滤
- `time_semantic`：none/range/yoy/mom/wow
- `slot_complexity`：0/1/2+ slots

这样后续可以统计“不是生成了多少条”，而是“每个层级是否覆盖齐了”。

---

## 4. 第一层（Layer-A）加固建议

## 4.1 字段角色识别从“类型判断”升级为“角色分类”

当前更多按字段类型判断（数值/非数值）。
建议增加角色识别：
- `metric`（可聚合数值）
- `dimension`（分类维度）
- `time_dimension`（日期/时间）
- `entity_key`（高唯一主键）

实现入口建议：
- 扩展 `profile_table_with_data` 输出 `role`
- 或新增 `generation/field_profiler.py`

## 4.2 时间层显式化（YoY/MoM/WoW）

你提到的同比环比非常关键，建议变成强制层：

- 同比模板（按月/季度）
- 环比模板（按月/周）
- 时间窗口模板（近7/30/90天）

前置条件：
- 表存在 `time_dimension`
- 具备可聚合 metric

校验约束：
- SQL 中必须有标准时间截断与对齐逻辑（如 month 粒度）
- query 中必须体现“同比/环比/时间窗口”词义

## 4.3 分组 + 过滤组合的阶梯化

建议把“分组叠加过滤”拆成难度阶梯：
- C1：单维分组 + 单过滤
- C2：单维分组 + 双过滤
- C3：多维分组 + 单过滤
- C4：多维分组 + 多过滤

避免一上来生成过复杂组合导致 invalid 率过高。

## 4.4 规则样本质量闸门

对 Layer-A 生成增加最低质量闸门：
- SQL 可执行率
- 语义一致率（IntentVerifier）
- 去重率（query+sql）
- 空结果容忍但结构正确（已有逻辑可保留）

---

## 5. 第二层（Layer-B）补洞机制建议

## 5.1 从“随机采样”改为“缺口驱动采样”

当前随机采样可保留，但触发条件应改为：
- 先计算 Layer-A coverage matrix
- 只在缺口格子（bucket）中进行字段组合探索

示例 bucket：
- `intent_family=group` + `constraint_depth=2` + `time_semantic=none`

## 5.2 动态权重替代静态权重

当前 `QUERY_TYPES` 是静态权重。
建议：
- 覆盖低的 bucket 权重自动提升
- invalid 率高的 bucket 降温
- 每 N 轮刷新一次权重

## 5.3 补洞样本更严格准入

Layer-B 样本建议多一道筛选：
- 语义去重（和已有 valid 相似度阈值）
- Round-trip 强校验（建议使用 `validation/round_trip.py` 的完整流程，而不是简化版）

---

## 6. 建议的数据结构扩展

在 `valid.jsonl` / `invalid.jsonl` 为每条记录新增：

```json
{
  "layer": "A|B",
  "intent_family": "aggregate|group|filter|time|topn|detail",
  "constraint_depth": 0,
  "time_semantic": "none|range|yoy|mom|wow",
  "slot_complexity": 1,
  "difficulty": "L1|L2|L3",
  "coverage_bucket": "group__depth2__none",
  "source_method": "rule|llm_generation|coverage_patch"
}
```

收益：
- 方便量化“分层覆盖率”
- 方便做增量重采样
- 方便线上问题回放定位到具体层

---

## 7. 目标配比（建议初始值）

可先用一组工程上稳妥的配比：

- Layer-A（主干）: 80%
  - A1/A2/A3/A6 为主体
  - A4（时间层）建议占 Layer-A 的 20%~30%
- Layer-B（补洞）: 20%

每轮构建输出指标：
- 各层样本数
- 各 bucket 覆盖率
- 各层 SQL 可执行率
- 各层意图一致率
- 各层最终 valid 率

---

## 8. 落地顺序（低风险迭代）

### 第一步（先做，低侵入）
- 给现有生成结果加分层标签
- 区分 `layer=A/B`
- 建立 coverage matrix 统计脚本

### 第二步（中等改动）
- 把时间语义（同比/环比/窗口）纳入 Layer-A 强制层
- 扩展字段角色识别

### 第三步（增强收益）
- Layer-B 改为缺口驱动触发
- 引入动态权重与严格准入

### 第四步（质量闭环）
- `GenerationPipeline` 里替换简化 round-trip 为完整验证
- 把线上反馈 case 反推到具体分层 bucket

---

## 9. 对你当前设计的结论

你的两层思路完全成立，建议这样定性：

- **第一层（基于统计特征）= 分层主干（必须强化）**
- **第二层（随机字段组合）= 查漏补缺（必须收敛为缺口驱动）**

如果按这个方向推进，预热阶段会从“能生成”升级到“可控覆盖、可解释质量、可持续迭代”。

---

## 10. 表名与 Base Schema 前移到预热（新增）

你提到的关键点非常重要：
- table 不应由 runtime 决定
- 预热输入应显式绑定某个表（或表集合）的 base_schema

建议原则：
- `runtime` 只消费 `valid.jsonl` 与路由结果，不再承担“定义预热表范围”职责
- `build` 阶段通过 `--tables`（或配置中的 `schema.table_names`）确定预热范围
- 若未提供 `schema.path`，则在预热阶段基于 DB 元数据自动构建 `schema_from_db.json`

推荐流程：
1. 读取 `schema.path`；若为空则走 DB 自动建 schema
2. 用 `schema.table_names` 或 `--tables` 过滤目标表
3. 将该 base_schema 作为 Layer-A / Layer-B 的统一输入
4. 输出中保留 `table` 字段，便于后续按表 coverage 与导出

这样可保证：
- “哪个表参与预热”在 build 时就确定
- schema 与样本来源一致，避免 runtime 才补 schema 的歧义
