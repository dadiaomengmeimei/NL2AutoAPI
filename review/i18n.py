"""
Internationalization (i18n) module for the Review Interface.

Usage:
    from review.i18n import t, set_language
    set_language("en")  # or "zh"
    label = t("reviewer")  # -> "Reviewer"
"""

_current_lang = "en"

# ─── Translation dictionary ───────────────────────────────────────────
# Keys are logical identifiers; values are {lang: text} dicts.
_TRANSLATIONS: dict[str, dict[str, str]] = {
    # ── Global ──
    "app_title": {
"zh": "# NL2AutoAPI",
"en": "# NL2AutoAPI",
    },
    "app_subtitle": {
        "zh": "*Schema › Pre-build › 校验 › 审核 › 运行时查询*",
        "en": "*Schema › Pre-build › Validation › Review › Runtime Query*",
    },
    "reviewer": {
        "zh": "审核人",
        "en": "Reviewer",
    },
    "reviewer_placeholder": {
        "zh": "请输入审核人姓名",
        "en": "Enter reviewer name",
    },
    "table_desc_label": {
        "zh": "表描述（自动生成）",
        "en": "Table Description (auto-generated)",
    },

    # ── Tab names ──
    "tab_schema": {"zh": "Schema", "en": "Schema"},
    "tab_dataset": {"zh": "数据集", "en": "Dataset"},
    "tab_validation": {"zh": "校验", "en": "Validation"},
    "tab_runtime": {"zh": "查询", "en": "Query"},
    "tab_review_queue": {"zh": "审核", "en": "Review"},
    "tab_version": {"zh": "版本", "en": "Versions"},
    "tab_stats": {"zh": "统计", "en": "Stats"},

    # ── Schema tab ──
    "schema_title": {
        "zh": "## Schema 管理",
        "en": "## Schema Management",
    },
    "schema_desc": {
        "zh": "浏览/编辑 Schema、自动修正字段描述、裁剪无用列、运行 Pre-build 生成初始数据集。",
        "en": "Browse/edit Schema, auto-fix field descriptions, prune unused columns, run Pre-build to generate initial dataset.",
    },
    "schema_gen_accordion": {
        "zh": "从数据库生成",
        "en": "Generate from Database",
    },
    "schema_gen_desc": {
        "zh": "从数据库拉取表结构，自动生成 Schema JSON。",
        "en": "Pull table structure from database and auto-generate Schema JSON.",
    },
    "btn_gen_schema": {
        "zh": "生成 Schema",
        "en": "Generate Schema",
    },
    "schema_editor_accordion": {
        "zh": "编辑器",
        "en": "Editor",
    },
    "schema_editor_label": {
        "zh": "Schema JSON（可编辑）",
        "en": "Schema JSON (editable)",
    },
    "btn_reload": {"zh": "重新加载", "en": "Reload"},
    "btn_save": {"zh": "保存", "en": "Save"},
    "field_mgmt_accordion": {
        "zh": "字段管理",
        "en": "Field Management",
    },
    "field_ops_title": {"zh": "#### 字段操作", "en": "#### Field Operations"},
    "field_del_label": {"zh": "选择要删除的字段", "en": "Select field to delete"},
    "btn_delete_field": {"zh": "删除字段", "en": "Delete Field"},
    "smart_prune_desc": {
        "zh": "**智能裁剪** — LLM 自动识别并移除内部/系统字段（如 created_at, etl_flag）",
        "en": "**Smart Prune** — LLM auto-identifies and removes internal/system fields (e.g. created_at, etl_flag)",
    },
    "btn_smart_prune": {"zh": "智能裁剪", "en": "Smart Prune"},
    "autofix_desc_title": {"zh": "#### 自动修正描述", "en": "#### Auto-Fix Descriptions"},
    "autofix_desc_hint": {
        "zh": "探索循环: 生成查询 → SQL → 执行 → 校验 → 优化描述",
        "en": "Exploration loop: Generate query → SQL → Execute → Verify → Optimize description",
    },
    "global_autofix_title": {
        "zh": "**全局自动修正** — 修正所有字段",
        "en": "**Global Auto-Fix** — Fix all fields",
    },
    "rounds_label": {"zh": "轮次", "en": "Rounds"},
    "btn_global_autofix": {"zh": "全局修正", "en": "Global Auto-Fix"},
    "single_field_autofix_title": {"zh": "**单字段自动修正**", "en": "**Single Field Auto-Fix**"},
    "field_select_label": {"zh": "选择字段", "en": "Select field"},
    "btn_field_autofix": {"zh": "单字段修正", "en": "Field Auto-Fix"},
    "autofix_log_title": {"zh": "#### 自动修正进度日志", "en": "#### Auto-Fix Progress Log"},
    "autofix_log_label": {"zh": "进度日志", "en": "Progress log"},
    "prebuild_accordion": {
        "zh": "Pre-build",
        "en": "Pre-build",
    },
    "prebuild_desc": {
        "zh": "基于 Schema 自动生成初始 Query-SQL 数据集，写入 `valid.jsonl`。请确保先完成 Auto-Fix 审核。",
        "en": "Auto-generate initial Query-SQL dataset from Schema into `valid.jsonl`. Make sure Auto-Fix review is done first.",
    },
    "btn_prebuild": {"zh": "运行 Pre-build", "en": "Run Pre-build"},
    "prebuild_log_label": {"zh": "Pre-build 日志", "en": "Pre-build Log"},

    # ── Dataset tab ──
    "dataset_title": {"zh": "## 数据集", "en": "## Dataset"},
    "dataset_desc": {
        "zh": "浏览、编辑或删除 `valid.jsonl` 中的记录。",
        "en": "Browse, edit, or delete records in `valid.jsonl`.",
    },
    "edit_delete_title": {"zh": "### 编辑 / 删除", "en": "### Edit / Delete"},
    "record_idx_label": {
        "zh": "记录索引 #（从上方表格中获取）",
        "en": "Record index # (from the table above)",
    },
    "btn_load_record": {"zh": "加载记录", "en": "Load Record"},
    "query_label": {"zh": "查询", "en": "Query"},
    "api_name_label": {"zh": "API 名称", "en": "API Name"},
    "api_desc_label": {"zh": "描述", "en": "Description"},
    "sql_label": {"zh": "SQL", "en": "SQL"},
    "api_schema_json_label": {"zh": "API Schema JSON", "en": "API Schema JSON"},
    "btn_save_edit": {"zh": "保存修改", "en": "Save Changes"},
    "btn_delete_record": {"zh": "删除记录", "en": "Delete Record"},
    "schema_feedback_accordion_edit": {
        "zh": "🔔 Schema 反馈（修改 SQL 时自动分析）",
        "en": "🔔 Schema Feedback (auto-analyzed on SQL edit)",
    },
    "schema_feedback_default_edit": {
        "zh": "_保存修改后，系统会自动分析 SQL 变更并提供 Schema 字段描述更新建议。_",
        "en": "_After saving, the system will auto-analyze SQL/Query changes and suggest Schema field description updates._",
    },
    "btn_apply_suggestion": {"zh": "应用建议", "en": "Apply"},
    "btn_dismiss_suggestion": {"zh": "忽略", "en": "Dismiss"},
    "btn_prev_page": {"zh": "上一页", "en": "Previous"},
    "btn_next_page": {"zh": "下一页", "en": "Next"},
    "btn_refresh": {"zh": "刷新", "en": "Refresh"},
    "table_headers": {
        "zh": ["#", "API名称", "查询", "SQL", "描述", "来源"],
        "en": ["#", "API Name", "Query", "SQL", "Description", "Source"],
    },

    # ── Validation tab ──
    "validation_title": {"zh": "## 数据校验", "en": "## Validation"},
    "validation_desc": {
        "zh": "自动复审 → 修正 SQL → 通过 / 跳过 / 拒绝",
        "en": "Auto-review → Fix SQL → Approve / Skip / Reject",
    },
    "ops_title": {"zh": "### 操作", "en": "### Actions"},
    "btn_concretize": {"zh": "具体化", "en": "Concretize"},
    "btn_auto_sql": {"zh": "生成 SQL", "en": "Generate SQL"},
    "btn_auto_review": {"zh": "自动复审", "en": "Auto-Review"},
    "btn_approve": {"zh": "通过", "en": "Approve"},
    "btn_skip": {"zh": "跳过", "en": "Skip"},
    "btn_reject": {"zh": "拒绝", "en": "Reject"},
    "validation_hint": {
        "zh": "提示：使用「具体化 Query」将抽象查询变具体，或使用「自动生成 SQL」从查询自动起草 SQL。",
        "en": "Tip: Use *Concretize Query* to make abstract queries concrete, or *Auto-Generate SQL* to draft SQL from the query.",
    },
    "api_name_auto_label": {"zh": "API 名称（自动生成）", "en": "API Name (auto-generated)"},
    "api_desc_auto_label": {"zh": "API 描述（自动生成）", "en": "API Description (auto-generated)"},
    "input_schema_label": {"zh": "输入参数（从 SQL 推断）", "en": "Input Schema (inferred from SQL)"},
    "sql_editable_label": {"zh": "SQL（可编辑）", "en": "SQL (editable)"},
    "schema_feedback_accordion_approve": {
        "zh": "🔔 Schema 反馈（审核通过时自动分析）",
        "en": "🔔 Schema Feedback (auto-analyzed on approval)",
    },
    "schema_feedback_default_approve": {
        "zh": "_审核通过后，系统会自动分析 SQL/Query 变更并提供 Schema 字段描述更新建议。_",
        "en": "_After approval, the system will auto-analyze SQL/Query changes and suggest Schema field description updates._",
    },

    # ── Runtime Query tab ──
    "runtime_title": {"zh": "## 运行时查询", "en": "## Runtime Query"},
    "runtime_desc": {
        "zh": "将单条查询走完整运行时流程（路由 → 槽位填充 → SQL 执行 → 结果校验）。",
        "en": "Run a single query through the full runtime pipeline (Route → Slot Fill → SQL Execute → Result Verify).",
    },
    "runtime_unavailable": {
        "zh": "运行时查询不可用，请检查 `runtime_query_ui.py`。",
        "en": "Runtime query unavailable. Please check `runtime_query_ui.py`.",
    },
    "advanced_settings": {"zh": "高级设置（仅高级用户）", "en": "Advanced Settings (power users only)"},
    "btn_run": {"zh": "运行", "en": "Run"},
    "status_label": {"zh": "状态", "en": "Status"},
    "generated_sql_label": {"zh": "生成的 SQL", "en": "Generated SQL"},
    "record_json_label": {"zh": "记录 JSON", "en": "Record JSON"},
    "note_label": {"zh": "备注", "en": "Note"},
    "step1_title": {"zh": "### 第一步 — 编辑 Query / SQL", "en": "### Step 1 — Edit Query / SQL"},
    "manual_sql_label": {"zh": "手动 SQL（可编辑）", "en": "Manual SQL (editable)"},
    "btn_fill_from_record": {"zh": "回填", "en": "Fill"},
    "btn_auto_review_short": {"zh": "自动复审", "en": "Auto-Review"},
    "btn_test_query_sql": {"zh": "测试", "en": "Test"},
    "step2_title": {"zh": "### 第二步 — 导入到 valid.jsonl", "en": "### Step 2 — Import to valid.jsonl"},
    "btn_import_valid": {"zh": "导入 Valid", "en": "Import to Valid"},
    "import_result_label": {"zh": "导入结果", "en": "Import Result"},
    "input_schema_inferred_label": {"zh": "输入参数（从 SQL 推断）", "en": "Input Schema (inferred from SQL)"},

    # ── Review Queue tab ──
    "review_queue_title": {"zh": "## 审核", "en": "## Review"},
    "review_queue_desc": {
        "zh": "运行时纠错和扩展任务。",
        "en": "Runtime correction & expansion tasks.",
    },
    "sql_template_label": {"zh": "SQL（模板，可编辑）", "en": "SQL (template, editable)"},
    "invoked_sql_label": {
        "zh": "🔍 填槽后SQL（只读，实际执行的SQL）",
        "en": "🔍 Invoked SQL (read-only, actual executed SQL)",
    },
    "task_instruction_label": {"zh": "任务说明 / 区分指令", "en": "Task Instruction / Distinction"},
    "review_ops_title": {"zh": "### 审核操作", "en": "### Review Actions"},
    "comment_label": {"zh": "审核意见", "en": "Review Comment"},
    "btn_next": {"zh": "下一个", "en": "Next"},
    "review_queue_hint": {
        "zh": "提示：先具体化抽象查询，然后生成 SQL 或自动复审。",
        "en": "Tip: Concretize abstract queries first, then generate SQL or auto-review.",
    },

    # ── Version History tab ──
    "version_title": {"zh": "## 版本历史", "en": "## Version History"},
    "version_desc": {
        "zh": "查看操作日志，支持按时间点恢复数据集。",
        "en": "View operation logs and restore datasets to a specific point in time.",
    },
    "binlog_detail_title": {"zh": "### 操作日志详情", "en": "### Operation Log Details"},
    "dataset_name_label": {"zh": "数据集", "en": "Dataset"},
    "btn_view_log": {"zh": "查看", "en": "View"},
    "restore_title": {"zh": "### 按时间点恢复", "en": "### Restore by Timestamp"},
    "restore_ts_label": {
        "zh": "选择恢复时间点",
        "en": "Select restore point",
    },
    "btn_restore": {"zh": "恢复", "en": "Restore"},

    # ── Statistics tab ──
    "stats_pending_validation": {"zh": "待校验记录数", "en": "Pending validation records"},
    "stats_pending_review": {"zh": "待审核任务数", "en": "Pending review tasks"},
    "stats_overview": {
        "zh": "### 队列概览\n- 待校验记录数: **{invalid}**\n- 待审核任务数: **{tasks}**\n\n### 任务类型分布",
        "en": "### Queue Overview\n- Pending validation: **{invalid}**\n- Pending review: **{tasks}**\n\n### Task Type Distribution",
    },
    "stats_type_dist": {"zh": "### 任务类型分布", "en": "### Task Type Distribution"},

    # ── Status messages ──
    "msg_all_invalid_done": {
        "zh": "✅ 所有无效记录已处理完毕。",
        "en": "✅ All invalid records have been processed.",
    },
    "msg_all_tasks_done": {
        "zh": "✅ 所有审核任务已处理完毕。",
        "en": "✅ All review tasks have been processed.",
    },
    "msg_reviewer_required": {
        "zh": "⚠️ 请先填写审核人",
        "en": "⚠️ Please enter Reviewer name first.",
    },
    "msg_action_in_progress": {
        "zh": "⚠️ 另一操作正在进行中，请稍候。",
        "en": "⚠️ Another action is in progress. Please wait.",
    },
    "msg_auto_review_start": {
        "zh": "请先点击「自动复审（基于当前SQL）」",
        "en": "Click *Auto-Review (current SQL)* to start.",
    },
    "msg_record_progress": {
        "zh": "记录 {current} / {total} 待处理",
        "en": "Record {current} / {total} remaining",
    },
    "msg_task_progress": {
        "zh": "任务 {current} / {total} (类型: {type})",
        "en": "Task {current} / {total} (type: {type})",
    },
    "msg_page_info": {
        "zh": "共 {total} 条 | 第 {page}/{pages} 页",
        "en": "{total} records | Page {page}/{pages}",
    },
    "msg_prebuild_blocked": {
        "zh": (
            "🚫 **全量 Pre-build 已禁用**\n\n"
            "当前 valid.jsonl 已有 **{total}** 条记录"
            "（其中 **{edited}** 条为用户手动编辑）。\n\n"
            "全量 Pre-build 会覆盖这些数据，包括您手动修改的 SQL 和 Query。\n\n"
            "---\n\n"
            "**替代方案（推荐）：**\n"
            "1. 修改 Schema 字段描述后，在各页签（数据集/数据校验/审核队列）中修改 SQL\n"
            "2. 系统会**自动分析差异**并提供级联更新建议\n"
            "3. 点击「✅ 应用建议」即可自动更新相关字段的描述、SQL 和 Query\n"
            "4. 用户手动编辑过的记录（标记为 user_edited）不会被级联更新影响\n\n"
            "如果确实需要全量重建，请先手动清空 valid.jsonl 后再执行。"
        ),
        "en": (
            "🚫 **Full Pre-build Disabled**\n\n"
            "valid.jsonl already has **{total}** records "
            "(**{edited}** user-edited).\n\n"
            "A full Pre-build would overwrite this data, including your manually modified SQL and Query.\n\n"
            "---\n\n"
            "**Recommended alternatives:**\n"
            "1. After editing Schema field descriptions, modify SQL in respective tabs (Dataset/Validation/Review Queue)\n"
            "2. The system will **auto-analyze diffs** and suggest cascade updates\n"
            "3. Click *✅ Apply Suggestions* to auto-update related descriptions, SQL and Query\n"
            "4. User-edited records (marked as `user_edited`) will not be affected by cascade updates\n\n"
            "If you really need a full rebuild, manually clear valid.jsonl first."
        ),
    },
    "msg_prebuild_no_autofix": {
        "zh": (
            "⚠️ 检测到您尚未执行 Auto-Fix 和 Schema 审核。\n\n"
            "建议流程：\n"
            "1. 先点击「✂️ 自动裁剪」清理无用字段\n"
            "2. 再点击「🔧 全局 Auto-Fix」修正字段描述\n"
            "3. 手动检查 Schema 确认无误\n"
            "4. 最后再点击「🚀 运行 Prebuild」\n\n"
            "如果确认无需 Auto-Fix，请再次点击「🚀 运行 Prebuild」按钮。"
        ),
        "en": (
            "⚠️ Auto-Fix and Schema review have not been performed yet.\n\n"
            "Recommended workflow:\n"
            "1. Click *✂️ Smart Prune* to remove useless fields\n"
            "2. Click *🔧 Global Auto-Fix* to fix field descriptions\n"
            "3. Manually inspect Schema for accuracy\n"
            "4. Then click *🚀 Run Pre-build*\n\n"
            "If you're sure Auto-Fix is not needed, click *🚀 Run Pre-build* again."
        ),
    },
    "msg_prebuild_starting": {"zh": "⏳ Prebuild 启动中...\n", "en": "⏳ Prebuild starting...\n"},
    "msg_prebuild_done": {"zh": "\n✅ Prebuild 完成！", "en": "\n✅ Prebuild complete!"},
    "msg_prebuild_failed": {"zh": "\n❌ Prebuild 失败", "en": "\n❌ Prebuild failed"},
    "msg_prebuild_timeout": {"zh": "\n❌ Prebuild 超时（>10分钟）", "en": "\n❌ Prebuild timed out (>10 min)"},

    # ── Render: review suggestion ──
    "review_suggestion_title": {"zh": "### 审核建议", "en": "### Review Suggestion"},
    "review_suggestion_status": {"zh": "- 状态: 先试试 *自动复审（基于当前SQL）*", "en": "- Status: Try *Auto-Review (current SQL)* first"},
    "review_suggestion_error_type": {"zh": "- 错误类型:", "en": "- Error type:"},
    "review_suggestion_failure": {"zh": "- 失败原因:", "en": "- Failure reason:"},
    "original_sql_title": {"zh": "### 原始 SQL", "en": "### Original SQL"},

    # ── Schema feedback messages ──
    "feedback_reason_column_ambiguity": {"zh": "字段歧义", "en": "Column ambiguity"},
    "feedback_reason_sql_logic": {"zh": "SQL逻辑错误", "en": "SQL logic error"},
    "feedback_reason_value_mismatch": {"zh": "值格式不匹配", "en": "Value format mismatch"},
    "feedback_reason_none": {"zh": "无需更新", "en": "No update needed"},
    "feedback_no_suggestion": {
        "zh": "✅ 无需更新 Schema 字段描述。",
        "en": "✅ No Schema field description updates needed.",
    },
    "feedback_modify_reason": {"zh": "**修改原因**:", "en": "**Modification reason**:"},
    "feedback_suggested_fields": {"zh": "### 📋 建议更新的字段描述", "en": "### 📋 Suggested Field Description Updates"},
    "feedback_field_header": {
        "zh": "| 字段 | 原描述 | 建议描述 | 置信度 |",
        "en": "| Field | Old Desc | Suggested Desc | Confidence |",
    },
    "feedback_cascade_title": {
        "zh": "### 🔗 级联影响的 API（自动跳过用户手动编辑过的记录）",
        "en": "### 🔗 Cascade-Affected APIs (skipping user-edited records)",
    },
    "feedback_cascade_header": {
        "zh": "| API 名称 | 查询 | 更新维度 | 建议描述 | 建议SQL | 原因 |",
        "en": "| API Name | Query | Update Scope | Suggested Desc | Suggested SQL | Reason |",
    },
    "feedback_apply_hint": {
        "zh": "⚠️ **默认不会自动更新。** 请点击「✅ 应用建议」确认更新，或点击「↩️ 撤销」忽略。",
        "en": "⚠️ **Updates are NOT applied automatically.** Click *✅ Apply Suggestions* to confirm, or *↩️ Dismiss* to ignore.",
    },
    "feedback_applied": {
        "zh": "✅ 已更新 {count} 个字段描述: {fields}",
        "en": "✅ Updated {count} field description(s): {fields}",
    },
    "feedback_cascade_applied": {
        "zh": "✅ 已级联更新 {count} 个关联 API: {details}",
        "en": "✅ Cascade-updated {count} related API(s): {details}",
    },
    "feedback_no_pending": {"zh": "ℹ️ 没有待应用的建议。", "en": "ℹ️ No pending suggestions."},
    "feedback_no_applicable": {"zh": "⚠️ 没有可应用的字段更新。", "en": "⚠️ No applicable field updates."},
    "feedback_dismissed": {
        "zh": "↩️ 已忽略 {count} 条字段描述更新建议。",
        "en": "↩️ Dismissed {count} field description update suggestion(s).",
    },
    "feedback_dismiss_empty": {"zh": "ℹ️ 没有待处理的建议。", "en": "ℹ️ No pending suggestions."},

    # ── Auth ──
    "auth_message": {
        "zh": "请输入用户名登录（密码任意）",
        "en": "Enter your username to log in (any password)",
    },
    "auth_enabled": {
        "zh": "✓ 白名单校验已启用，允许用户: {users}",
        "en": "✓ Whitelist auth enabled, allowed users: {users}",
    },
    "auth_disabled": {
        "zh": "ℹ️  未配置 auth_users，跳过白名单校验",
        "en": "ℹ️  No auth_users configured, skipping whitelist auth",
    },

    # ── Misc status ──
    "msg_query_empty": {"zh": "⚠️ Query 为空", "en": "⚠️ Query is empty"},
    "msg_sql_empty": {"zh": "⚠️ SQL 为空", "en": "⚠️ SQL is empty"},
    "msg_schema_not_found": {"zh": "⚠️ Schema 文件不存在", "en": "⚠️ Schema file not found"},
    "msg_record_saved": {"zh": "✅ 记录 #{idx} 已保存", "en": "✅ Record #{idx} saved"},
    "msg_record_deleted": {"zh": "✅ 记录 #{idx} 已删除", "en": "✅ Record #{idx} deleted"},
    "msg_invalid_index": {"zh": "⚠️ 无效的记录索引", "en": "⚠️ Invalid record index"},
    "msg_index_out_of_range": {"zh": "⚠️ 记录索引越界", "en": "⚠️ Record index out of range"},
    "msg_json_error": {"zh": "⚠️ API Schema JSON 格式错误", "en": "⚠️ API Schema JSON format error"},
    "msg_boundary_saved": {"zh": "✅ 已保存能力边界: {path}", "en": "✅ Boundary saved: {path}"},
    "msg_boundary_empty": {"zh": "⚠️ 请先生成或填写能力边界", "en": "⚠️ Please generate or fill in the boundary first"},
    "msg_auto_gen_success": {"zh": "✅ 自动生成并通过校验", "en": "✅ Auto-generated and passed validation"},
    "msg_auto_gen_warn": {"zh": "⚠️ 自动生成但SQL检查未通过，请人工调整", "en": "⚠️ Auto-generated but SQL check failed, please adjust manually"},
    "msg_auto_gen_fail": {"zh": "❌ 自动生成失败，请检查数据库连接或手动填写", "en": "❌ Auto-generation failed, check DB connection or fill manually"},
    "msg_auto_review_pass": {"zh": "✅ 自动复审通过", "en": "✅ Auto-review passed"},
    "msg_auto_review_fail": {"zh": "❌ 自动复审未通过", "en": "❌ Auto-review failed"},
    "msg_concretize_done": {"zh": "✅ 已将抽象Query具体化", "en": "✅ Abstract query concretized"},
    "msg_concretize_skip": {"zh": "ℹ️ Query 已较具体，无需具体化", "en": "ℹ️ Query is already concrete, no change needed"},
    "msg_concretize_fail": {"zh": "⚠️ 未能自动具体化，保留原Query", "en": "⚠️ Could not concretize automatically, keeping original query"},
    "msg_write_in_progress": {"zh": "⚠️ 另一写操作正在进行中，请稍候。", "en": "⚠️ Another write operation is in progress. Please wait."},
    "msg_schema_saved": {"zh": "✅ Schema 已保存到 {path}", "en": "✅ Schema saved to {path}"},
    "msg_schema_loaded": {"zh": "✅ 已加载: {path}", "en": "✅ Loaded: {path}"},
    "msg_global_autofix_done": {"zh": "✅ 全局 Auto-Fix 完成 ({rounds} 轮)", "en": "✅ Global Auto-Fix complete ({rounds} rounds)"},
    "msg_field_autofix_done": {"zh": "✅ 字段 [{field}] Auto-Fix 完成: {desc}", "en": "✅ Field [{field}] Auto-Fix complete: {desc}"},
    "msg_field_deleted": {"zh": "✅ 字段 [{field}] 已删除", "en": "✅ Field [{field}] deleted"},
    "msg_field_not_found": {"zh": "⚠️ 字段 [{field}] 不存在", "en": "⚠️ Field [{field}] does not exist"},
    "msg_select_field": {"zh": "⚠️ 请选择字段", "en": "⚠️ Please select a field"},
    "msg_prune_done": {
        "zh": "✅ 自动裁剪完成，删除了 {count} 个字段: {fields}",
        "en": "✅ Smart prune complete, removed {count} field(s): {fields}",
    },
    "msg_prune_none": {"zh": "✅ 所有字段都有业务价值，无需裁剪", "en": "✅ All fields are useful, no pruning needed"},
    "msg_schema_gen_done": {
        "zh": "✅ 从数据库生成 Schema 完成: {tables} 个表, {fields} 个字段, 已保存到 {path}",
        "en": "✅ Schema generated from DB: {tables} table(s), {fields} field(s), saved to {path}",
    },
    "msg_schema_gen_fail": {"zh": "❌ 生成 Schema 失败: {error}", "en": "❌ Schema generation failed: {error}"},
    "msg_db_connect_fail": {"zh": "❌ 数据库连接失败", "en": "❌ Database connection failed"},
    "msg_autofix_running": {"zh": "⚠️ Auto-Fix 正在运行中，请稍候。", "en": "⚠️ Auto-Fix is already running. Please wait."},
    "msg_prebuild_running": {"zh": "⚠️ Pre-build 正在运行中，请稍候。", "en": "⚠️ Pre-build is already running. Please wait."},
    "msg_restored": {
        "zh": "✅ 已恢复 {ds} 到 {ts}，共 {count} 条记录，文件: {dest}",
        "en": "✅ Restored {ds} to {ts}, {count} record(s), file: {dest}",
    },
    "msg_restore_empty": {"zh": "⚠️ 恢复结果为空", "en": "⚠️ Restore result is empty"},
    "msg_restore_input_required": {"zh": "⚠️ 请填写数据集名和时间点", "en": "⚠️ Please enter dataset name and timestamp"},
    "msg_restore_unsupported": {"zh": "⚠️ 不支持的数据集: {ds}", "en": "⚠️ Unsupported dataset: {ds}"},
    "msg_no_version_records": {"zh": "暂无版本记录", "en": "No version records yet"},
    "msg_no_ops": {"zh": "暂无操作记录。", "en": "No operation records yet."},
    "msg_no_log": {"zh": "无日志", "en": "No log"},
    "msg_no_sample_values": {"zh": "暂无样例值", "en": "No sample values available"},
    "msg_table_info_fail": {"zh": "表信息获取失败", "en": "Failed to retrieve table information"},
    "msg_build_api_unavailable": {
        "zh": "⚠️ 无法构建API（runtime_query_ui helper不可用）",
        "en": "⚠️ Cannot build API (runtime_query_ui helper unavailable)",
    },
    "binlog_table_header": {
        "zh": "| 时间戳 | 操作 | API 名称 | 查询 | 元数据 |",
        "en": "| Timestamp | Operation | API Name | Query | Metadata |",
    },

    # ── Render: task content ──
    "task_runtime_correction": {"zh": "## 运行时纠错任务", "en": "## Runtime Correction Task"},
    "task_user_query": {"zh": "**用户查询**:", "en": "**User Query**:"},
    "task_wrong_api": {"zh": "### 匹配错误的 API", "en": "### Mismatched API"},
    "task_candidate_tables": {"zh": "### 候选表", "en": "### Candidate Tables"},
    "task_correct_api": {"zh": "### 建议正确 API", "en": "### Suggested Correct API"},
    "task_distinction": {"zh": "### 区分指令", "en": "### Distinction Instruction"},
    "task_auto_analysis": {"zh": "### 自动分析", "en": "### Auto Analysis"},
    "task_schema_expansion": {"zh": "## Schema 扩展任务（事后）", "en": "## Schema Expansion Task (Post-hoc)"},
    "task_original_query": {"zh": "**原始查询**:", "en": "**Original Query**:"},
    "task_expanded_queries": {"zh": "### 扩展查询", "en": "### Expanded Queries"},
    "task_base_api": {"zh": "### 基础 API", "en": "### Base API"},
    "task_schema_candidates": {"zh": "### 生成的 Schema 候选", "en": "### Generated Schema Candidates"},
    "task_candidate": {"zh": "#### 候选", "en": "#### Candidate"},
}


def set_language(lang: str):
    """Set the current language. Supported: 'en', 'zh'. Default: 'en'."""
    global _current_lang
    if lang in ("en", "zh"):
        _current_lang = lang
    else:
        _current_lang = "en"


def get_language() -> str:
    """Return the current language code."""
    return _current_lang


def t(key: str, **kwargs) -> str:
    """
    Translate a key to the current language.
    Supports keyword formatting: t("msg_record_saved", idx=5) -> "✅ Record #5 saved"
    """
    entry = _TRANSLATIONS.get(key)
    if entry is None:
        return key  # fallback: return key itself

    text = entry.get(_current_lang) or entry.get("en") or key
    if kwargs and isinstance(text, str):
        try:
            text = text.format(**kwargs)
        except (KeyError, IndexError):
            pass
    return text


def t_list(key: str) -> list:
    """Return a translation that is expected to be a list (e.g. table headers)."""
    entry = _TRANSLATIONS.get(key)
    if entry is None:
        return [key]
    val = entry.get(_current_lang) or entry.get("en") or [key]
    return val if isinstance(val, list) else [val]
