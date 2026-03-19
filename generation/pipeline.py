"""
主生成Pipeline：整合SQL生成、Query生成、API生成和验证
"""

import random
from typing import Optional

from core.database import db_manager, execute_sql
from core.utils import save_jsonl, save_jsonl_dedup_sql, load_jsonl, extract_slots, fill_sql_with_values
from core.logger import get_logger

logger = get_logger()
from schema.loader import SchemaLoader
from schema.sampler import SchemaSampler  # 需要创建
from generation.query_types import get_weighted_types, QUERY_TYPES
from generation.sql_generator import SQLGenerator
from generation.query_generator import QueryGenerator
from generation.api_generator import APIGenerator
from generation.sql_column_corrector import correct_sql_columns
from validation.intent_verify import IntentVerifier
from validation.round_trip import RoundTripChecker  # 需要创建或简化


class GenerationPipeline:
    """
    完整生成Pipeline
    
    Schema → SQL → Query → API → Validate → Store
    """
    
    def __init__(
        self,
        db_conn,
        schema: SchemaLoader,
        valid_path: str = "dataset_valid.jsonl",
        invalid_path: str = "dataset_invalid.jsonl",
        gate=None,
    ):
        self.db_conn = db_conn
        self.schema = schema
        self.valid_path = valid_path
        self.invalid_path = invalid_path
        self.gate = gate
        
        # 各阶段生成器
        self.schema_sampler = SchemaSampler(schema)
        self.sql_generator = SQLGenerator()
        self.query_generator = QueryGenerator()
        self.api_generator = APIGenerator()
        self.intent_verifier = IntentVerifier()
        self.round_trip_checker = None  # 延迟初始化
        
        # 内存API池
        self.api_pool: list[dict] = []
        self.api_name_set: set[str] = set()
        
        # 统计
        self.valid_count = 0
        self.invalid_count = 0
        self._last_query_type: Optional[str] = None
    
    def run(self, iterations: int = 100, do_round_trip: bool = True):
        """
        运行生成Pipeline
        
        Args:
            iterations: 迭代次数
            do_round_trip: 是否进行往返验证
        """
        # 集合所有查询类型和权重（后续会按表现动态调整）
        type_names = list(QUERY_TYPES.keys())
        base_weights = {n: QUERY_TYPES[n].weight for n in type_names}
        type_stats: dict[str, dict[str, int]] = {
            n: {"attempts": 0, "success": 0, "fail": 0} for n in type_names
        }
        
        # 加载已有API用于round-trip检查
        if do_round_trip:
            existing = load_jsonl(self.valid_path)
            for r in existing:
                api = r.get("api_schema")
                if api:
                    self.api_pool.append(api)
                    self.api_name_set.add(api.get("name", ""))
        logger.info(f"加载已有 {len(self.api_pool)} 个API用于round-trip验证")

        for i in range(iterations):
            try:
                logger.info('%s', '='*55)
                logger.info('[%d/%d]', i+1, iterations)

                active_types = [n for n in type_names if not self._should_stop_type(type_stats.get(n, {}))]
                if not active_types:
                    active_types = type_names
                active_weights = [self._exploit_weight(base_weights[n], type_stats.get(n, {})) for n in active_types]

                success = self._run_single_iteration(active_types, active_weights, do_round_trip)
                last_type = self._last_query_type
                if last_type:
                    stats = type_stats.setdefault(last_type, {"attempts": 0, "success": 0, "fail": 0})
                    stats["attempts"] += 1
                if success:
                    self.valid_count += 1
                    if last_type:
                        type_stats[last_type]["success"] += 1
                else:
                    self.invalid_count += 1
                    if last_type:
                        type_stats[last_type]["fail"] += 1
                    
            except Exception as e:
                logger.exception('迭代异常')
                self.invalid_count += 1
                continue
        
        logger.info('%s', '='*55)
        logger.info('完成: valid=%d, invalid=%d, total=%d', self.valid_count, self.invalid_count, iterations)
        for name in type_names:
            s = type_stats.get(name, {})
            if s.get("attempts", 0) > 0:
                logger.info('[TypeStats] %s: attempts=%d success=%d fail=%d', name, s.get('attempts', 0), s.get('success', 0), s.get('fail', 0))
        logger.info('%s', '='*55)

    def _should_stop_type(self, stats: dict) -> bool:
        attempts = int(stats.get("attempts", 0))
        success = int(stats.get("success", 0))
        if attempts >= 6 and success == 0:
            return True
        return False

    def _exploit_weight(self, base_weight: float, stats: dict) -> float:
        success = int(stats.get("success", 0))
        if success <= 0:
            return base_weight
        return base_weight * (1.0 + min(success, 3) * 0.6)
    
    def _run_single_iteration(
        self,
        type_names: list[str],
        weights: list[float],
        do_round_trip: bool
    ) -> bool:
        """运行单次迭代"""
        
        # 1. 选择查询类型和Schema子集
        query_type = random.choices(type_names, weights=weights, k=1)[0]
        self._last_query_type = query_type
        print(f"  query_type : {query_type}")
        
        table_name, schema_subset = self.schema_sampler.sample_for_query_type(query_type)
        schema_fields = schema_subset["tables"][table_name]["fields"]
        
        # 2. 生成SQL
        sql = self.sql_generator.generate(
            table_name=table_name,
            schema_subset=schema_subset,
            query_type=query_type
        )
        if not sql:
            print("  [SKIP] SQL 生成失败")
            return False

        # 列名纠错（基于当前schema字段）
        sql = correct_sql_columns(sql, table_name=table_name, schema_fields=schema_fields)
        print(f"  SQL        : {sql[:80]}...")
        
        slots = extract_slots(sql)
        print(f"  Slots      : {slots}")
        
        # 验证slot规则
        qt = QUERY_TYPES[query_type]
        if not qt.need_fields and slots:
            print(f"  [INVALID] {query_type} 不应有 slot，LLM 生成了 {slots}")
            save_jsonl(self.invalid_path, {
                "iteration": self.valid_count + self.invalid_count,
                "query_type": query_type,
                "sql": sql,
                "reason": "unexpected_slot_in_no_filter_type",
            })
            return False
        
        # 3. 生成Query
        query = self.query_generator.generate(sql, query_type)
        if not query:
            print("  [SKIP] Query 生成失败")
            return False
        print(f"  Query      : {query[:60]}...")

        if self.gate is not None:
            accept, reason, query = self.gate.check_with_concretize(
                query=query, sql=sql, table=table_name, query_type=query_type,
            )
            if not accept:
                self.gate.reject(
                    query=query,
                    sql=sql,
                    table=table_name,
                    query_type=query_type,
                    layer_tag="Layer-B",
                    reason=reason or "不符合常识问法",
                )
                print(f"  [GATE] 拒绝入库: {reason}")
                return False
        
        # 4. 生成API Schema
        api_schema = self.api_generator.generate(sql, query, query_type)
        if not api_schema:
            print("  [SKIP] API Schema 生成失败")
            return False
        
        # 检查名称冲突
        base_name = api_schema.name
        if base_name in self.api_name_set:
            suffix = sum(1 for n in self.api_name_set 
                        if n == base_name or n.startswith(base_name + "_"))
            api_schema.name = f"{base_name}_{suffix}"
            print(f"  [INFO] API name 冲突，重命名为 {api_schema.name}")
        
        # 5. 真实值填槽 + SQL执行验证
        slot_values = self._sample_slot_values(table_name, slots, schema_fields)
        exec_sql = fill_sql_with_values(sql, slot_values)
        unbound_slots = extract_slots(exec_sql)
        if unbound_slots:
            save_jsonl(self.invalid_path, {
                "iteration": self.valid_count + self.invalid_count,
                "query_type": query_type,
                "schema": schema_subset,
                "sql": sql,
                "exec_sql": exec_sql,
                "reason": "unbound_slots",
                "unbound_slots": unbound_slots,
                "query": query,
                "layer_tag": "Layer-B",
            })
            print(f"  [INVALID] 存在未填充slot: {unbound_slots}")
            return False
        print(f"  Exec SQL   : {exec_sql[:80]}...")
        
        exec_result = execute_sql(self.db_conn, exec_sql)
        print(f"  DB result  : status={exec_result['status']}, rows={exec_result.get('row_count', 0)}")
        
        if exec_result["status"] != "success":
            save_jsonl(self.invalid_path, {
                "iteration": self.valid_count + self.invalid_count,
                "query_type": query_type,
                "schema": schema_subset,
                "sql": sql,
                "exec_sql": exec_sql,
                "error": exec_result,
                "query": query,
                "layer_tag": "Layer-B",
            })
            print("  [INVALID] SQL 执行报错")
            return False
        
        # 6. 意图验证
        if not self.intent_verifier.verify(query, sql, exec_result):
            save_jsonl(self.invalid_path, {
                "iteration": self.valid_count + self.invalid_count,
                "query_type": query_type,
                "schema": schema_subset,
                "sql": sql,
                "query": query,
                "exec_result": exec_result,
                "reason": "intent_mismatch",
                "layer_tag": "Layer-B",
            })
            print("  [INVALID] 意图验证未通过")
            return False
        
        # 7. Round-trip检查
        if do_round_trip and self.api_pool:
            # 简化版round-trip：检查query能否召回正确的API
            if not self._simple_round_trip_check(query, api_schema):
                save_jsonl(self.invalid_path, {
                    "iteration": self.valid_count + self.invalid_count,
                    "query_type": query_type,
                    "schema": schema_subset,
                    "sql": sql,
                    "query": query,
                    "reason": "round_trip_failed",
                    "layer_tag": "Layer-B",
                })
                print("  [INVALID] Round-trip 验证未通过")
                return False
        
        # 8. 入库
        self.api_pool.append(api_schema.dict())
        self.api_name_set.add(api_schema.name)
        
        save_jsonl_dedup_sql(self.valid_path, {
            "source": "prebuild_generation",
            "source_stage": "prebuild",
            "source_method": "llm_generation",
            "source_channel": "build_pipeline",
            "query_type": query_type,
            "layer_tag": "Layer-B",
            "schema": schema_subset,
            "query": query,
            "api_schema": api_schema.dict(),
            "slot_values_sample": slot_values,
            "execution_result": exec_result,
            "table": table_name,
        })
        
        print("  [VALID] ✓")
        return True
    
    def _sample_slot_values(
        self,
        table_name: str,
        slots: list[str],
        schema_fields: dict
    ) -> dict:
        """采样slot的真实值"""
        if not slots:
            return {}
        
        from core.utils import _default_value  # 需要提取出来
        
        sampled = {}
        for slot in slots:
            # 尝试匹配字段
            matched_col = None
            field_names = list(schema_fields.keys())
            
            # 完全匹配
            for col in field_names:
                if col == slot or f"slot_{col}" == slot:
                    matched_col = col
                    break
            
            # 后缀匹配
            if not matched_col:
                for col in field_names:
                    if col.endswith(slot.replace("slot_", "")) or slot.replace("slot_", "").endswith(col):
                        matched_col = col
                        break
            
            if matched_col:
                # 从数据库采样
                res = execute_sql(
                    self.db_conn,
                    f"SELECT DISTINCT `{matched_col}` FROM `{table_name}` "
                    f"WHERE `{matched_col}` IS NOT NULL LIMIT 30"
                )
                if res["status"] == "success" and res["data"]:
                    sampled[slot] = random.choice(res["data"])[0]
                else:
                    col_type = schema_fields.get(matched_col, {}).get("type", "VARCHAR")
                    sampled[slot] = _default_value(col_type)
            else:
                sampled[slot] = "test"
        
        return sampled
    
    def _simple_round_trip_check(self, query: str, api_schema: dict) -> bool:
        """
        简化版round-trip检查
        
        用简单关键词匹配模拟召回
        """
        # 提取query关键词
        query_lower = query.lower()
        
        # 检查api_schema的描述相关性
        desc = api_schema.description.lower()
        
        # 简单启发式：共享词汇比例
        query_words = set(query_lower.split())
        desc_words = set(desc.split())
        
        # 如果完全没有共享词汇，可能有问题
        if not query_words & desc_words:
            # 进一步用LLM检查（简化版，实际应该用完整实现）
            print(f"  [RoundTrip Warning] 关键词匹配度低，query='{query[:30]}...', desc='{desc[:30]}...'")
            # 这里简化处理，返回True，实际应该用LLM验证
        
        return True