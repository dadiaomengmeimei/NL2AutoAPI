"""
Version management module — binlog-style operation logging.

Every mutation (insert / update / delete) on a JSONL dataset is recorded
as an operation entry.  An initial snapshot is taken when the file is first
registered.  At any point a caller can request a "point-in-time restore"
that replays operations up to the given timestamp.
"""

import json
import os
import shutil
from datetime import datetime
from typing import Optional, Any


_JSON_DEFAULT = lambda obj: obj.isoformat() if isinstance(obj, (datetime,)) else str(obj)


class VersionManager:
    """Manages binlog-style operation logs for JSONL data files."""

    def __init__(self, base_dir: str):
        """
        Args:
            base_dir: directory where .binlog and .snapshot files are stored,
                      typically output/<table>/.versions/
        """
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    # ------------------------------------------------------------------ paths
    def _binlog_path(self, dataset_name: str) -> str:
        return os.path.join(self.base_dir, f"{dataset_name}.binlog.jsonl")

    def _snapshot_path(self, dataset_name: str) -> str:
        return os.path.join(self.base_dir, f"{dataset_name}.snapshot.jsonl")

    # ----------------------------------------------------------- snapshot init
    def ensure_snapshot(self, dataset_name: str, source_path: str):
        """Take an initial snapshot if one does not yet exist."""
        snap = self._snapshot_path(dataset_name)
        if os.path.exists(snap):
            return
        if os.path.exists(source_path):
            shutil.copy2(source_path, snap)
        else:
            # empty snapshot
            with open(snap, "w", encoding="utf8") as f:
                pass

    def refresh_snapshot(self, dataset_name: str, source_path: str):
        """Overwrite the snapshot with current file content."""
        snap = self._snapshot_path(dataset_name)
        if os.path.exists(source_path):
            shutil.copy2(source_path, snap)
        else:
            with open(snap, "w", encoding="utf8") as f:
                pass

    # --------------------------------------------------------- log operations
    def log_operation(
        self,
        dataset_name: str,
        op_type: str,
        record: Optional[dict] = None,
        old_record: Optional[dict] = None,
        meta: Optional[dict] = None,
    ):
        """
        Append an operation entry to the binlog.

        op_type: "insert" | "update" | "delete"
        record:  the new / current record (None for delete)
        old_record: the previous record (for update / delete)
        meta:    arbitrary metadata (reviewer, source, etc.)
        """
        entry = {
            "ts": datetime.now().isoformat(),
            "op": op_type,
            "dataset": dataset_name,
            "record": record,
            "old_record": old_record,
            "meta": meta or {},
        }
        path = self._binlog_path(dataset_name)
        with open(path, "a", encoding="utf8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=_JSON_DEFAULT) + "\n")

    # ----------------------------------------------------------- read binlog
    def read_binlog(self, dataset_name: str) -> list[dict]:
        path = self._binlog_path(dataset_name)
        if not os.path.exists(path):
            return []
        entries = []
        with open(path, "r", encoding="utf8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return entries

    # -------------------------------------------------------- point-in-time restore
    def restore_to_timestamp(self, dataset_name: str, target_ts: str) -> list[dict]:
        """
        Replay operations on top of the initial snapshot up to *target_ts*
        and return the resulting list of records.

        target_ts: ISO-8601 timestamp string, e.g. "2026-03-18T10:30:00"
        """
        # 1) load snapshot
        snap = self._snapshot_path(dataset_name)
        records: list[dict] = []
        if os.path.exists(snap):
            with open(snap, "r", encoding="utf8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        # 2) replay binlog up to target_ts
        entries = self.read_binlog(dataset_name)
        for entry in entries:
            if entry.get("ts", "") > target_ts:
                break
            op = entry.get("op")
            rec = entry.get("record")
            old = entry.get("old_record")

            if op == "insert" and rec:
                records.append(rec)
            elif op == "update" and rec:
                # find and replace old record
                idx = self._find_record_index(records, old or rec)
                if idx is not None:
                    records[idx] = rec
                else:
                    records.append(rec)
            elif op == "delete":
                idx = self._find_record_index(records, old or rec)
                if idx is not None:
                    records.pop(idx)

        return records

    def write_restored(self, dataset_name: str, target_ts: str, dest_path: str):
        """Convenience: restore and write to a JSONL file."""
        records = self.restore_to_timestamp(dataset_name, target_ts)
        os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
        with open(dest_path, "w", encoding="utf8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, default=_JSON_DEFAULT) + "\n")
        return len(records)

    # ----------------------------------------------------------- helpers
    @staticmethod
    def _find_record_index(records: list[dict], target: Optional[dict]) -> Optional[int]:
        """Locate a record by matching SQL + query (best effort)."""
        if not target:
            return None

        def _key(r: dict):
            api = r.get("api_schema") or {}
            sql = (api.get("bound_sql") or r.get("sql") or "").strip().lower()
            query = (r.get("query") or "").strip().lower()
            return (sql, query)

        tgt_key = _key(target)
        for i, rec in enumerate(records):
            if _key(rec) == tgt_key:
                return i
        return None

    # ------------------------------------------------ summary / stats
    def get_summary(self, dataset_name: str) -> dict:
        """Return a human-readable summary of the version history."""
        entries = self.read_binlog(dataset_name)
        if not entries:
            return {"total_ops": 0, "first_ts": None, "last_ts": None, "ops": {}}
        ops_count: dict[str, int] = {}
        for e in entries:
            op = e.get("op", "unknown")
            ops_count[op] = ops_count.get(op, 0) + 1
        return {
            "total_ops": len(entries),
            "first_ts": entries[0].get("ts"),
            "last_ts": entries[-1].get("ts"),
            "ops": ops_count,
        }
