"""回溯采集：管理历史数据的回溯采集任务，支持时间范围、断点续做和失败重试。"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BackfillRangeService(BaseModuleService):
    MODULE = "backfill_range"
    ENTITY = "回溯任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "关联源", "时间范围", "数据量估计", "并发度", "跳过空文件", "校验策略", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "关联源", "时间范围"]
    STATUS_ORDER = ["排队中", "采集", "已完成", "已失败"]
    ACTION_RULES = {"发起采集": "采集", "确认完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    # 登记入口的并发保护：同一秒到达的批量登记也必须拿到唯一编号
    _id_lock = threading.Lock()
    _id_seq = 0

    # -- 定位凭证：编号生成、登记注册与冲突解析 --

    def _registry_file(self) -> Path:
        return self._data_dir("checkpoints") / "backfill_registry.json"

    def _load_registry(self) -> dict[str, Any]:
        """登记注册表：定位凭证 -> 原登记（源、时间范围、位点文件），用于历史冲突追溯。"""
        path = self._registry_file()
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and isinstance(data.get("records"), list):
                    return data
            except (json.JSONDecodeError, OSError):
                pass
        return {"records": []}

    def _save_registry(self, registry: dict[str, Any]) -> None:
        with open(self._registry_file(), "w", encoding="utf-8") as f:
            json.dump(registry, f, ensure_ascii=False, indent=2, default=str)

    def _register_credential(self, *, task_id: str, entry_id: Any, source_key: str,
                             time_range: str, checkpoint: str) -> dict[str, Any]:
        """把定位凭证与其原登记绑定入册，历史冲突凭证据此追到原登记。"""
        registry = self._load_registry()
        for rec in registry["records"]:
            if (rec.get("task_id") == task_id and rec.get("entry_id") == entry_id
                    and rec.get("checkpoint") == checkpoint):
                return rec
        rec = {
            "task_id": task_id,
            "entry_id": entry_id,
            "source_key": source_key,
            "time_range": time_range,
            "checkpoint": checkpoint,
            "registered_at": time.time(),
        }
        registry["records"].append(rec)
        self._save_registry(registry)
        return rec

    def _existing_task_ids(self) -> set[str]:
        """已占用的任务编号：内存登记表 + 磁盘位点文件 + 注册表（覆盖进程重启）。"""
        ids = {str(row.get("任务编号")) for row in store.rows(self.MODULE) if row.get("任务编号")}
        cp_dir = self._data_dir("checkpoints")
        for path in cp_dir.glob("backfill_*.json"):
            if path.name == "backfill_registry.json":
                continue
            ids.add(path.stem[len("backfill_"):])
        for rec in self._load_registry()["records"]:
            if rec.get("task_id"):
                ids.add(str(rec["task_id"]))
        return ids

    def _next_task_id(self) -> str:
        """生成唯一任务编号：同一秒并发登记由序号区分，且不与历史凭证冲突。"""
        existing = self._existing_task_ids()
        while True:
            type(self)._id_seq += 1
            candidate = f"BACK-{int(time.time())}-{type(self)._id_seq:04d}"
            if candidate not in existing:
                return candidate

    def _find_task_rows(self, task_id: str) -> list[dict[str, Any]]:
        return [row for row in store.rows(self.MODULE) if str(row.get("任务编号")) == str(task_id)]

    def _find_task_row(self, task_id: str, entry_id: Any = None) -> dict[str, Any] | None:
        """按定位凭证找到对应登记；历史重复编号时按位点内容归属到原登记。"""
        rows = self._find_task_rows(task_id)
        if entry_id is not None:
            for row in rows:
                if int(row.get("id", 0)) == int(entry_id):
                    return row
            return None
        if not rows:
            return None
        if len(rows) == 1:
            return rows[0]
        # 历史冲突：同一编号对应多条登记，位点内容里的身份指向原登记
        cp = self._read_checkpoint(f"backfill_{task_id}")
        if cp:
            if cp.get("entry_id") is not None:
                for row in rows:
                    if int(row.get("id", 0)) == int(cp["entry_id"]):
                        return row
            for row in rows:
                if (row.get("关联源") == cp.get("source_key")
                        and row.get("时间范围") == cp.get("time_range")):
                    return row
        return rows[0]

    def _resolve_checkpoint_name(self, task_id: str, task: dict[str, Any] | None = None) -> str:
        """把定位凭证解析为实际位点文件。

        正常情况即 ``backfill_<任务编号>``；若磁盘上已存在同编号的历史位点且其
        内容身份（源 + 时间范围）属于另一条登记，则该历史凭证保留给原登记可追溯，
        当前任务改用带登记 id 的独立位点，避免误认成另一任务。
        """
        base = f"backfill_{task_id}"
        if task is None:
            task = self._find_task_row(task_id)
        if task is not None:
            # 注册表优先（要求记录身份与当前登记一致，过期记录不误导）
            for rec in self._load_registry()["records"]:
                if (rec.get("task_id") == task_id
                        and rec.get("entry_id") == task.get("id")
                        and rec.get("source_key") == task.get("关联源")
                        and rec.get("time_range") == task.get("时间范围")):
                    return str(rec["checkpoint"])
        cp = self._read_checkpoint(base)
        if cp is None or task is None:
            return base
        cp_entry = cp.get("entry_id")
        if cp_entry is not None:
            owns = int(cp_entry) == int(task.get("id", 0))
        else:
            owns = (str(cp.get("task_id")) == str(task_id)
                    and cp.get("source_key") == task.get("关联源")
                    and cp.get("time_range") == task.get("时间范围"))
        if owns:
            return base
        # 历史冲突凭证：归属原登记，当前任务使用独立位点，双方入册均可追
        owner = self._find_task_row(task_id)
        if owner is not None and int(owner.get("id", 0)) != int(task.get("id", 0)):
            self._register_credential(
                task_id=task_id, entry_id=owner.get("id"),
                source_key=str(cp.get("source_key", "")),
                time_range=str(cp.get("time_range", "")),
                checkpoint=base,
            )
        disambiguated = f"backfill_{task_id}_{task.get('id')}"
        self._register_credential(
            task_id=task_id, entry_id=task.get("id"),
            source_key=str(task.get("关联源", "")),
            time_range=str(task.get("时间范围", "")),
            checkpoint=disambiguated,
        )
        return disambiguated

    # -- 登记与进度查询 --

    def create_backfill(self, source_key: str, time_range: str, concurrency: int = 1) -> dict[str, Any]:
        """创建回溯采集任务：并发登记时每个任务的身份唯一对应真实源和时间范围。"""
        with self._id_lock:
            task_id = self._next_task_id()
            entry, _ = self.create_entry({
                "任务编号": task_id,
                "关联源": source_key,
                "时间范围": time_range,
                "并发度": str(concurrency),
                "数据量估计": "待计算",
                "跳过空文件": "是",
                "校验策略": "完整性校验",
            })
        if entry is None:
            return {}
        cp_name = f"backfill_{task_id}"
        self._write_checkpoint(cp_name, {
            "task_id": task_id, "entry_id": entry.get("id"),
            "source_key": source_key,
            "time_range": time_range, "status": "created", "created_at": time.time(),
            "offset": 0, "line_no": 0,
        })
        self._register_credential(
            task_id=task_id, entry_id=entry.get("id"),
            source_key=source_key, time_range=time_range, checkpoint=cp_name,
        )
        return {**entry, "checkpoint": cp_name}

    def create_backfill_batch(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """批量登记回溯任务：同一批内每个任务拿到唯一定位凭证。"""
        tasks = []
        for item in items:
            entry = self.create_backfill(
                str(item.get("source_key", "")),
                str(item.get("time_range", "")),
                int(item.get("concurrency", 1)),
            )
            if entry:
                tasks.append(entry)
        return tasks

    def get_backfill_progress(self, task_id: str, entry_id: Any = None) -> dict[str, Any]:
        """按定位凭证查看单个任务的已存进度；历史冲突凭证追到原登记。"""
        task = self._find_task_row(task_id, entry_id)
        cp_name = self._resolve_checkpoint_name(task_id, task)
        cp = self._read_checkpoint(cp_name)
        progress = cp if cp else {
            "task_id": task_id, "status": "no_checkpoint", "line_no": 0, "offset": 0,
        }
        return {
            "task_id": task_id,
            "entry_id": task.get("id") if task else None,
            "source_key": (task or {}).get("关联源") or progress.get("source_key", ""),
            "time_range": (task or {}).get("时间范围") or progress.get("time_range", ""),
            "checkpoint": cp_name,
            "progress": progress,
        }

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """任务详情附带按定位凭证解析出的采集进度，并发登记后也不读错。"""
        entry = super().get_entry(entry_id)
        if entry is None:
            return None
        progress = self.get_backfill_progress(str(entry.get("任务编号", "")), entry_id=entry_id)
        return {**entry, "checkpoint_progress": progress}

    # -- 执行与续做 --

    def execute_backfill(self, task_id: str, batch_size: int = 50,
                         interrupt_after: int | None = None,
                         entry_id: Any = None) -> dict[str, Any]:
        """执行回溯采集（模拟历史日志采集）。

        - 定位凭证冲突时按位点内容归属到正确登记，不读错源采集位点；
        - ``interrupt_after`` 模拟中断：部分行已落盘但位点尚未保存即中断；
        - 重复提交续做请求时按行号去重，已经确认的回溯行只计一次。
        """
        task = self._find_task_row(task_id, entry_id)
        if task is None:
            return {"error": f"回溯任务 {task_id} 不存在"}

        source_key = task["关联源"]
        cp_name = self._resolve_checkpoint_name(task_id, task)
        cp = self._read_checkpoint(cp_name) or {
            "task_id": task_id, "entry_id": task.get("id"),
            "source_key": source_key, "time_range": task.get("时间范围"),
            "offset": 0, "line_no": 0, "status": "created",
        }
        cp.setdefault("task_id", task_id)
        cp.setdefault("entry_id", task.get("id"))
        cp.setdefault("source_key", source_key)
        cp.setdefault("time_range", task.get("时间范围"))

        start_line = int(cp.get("line_no", 0))

        # 模拟回溯日志
        base_ts = time.time() - 86400 * 30  # 30天前
        lines = []
        for i in range(batch_size):
            ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(base_ts + (start_line + i) * 3600))
            lines.append(f"{ts} [backfill] INFO source={source_key} task={task_id} record_id={start_line + i + 1}")

        records = [{
            "source_key": source_key, "task_id": task_id, "entry_id": task.get("id"),
            "line_no": start_line + i + 1, "raw": line, "backfilled_at": time.time(),
        } for i, line in enumerate(lines)]

        interrupted = interrupt_after is not None
        if interrupted:
            records = records[:max(0, int(interrupt_after))]

        output_file = self._data_dir("logs") / f"{source_key}_backfill.jsonl"
        # 幂等：已落盘确认的回溯行只计一次，重复续做不重复写
        confirmed_lines = {
            int(r["line_no"]) for r in self._read_jsonl(output_file)
            if str(r.get("task_id")) == str(task_id) and r.get("line_no") is not None
        }
        new_records = [r for r in records if r["line_no"] not in confirmed_lines]
        written = self._write_jsonl(output_file, new_records)

        if interrupted:
            # 中断：行已写出但位点未保存；恢复时靠幂等去重，已确认行仍只计一次
            return {
                "task_id": task_id, "entry_id": task.get("id"), "source_key": source_key,
                "written": written, "interrupted": True,
                "confirmed_lines": start_line, "checkpoint": cp_name,
            }

        confirmed_lines |= {r["line_no"] for r in new_records}
        cp["line_no"] = max(confirmed_lines) if confirmed_lines else start_line
        cp["offset"] = sum(
            len(str(r.get("raw", "")).encode("utf-8")) + 1
            for r in self._read_jsonl(output_file)
            if str(r.get("task_id")) == str(task_id)
        )
        cp["status"] = "in_progress"
        cp["last_batch_at"] = time.time()
        self._write_checkpoint(cp_name, cp)

        if task.get("status") != "采集":
            task["status"] = "采集"

        return {
            "task_id": task_id, "entry_id": task.get("id"), "source_key": source_key,
            "written": written, "skipped_duplicates": len(records) - written,
            "interrupted": False, "total_lines": cp["line_no"], "offset": cp["offset"],
            "checkpoint": cp_name,
        }

    def complete_backfill(self, task_id: str, entry_id: Any = None) -> dict[str, Any]:
        """完成回溯任务并自动触发质量诊断。"""
        task = self._find_task_row(task_id, entry_id)
        cp_name = self._resolve_checkpoint_name(task_id, task)
        cp = self._read_checkpoint(cp_name)
        source_key = ""
        if cp:
            cp["status"] = "completed"
            cp["completed_at"] = time.time()
            self._write_checkpoint(cp_name, cp)
            source_key = cp.get("source_key", "")
        if not source_key and task is not None:
            source_key = str(task.get("关联源", ""))

        result = {"task_id": task_id, "status": "completed", "checkpoint": cp_name, "result": cp}

        # 回溯完成后自动触发质量诊断
        if source_key:
            quality_r = self._trigger_quality_diagnosis(source_key)
            if "error" not in quality_r:
                result["auto_quality"] = quality_r
                # 质量诊断后触发告警评估
                alert_r = self._trigger_alert_evaluation(source_key, {
                    "parse_success_rate": quality_r.get("parse_success_rate", 100),
                    "field_completeness": quality_r.get("field_completeness", {}).get("overall", 100),
                    "source_key": source_key,
                })
                if "error" not in alert_r:
                    result["auto_alert"] = alert_r

        return result
