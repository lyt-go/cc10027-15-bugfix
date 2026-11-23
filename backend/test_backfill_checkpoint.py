"""回溯批量任务定位凭证回归测试。

覆盖：
1. 同一秒并发登记产生唯一编号，身份唯一对应真实源和时间范围；
2. 历史冲突凭证追到原登记，不误认成另一任务；
3. 重复提交续做请求时，已经确认的回溯行只计一次；
4. 采集位点消费定位凭证，把每份已存进度交给正确的回溯采集输入，历史范围仍可查；
5. 验收流程：批量创建两个时间段 -> 逐条查看进度 -> 第二个编号开跑并中断一次
   -> 恢复后写出的日志全部归到正确来源。
"""
import json
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.services.backfill_range import BackfillRangeService
from app.services.collect_checkpoint import CollectCheckpointService
from app.services.base import DATA_ROOT
from app.store import store


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _source_rows(task_id: str, source_key: str) -> list[dict]:
    return [
        r for r in _read_jsonl(DATA_ROOT / "logs" / f"{source_key}_backfill.jsonl")
        if str(r.get("task_id")) == str(task_id)
    ]


def test_same_second_concurrent_registration_unique_ids():
    """同一秒并发登记：每个任务编号唯一，且唯一对应真实源和时间范围。"""
    svc = BackfillRangeService()
    results = []
    errors = []

    def register(idx: int):
        try:
            results.append(svc.create_backfill(f"LOG_-01{idx:02d}", f"2026-11-{idx + 1:02d}~2026-11-{idx + 1:02d}"))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=register, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    ids = [r["任务编号"] for r in results]
    assert len(set(ids)) == len(ids) == 12, f"同一秒并发登记产生重复编号: {ids}"
    entry_ids = [r["id"] for r in results]
    assert len(set(entry_ids)) == len(entry_ids), "并发登记产生重复登记id"
    for r in results:
        cp = svc._read_checkpoint(f"backfill_{r['任务编号']}")
        assert cp["source_key"] == r["关联源"], "凭证绑定的源与登记不一致"
        assert cp["time_range"] == r["时间范围"], "凭证绑定的时间范围与登记不一致"


def test_batch_create_and_individual_progress():
    """批量创建两个时间段，逐条查看对应进度。"""
    svc = BackfillRangeService()
    cp_svc = CollectCheckpointService()
    tasks = svc.create_backfill_batch([
        {"source_key": "LOG_-0201", "time_range": "2026-11-16~2026-11-16"},
        {"source_key": "LOG_-0202", "time_range": "2026-11-17~2026-11-17"},
    ])
    assert len(tasks) == 2
    id_a, id_b = tasks[0]["任务编号"], tasks[1]["任务编号"]
    assert id_a != id_b, "同一批次两个任务编号必须不同"

    svc.execute_backfill(id_a, batch_size=5)
    svc.execute_backfill(id_b, batch_size=8)

    pa = cp_svc.get_backfill_checkpoint(id_a)
    pb = cp_svc.get_backfill_checkpoint(id_b)
    assert pa["source_key"] == "LOG_-0201" and pa["time_range"] == "2026-11-16~2026-11-16"
    assert pb["source_key"] == "LOG_-0202" and pb["time_range"] == "2026-11-17~2026-11-17"
    assert pa["progress"]["line_no"] == 5, f"A 进度应各归各: {pa}"
    assert pb["progress"]["line_no"] == 8, f"B 进度应各归各: {pb}"

    # 任务详情读到的是自己的进度
    detail_a = svc.get_entry(tasks[0]["id"])
    detail_b = svc.get_entry(tasks[1]["id"])
    assert detail_a["checkpoint_progress"]["progress"]["line_no"] == 5
    assert detail_b["checkpoint_progress"]["progress"]["line_no"] == 8


def test_legacy_conflicting_credential_traces_to_original():
    """历史冲突凭证：同一编号两条登记，位点追到原登记，另一任务获得独立位点。"""
    svc = BackfillRangeService()
    conflict_id = f"BACK-LEGACY-{int(time.time() * 1000)}"

    # 模拟修复前的历史数据：同一编号登记了 A源周一 和 B源周二 两条任务
    entry_a, _ = svc.create_entry({
        "任务编号": conflict_id, "关联源": "LOG_-0301", "时间范围": "2026-11-16~2026-11-16",
        "数据量估计": "待计算", "并发度": "1", "跳过空文件": "是", "校验策略": "完整性校验",
    })
    entry_b, _ = svc.create_entry({
        "任务编号": conflict_id, "关联源": "LOG_-0302", "时间范围": "2026-11-17~2026-11-17",
        "数据量估计": "待计算", "并发度": "1", "跳过空文件": "是", "校验策略": "完整性校验",
    })
    # 原登记 A 已写一批，位点内容身份属于 A
    svc._write_checkpoint(f"backfill_{conflict_id}", {
        "task_id": conflict_id, "source_key": "LOG_-0301",
        "time_range": "2026-11-16~2026-11-16", "status": "in_progress",
        "offset": 500, "line_no": 20, "created_at": time.time(),
    })

    # 原登记 A：继续读到自己的历史进度（追到原登记）
    pa = svc.get_backfill_progress(conflict_id, entry_id=entry_a["id"])
    assert pa["checkpoint"] == f"backfill_{conflict_id}"
    assert pa["progress"]["line_no"] == 20
    assert pa["source_key"] == "LOG_-0301"

    # 另一任务 B：不得误认 A 的凭证，获得独立位点从零开始
    pb = svc.get_backfill_progress(conflict_id, entry_id=entry_b["id"])
    assert pb["checkpoint"] != f"backfill_{conflict_id}"
    assert pb["progress"]["line_no"] == 0
    assert pb["source_key"] == "LOG_-0302"

    # B 从断点续跑：日志写到 B 自己的来源，A 的位点不被污染
    rb = svc.execute_backfill(conflict_id, batch_size=6, entry_id=entry_b["id"])
    assert rb["source_key"] == "LOG_-0302"
    assert rb["total_lines"] == 6
    b_rows = _source_rows(conflict_id, "LOG_-0302")
    assert len(b_rows) == 6 and all(r["source_key"] == "LOG_-0302" for r in b_rows)
    assert not _source_rows(conflict_id, "LOG_-0301"), "B 的日志不得混入 A 的输出文件"

    # A 的历史进度仍保持 20 行，未因 B 的续跑被覆盖
    pa_after = svc.get_backfill_progress(conflict_id, entry_id=entry_a["id"])
    assert pa_after["progress"]["line_no"] == 20

    # 双方凭证均入册，可追到各自原登记
    registry = svc._load_registry()["records"]
    rec_a = [r for r in registry if r.get("task_id") == conflict_id and r.get("entry_id") == entry_a["id"]]
    rec_b = [r for r in registry if r.get("task_id") == conflict_id and r.get("entry_id") == entry_b["id"]]
    assert rec_a and rec_a[0]["checkpoint"] == f"backfill_{conflict_id}"
    assert rec_b and rec_b[0]["checkpoint"] != f"backfill_{conflict_id}"


def test_resume_counts_confirmed_lines_once():
    """重复提交续做请求：已经确认的回溯行只计一次。"""
    svc = BackfillRangeService()
    task = svc.create_backfill("LOG_-0401", "2026-11-17~2026-11-17")
    task_id = task["任务编号"]

    # 中断一次：6 行已落盘但位点来不及保存
    r1 = svc.execute_backfill(task_id, batch_size=10, interrupt_after=6)
    assert r1["interrupted"] and r1["written"] == 6
    assert svc.get_backfill_progress(task_id)["progress"]["line_no"] == 0

    # 恢复续做：本批 10 行中 6 行已确认，只新写 4 行
    r2 = svc.execute_backfill(task_id, batch_size=10)
    assert r2["written"] == 4 and r2["skipped_duplicates"] == 6
    assert r2["total_lines"] == 10

    # 位点已前进，再次续做正常写新行
    r3 = svc.execute_backfill(task_id, batch_size=10)
    assert r3["written"] == 10 and r3["total_lines"] == 20

    rows = _source_rows(task_id, "LOG_-0401")
    line_nos = [r["line_no"] for r in rows]
    assert len(line_nos) == len(set(line_nos)) == 20, "已确认行不得重复落盘"


def test_checkpoint_module_consumes_credential():
    """采集位点消费回溯定位凭证：每份已存进度归属正确任务，历史范围仍可查。"""
    svc = BackfillRangeService()
    cp_svc = CollectCheckpointService()
    tasks = svc.create_backfill_batch([
        {"source_key": "LOG_-0501", "time_range": "2026-11-16~2026-11-16"},
        {"source_key": "LOG_-0502", "time_range": "2026-11-17~2026-11-17"},
    ])
    id_a, id_b = tasks[0]["任务编号"], tasks[1]["任务编号"]
    svc.execute_backfill(id_a, batch_size=3)
    svc.execute_backfill(id_b, batch_size=7)

    all_cps = cp_svc.list_backfill_checkpoints()
    mine = {c["task_id"]: c for c in all_cps if c["task_id"] in (id_a, id_b)}
    assert mine[id_a]["source_key"] == "LOG_-0501" and mine[id_a]["line_no"] == 3
    assert mine[id_b]["source_key"] == "LOG_-0502" and mine[id_b]["line_no"] == 7
    # 历史范围仍可查
    assert mine[id_a]["time_range"] == "2026-11-16~2026-11-16"
    assert mine[id_b]["time_range"] == "2026-11-17~2026-11-17"
    assert mine[id_a]["entry_id"] == tasks[0]["id"]
    assert mine[id_b]["entry_id"] == tasks[1]["id"]


def test_batch_resume_attribution_acceptance_flow():
    """验收流程：批量创建两个时间段 -> 逐条查看 -> 第二个编号开跑中断一次
    -> 恢复后写出的日志全部归到正确来源。"""
    svc = BackfillRangeService()
    cp_svc = CollectCheckpointService()
    src_a, src_b = "LOG_-0601", "LOG_-0602"
    range_a, range_b = "2026-11-16~2026-11-16", "2026-11-17~2026-11-17"

    # 批量创建两个时间段
    tasks = svc.create_backfill_batch([
        {"source_key": src_a, "time_range": range_a},
        {"source_key": src_b, "time_range": range_b},
    ])
    id_a, id_b = tasks[0]["任务编号"], tasks[1]["任务编号"]
    assert id_a != id_b

    # 逐条查看对应进度
    pa = cp_svc.get_backfill_checkpoint(id_a)
    pb = cp_svc.get_backfill_checkpoint(id_b)
    assert pa["source_key"] == src_a and pa["time_range"] == range_a
    assert pb["source_key"] == src_b and pb["time_range"] == range_b

    # A 已写一批
    ra = svc.execute_backfill(id_a, batch_size=10)
    assert ra["written"] == 10 and ra["total_lines"] == 10

    # 从第二个编号开跑并中断一次
    rb1 = svc.execute_backfill(id_b, batch_size=10, interrupt_after=4)
    assert rb1["interrupted"] and rb1["written"] == 4

    # 恢复续做（重复提交也只计一次），再续一批
    rb2 = svc.execute_backfill(id_b, batch_size=10)
    rb3 = svc.execute_backfill(id_b, batch_size=10)
    assert rb2["skipped_duplicates"] == 4, "中断前已确认的 4 行只计一次"
    assert rb3["total_lines"] == 20

    # 核对恢复后写出的日志全部归到正确来源
    logs_a = _read_jsonl(DATA_ROOT / "logs" / f"{src_a}_backfill.jsonl")
    logs_b = _read_jsonl(DATA_ROOT / "logs" / f"{src_b}_backfill.jsonl")
    a_rows = [r for r in logs_a if r["task_id"] == id_a]
    b_rows = [r for r in logs_b if r["task_id"] == id_b]
    assert len(a_rows) == 10 and all(r["source_key"] == src_a for r in a_rows)
    assert all(r["source_key"] == src_b for r in b_rows)
    assert not any(r["task_id"] == id_b for r in logs_a), "B 的日志不得混入 A 的文件"
    assert not any(r["task_id"] == id_a for r in logs_b), "A 的日志不得混入 B 的文件"
    assert sorted(r["line_no"] for r in b_rows) == list(range(1, 21))

    # 位点进度各归各的任务
    assert cp_svc.get_backfill_checkpoint(id_a)["progress"]["line_no"] == 10
    assert cp_svc.get_backfill_checkpoint(id_b)["progress"]["line_no"] == 20


def test_pipeline_batch_and_resume():
    """管道层：批量登记 + 位点消费凭证续跑。"""
    from app.services.pipeline import CrossModulePipeline

    p = CrossModulePipeline()
    batch = p.backfill_batch_pipeline([
        {"source_key": "LOG_-0701", "time_range": "2026-11-16~2026-11-16"},
        {"source_key": "LOG_-0702", "time_range": "2026-11-17~2026-11-17"},
    ])
    assert batch["pipeline"] == "backfill_batch"
    ids = [t["任务编号"] for t in batch["tasks"]]
    assert len(set(ids)) == 2
    assert [pr["source_key"] for pr in batch["progresses"]] == ["LOG_-0701", "LOG_-0702"]

    resume = p.backfill_resume_pipeline(ids[1], batch_size=5)
    assert resume["pipeline"] == "backfill_resume"
    assert resume["resume"]["source_key"] == "LOG_-0702"
    assert resume["resume"]["total_lines"] == 5
    rows = _source_rows(ids[1], "LOG_-0702")
    assert len(rows) == 5 and all(r["source_key"] == "LOG_-0702" for r in rows)
