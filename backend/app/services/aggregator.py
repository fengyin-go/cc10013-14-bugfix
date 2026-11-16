"""数据聚合业务规则：状态流转、批次/片段台账与结果可消费判定都收在这里。

聚合任务下面挂一个聚合数据集，数据集由若干必需片段组成：
- 片段全部完成前，数据集保持「未完成」，结果不可消费；
- 失败批次保留待处理标记、可重试输入和已完成片段，并写入聚合任务待办台账；
- 只有完整成功的数据集才允许归档，失败批次不能越过状态直接归档。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "aggregator"
REQUIRED_FIELDS = ["任务编号", "输入端点列表", "合并策略"]
OPTIONAL_FIELDS = ["去重键", "排序字段", "输出格式", "批次大小", "调度周期"]
STATUS_ORDER = ["草稿", "待执行", "运行中", "已完成", "已失败"]
ACTION_RULES = {"提交执行": "待执行", "启动聚合": "运行中", "终止聚合": "已失败", "归档结果": "已完成"}
NEGATIVE_ACTIONS = ["终止聚合"]

FRAGMENT_DONE = "已完成"
FRAGMENT_FAILED = "失败"
DATASET_INCOMPLETE = "未完成"
DATASET_COMPLETE = "已完成"
ARCHIVED = "已归档"
UNARCHIVED = "未归档"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _text(value: Any) -> str:
    return str(value or "").strip()


class AggregatorService:
    def __init__(self) -> None:
        # 服务启动时按数据集的实际片段情况恢复任务状态：
        # 中止后遗留的「无待处理」和错误的「已归档」都会在这里被纠正。
        self._restore_states()

    # ------------------------------------------------------------------ 查询

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for row in rows:
            self._sync(row)
            self._annotate(row)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is not None:
            self._sync(entry)
            self._annotate(entry)
        return entry

    # ------------------------------------------------------------------ 登记

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not _text(values.get(field))]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        # 去重键与输出格式属于聚合结果的一部分，登记时一并落库，避免结果缺字段。
        for field in OPTIONAL_FIELDS:
            if field in values and values.get(field) is not None:
                entry[field] = values.get(field)
        entry.setdefault("去重键", "id")
        entry.setdefault("输出格式", "json")
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["归档状态"] = UNARCHIVED
        rows.append(entry)
        return entry, []

    # ------------------------------------------------------------------ 动作

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"聚合任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据聚合可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        current = self.get_entry(entry_id) or entry

        if action == "终止聚合":
            dataset = current.get("数据集")
            if isinstance(dataset, dict) and not list(dataset.get("remaining_fragments") or []):
                return None, "全部必需片段已齐备，聚合任务没有中断中的批次；如需留存请直接归档结果"
            self._mark_failed(current, "聚合任务被终止，批次在运行中断")
            entry, message = current, "聚合任务已终止，失败批次已写入聚合任务待办台账；可从缺失片段重试"
            return entry, message

        if action == "归档结果":
            dataset = current.get("数据集")
            if isinstance(dataset, dict):
                remaining = list(dataset.get("remaining_fragments") or [])
                if remaining:
                    return None, (
                        f"聚合数据集 {dataset.get('dataset_id')} 仍有缺失片段："
                        f"{'、'.join(remaining)}，补齐并完整成功后才能归档"
                    )
            if current.get("status") == "已失败":
                return None, "失败批次不能越过状态直接归档，请先补齐缺失片段并重试成功"
            current["status"] = "已完成"
            current["pending"] = False
            current["abnormal"] = False
            if isinstance(dataset, dict):
                dataset["archived"] = True
                dataset["consumable"] = not dataset.get("remaining_fragments")
                dataset["updated_at"] = _now()
                for batch in dataset.get("batches", []):
                    batch["archived"] = True
                    batch["pending"] = False
            current["归档状态"] = ARCHIVED
            self._annotate(current)
            return current, "聚合结果已归档"

        current["status"] = target
        current["pending"] = target != "已完成"
        current["abnormal"] = action in NEGATIVE_ACTIONS
        self._sync(current)
        self._annotate(current)
        return current, f"聚合任务已{action}"

    # -------------------------------------------------------------- 批次详情

    def batch_detail(self, entry_id: int, batch_id: str) -> dict[str, Any] | None:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        dataset = entry.get("数据集")
        if not isinstance(dataset, dict):
            return {
                "ok": True,
                "message": "该聚合任务还没有聚合数据集批次",
                "entry": entry,
                "batch": None,
                "failure_reason": entry.get("失败原因"),
                "abnormal_fragments": [],
                "completed_fragments": [],
                "remaining_fragments": [],
                "fragment_details": [],
                "retryable_input": {},
                "empty_state": {
                    "visible": True,
                    "message": "暂无批次记录，可先启动聚合生成批次",
                },
                "recovery": {"available": False, "action": "retry", "message": "没有可恢复的缺失片段"},
                "dataset_status": entry.get("数据集状态"),
                "consumable": bool(entry.get("可消费")),
            }

        batches = dataset.get("batches") or []
        batch = None
        if batch_id in ("latest", "", "current"):
            batch = batches[-1] if batches else None
        else:
            batch = next((item for item in batches if item.get("batch_id") == batch_id), None)

        remaining = list(dataset.get("remaining_fragments") or [])
        completed = list(dataset.get("completed_fragments") or [])
        if batch is None:
            failure_reason = entry.get("失败原因") or "批次尚未建立"
            abnormal = remaining
            empty_visible = True
            recovery_available = bool(remaining)
            recovery_message = (
                f"批次记录缺失，可从剩余片段 {'、'.join(remaining)} 重新建立并恢复"
                if remaining
                else "批次记录缺失，聚合数据集已完整，可直接归档"
            )
            retryable = self._retryable_input(entry, dataset, remaining, None)
        else:
            failure_reason = batch.get("failure_reason") or entry.get("失败原因")
            remaining = list(batch.get("remaining_fragments") or remaining)
            completed = list(batch.get("completed_fragments") or completed)
            abnormal = list(batch.get("abnormal_fragments") or remaining)
            empty_visible = not bool(remaining)
            recovery_available = bool(remaining)
            recovery_message = (
                f"从缺失片段 {'、'.join(remaining)} 重试，补齐前批次保持失败状态"
                if remaining
                else "全部必需片段已齐备，结果集可消费"
            )
            retryable = batch.get("retryable_input") or self._retryable_input(
                entry, dataset, remaining, batch
            )

        fragments = dataset.get("fragments") or []
        fragment_details = [
            fragment for fragment in fragments
            if fragment.get("fragment_id") in (set(remaining) | set(completed) | set(abnormal))
        ]
        return {
            "ok": True,
            "message": failure_reason or "批次详情已读取",
            "entry": entry,
            "batch": batch,
            "batch_id": batch.get("batch_id") if batch else batch_id,
            "failure_reason": failure_reason,
            "abnormal_fragments": abnormal,
            "completed_fragments": completed,
            "remaining_fragments": remaining,
            "fragment_details": fragment_details,
            "retryable_input": retryable,
            "empty_state": {
                "visible": empty_visible,
                "message": "全部必需片段已齐备" if empty_visible else "批次中断，仍存在缺失片段",
            },
            "recovery": {
                "available": recovery_available,
                "action": "retry",
                "message": recovery_message,
            },
            "next_step": recovery_message,
            "dataset_status": dataset.get("status"),
            "consumable": bool(dataset.get("consumable")),
        }

    # -------------------------------------------------------------- 批次重试

    def retry_batch(
        self,
        entry_id: int,
        batch_id: str,
        values: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return {"ok": False, "message": f"聚合任务 {entry_id} 不存在或已归档"}
        dataset = entry.get("数据集")
        if not isinstance(dataset, dict):
            return {"ok": False, "message": "该聚合任务没有可重试的聚合数据集批次", "entry": entry}

        batches = dataset.setdefault("batches", [])
        if batch_id in ("latest", "", "current"):
            batch = batches[-1] if batches else None
        else:
            batch = next((item for item in batches if item.get("batch_id") == batch_id), None)

        if batch is None:
            # 批次记录丢失时按当前缺失片段重建一个可重试批次，而不是让恢复入口失效。
            batch = self._new_batch(entry, dataset)
            batches.append(batch)

        remaining = list(batch.get("remaining_fragments") or dataset.get("remaining_fragments") or [])
        provided = self._provided_fragments(values or {}, remaining)
        fragments = dataset.get("fragments") or []
        now = _now()

        for fragment_id in remaining:
            fragment = next(
                (item for item in fragments if item.get("fragment_id") == fragment_id), None
            )
            summary = provided.get(fragment_id)
            if fragment is None:
                fragment = {
                    "fragment_id": fragment_id,
                    "source": batch.get("retryable_input", {}).get("endpoint"),
                    "source_priority": 20,
                    "version": 0,
                }
                fragments.append(fragment)
            if summary:
                fragment["status"] = FRAGMENT_DONE
                fragment["summary"] = summary
                fragment["abnormal"] = False
                fragment["failure_reason"] = None
                fragment["version"] = int(fragment.get("version") or 0) + 1
                fragment["updated_at"] = now
            else:
                fragment["status"] = FRAGMENT_FAILED
                fragment["summary"] = fragment.get("summary")
                fragment["abnormal"] = True
                fragment.setdefault("failure_reason", "来源未返回片段内容，重试后仍缺失")
                fragment["updated_at"] = now
        dataset["fragments"] = fragments
        batch["retry_count"] = int(batch.get("retry_count") or 0) + 1
        batch["last_retry_at"] = now
        batch["updated_at"] = now

        self._sync(entry)
        still_missing = list(dataset.get("remaining_fragments") or [])

        if still_missing:
            batch["status"] = "已失败"
            batch["pending"] = True
            batch["abnormal"] = True
            batch["archived"] = False
            batch["failure_reason"] = batch.get("failure_reason") or "重试后仍有片段缺失"
            batch["abnormal_fragments"] = still_missing
            batch["completed_fragments"] = list(dataset.get("completed_fragments") or [])
            batch["remaining_fragments"] = still_missing
            batch["retryable_input"] = self._retryable_input(entry, dataset, still_missing, batch)
            entry["status"] = "已失败"
            entry["pending"] = True
            entry["abnormal"] = True
            entry["失败原因"] = batch["failure_reason"]
            self._annotate(entry)
            next_step = (
                f"仍缺片段：{'、'.join(still_missing)}；请补齐后再次重试，补齐前结果集保持未完成且不可消费"
            )
            return {
                "ok": False,
                "message": f"批次重试未完成，{next_step}",
                "next_step": next_step,
                "entry": entry,
                "batch": batch,
                "remaining_fragments": still_missing,
                "completed_fragments": list(dataset.get("completed_fragments") or []),
                "consumable": False,
                "dataset_status": dataset.get("status"),
            }

        # 全部必需片段齐备：结果集转为可消费，但归档仍需显式执行。
        dataset["status"] = DATASET_COMPLETE
        dataset["consumable"] = True
        dataset["archived"] = False
        dataset["updated_at"] = now
        batch["status"] = "已完成"
        batch["pending"] = False
        batch["abnormal"] = False
        batch["archived"] = False
        batch["completed_fragments"] = list(dataset.get("completed_fragments") or [])
        batch["remaining_fragments"] = []
        batch["abnormal_fragments"] = []
        batch["retryable_input"] = {}
        batch.setdefault("previous_failure_reason", batch.get("failure_reason"))
        batch["failure_reason"] = None
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["失败原因"] = None
        entry["归档状态"] = UNARCHIVED
        self._annotate(entry)
        next_step = "全部必需片段已齐备，结果集已可消费；如需长期留存，请执行归档结果"
        return {
            "ok": True,
            "message": next_step,
            "next_step": next_step,
            "entry": entry,
            "batch": batch,
            "remaining_fragments": [],
            "completed_fragments": list(dataset.get("completed_fragments") or []),
            "consumable": True,
            "dataset_status": dataset.get("status"),
        }

    # ------------------------------------------------------------ 来源更正冲突

    def add_correction(
        self,
        entry_id: int,
        fragment_id: str,
        values: dict[str, Any],
    ) -> dict[str, Any]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return {"ok": False, "message": f"聚合任务 {entry_id} 不存在或已归档"}
        dataset = entry.get("数据集")
        if not isinstance(dataset, dict):
            return {"ok": False, "message": "该聚合任务没有聚合数据集，无法接收片段更正", "entry": entry}
        fragments = dataset.get("fragments") or []
        fragment = next((item for item in fragments if item.get("fragment_id") == fragment_id), None)
        if fragment is None:
            return {"ok": False, "message": f"片段 {fragment_id} 不存在", "entry": entry}

        summary = _text(values.get("summary") or values.get("摘要") or values.get("content"))
        if not summary:
            return {"ok": False, "message": "来源更正缺少片段摘要，未写入", "entry": entry}

        existing_priority = int(fragment.get("source_priority") or 0)
        raw_priority = values.get("source_priority", values.get("priority", values.get("优先级")))
        incoming_priority = existing_priority if raw_priority is None else int(raw_priority)
        existing_version = int(fragment.get("version") or 0)
        raw_version = values.get("version", values.get("版本"))
        incoming_version = (
            existing_version + 1 if raw_version is None else int(raw_version)
        )
        conflict = {
            "fragment_id": fragment_id,
            "existing_summary": fragment.get("summary"),
            "incoming_summary": summary,
            "existing_priority": existing_priority,
            "incoming_priority": incoming_priority,
            "existing_version": existing_version,
            "incoming_version": incoming_version,
            "received_at": _now(),
        }

        if incoming_priority < existing_priority:
            conflict["resolution"] = "保留原片段：来源更正优先级较低"
            fragment.setdefault("conflicts", []).append(conflict)
            return {
                "ok": False,
                "message": (
                    f"来源更正被拒绝：{fragment_id} 现有来源优先级 {existing_priority} "
                    f"高于更正来源 {incoming_priority}，保留原摘要"
                ),
                "conflict": conflict,
                "fragment": fragment,
                "entry": entry,
                "consumable": bool(dataset.get("consumable")),
            }

        if incoming_priority == existing_priority and incoming_version <= existing_version:
            conflict["resolution"] = "保留原片段：同优先级下来源版本未更新"
            fragment.setdefault("conflicts", []).append(conflict)
            return {
                "ok": False,
                "message": (
                    f"来源更正被拒绝：{fragment_id} 同优先级下版本 {incoming_version} "
                    f"不高于现有版本 {existing_version}，保留原摘要"
                ),
                "conflict": conflict,
                "fragment": fragment,
                "entry": entry,
                "consumable": bool(dataset.get("consumable")),
            }

        conflict["resolution"] = (
            "按来源优先级采纳更正"
            if incoming_priority > existing_priority
            else "同优先级按来源版本采纳更正"
        )
        fragment["summary"] = summary
        fragment["source_priority"] = incoming_priority
        fragment["version"] = incoming_version
        fragment["updated_at"] = _now()
        fragment["status"] = FRAGMENT_DONE
        fragment["abnormal"] = False
        fragment.setdefault("conflicts", []).append(conflict)
        dataset["updated_at"] = fragment["updated_at"]
        self._sync(entry)
        self._annotate(entry)
        return {
            "ok": True,
            "message": f"来源更正已按优先级规则采纳：{conflict['resolution']}",
            "conflict": conflict,
            "fragment": fragment,
            "entry": entry,
            "remaining_fragments": list(dataset.get("remaining_fragments") or []),
            "consumable": bool(dataset.get("consumable")),
        }

    # ------------------------------------------------------------ 结果消费门禁

    def get_result(self, entry_id: int) -> dict[str, Any] | None:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        dataset = entry.get("数据集")
        if not isinstance(dataset, dict):
            return {
                "ok": False,
                "consumable": False,
                "message": "该聚合任务还没有可消费的聚合数据集",
                "entry": entry,
            }
        remaining = list(dataset.get("remaining_fragments") or [])
        dedup_key = dataset.get("dedup_key") or entry.get("去重键") or "id"
        output_format = dataset.get("output_format") or entry.get("输出格式") or "json"
        if remaining:
            return {
                "ok": False,
                "consumable": False,
                "message": (
                    f"聚合数据集 {dataset.get('dataset_id')} 尚未完成，缺失片段："
                    f"{'、'.join(remaining)}，任何消费方都不能读取成品"
                ),
                "dataset_id": dataset.get("dataset_id"),
                "dataset_status": DATASET_INCOMPLETE,
                "remaining_fragments": remaining,
                "completed_fragments": list(dataset.get("completed_fragments") or []),
                "dedup_key": dedup_key,
                "output_format": output_format,
                "去重键": dedup_key,
                "输出格式": output_format,
            }

        fragments = [
            fragment for fragment in (dataset.get("fragments") or [])
            if fragment.get("fragment_id") in set(dataset.get("completed_fragments") or [])
        ]
        records: dict[str, dict[str, Any]] = {}
        for fragment in fragments:
            summary = fragment.get("summary")
            record = {
                dedup_key: summary,
                "summary": summary,
                "摘要": summary,
                "fragment_id": fragment.get("fragment_id"),
                "source": fragment.get("source"),
                "source_priority": fragment.get("source_priority"),
                "version": fragment.get("version"),
                "updated_at": fragment.get("updated_at"),
            }
            key = str(summary)
            previous = records.get(key)
            if previous is None or (
                int(record.get("source_priority") or 0),
                int(record.get("version") or 0),
                str(record.get("updated_at") or ""),
            ) > (
                int(previous.get("source_priority") or 0),
                int(previous.get("version") or 0),
                str(previous.get("updated_at") or ""),
            ):
                records[key] = record
        items = list(records.values())
        sort_field = entry.get("排序字段")
        if sort_field:
            items.sort(key=lambda item: str(item.get(sort_field) or ""))
        return {
            "ok": True,
            "consumable": True,
            "message": "聚合结果已就绪，可以消费",
            "dataset_id": dataset.get("dataset_id"),
            "dataset_status": DATASET_COMPLETE,
            "archived": bool(dataset.get("archived")),
            "dedup_key": dedup_key,
            "output_format": output_format,
            "去重键": dedup_key,
            "输出格式": output_format,
            "items": items,
            "total": len(items),
            "completed_fragments": list(dataset.get("completed_fragments") or []),
            "remaining_fragments": [],
        }

    # ------------------------------------------------------------ 待办台账

    def pending_ledger(self) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        for entry in store.rows(MODULE):
            self._sync(entry)
            self._annotate(entry)
            dataset = entry.get("数据集")
            if not isinstance(dataset, dict):
                continue
            for batch in dataset.get("batches", []):
                if not batch.get("pending") and batch.get("status") != "已失败":
                    continue
                items.append({
                    "task_id": entry.get("id"),
                    "任务编号": entry.get("任务编号"),
                    "dataset_id": dataset.get("dataset_id"),
                    "数据集状态": dataset.get("status"),
                    "batch_id": batch.get("batch_id"),
                    "批次状态": batch.get("status"),
                    "pending": bool(batch.get("pending")),
                    "abnormal": bool(batch.get("abnormal")),
                    "archived": bool(batch.get("archived")),
                    "failure_reason": batch.get("failure_reason") or entry.get("失败原因"),
                    "abnormal_fragments": list(batch.get("abnormal_fragments") or []),
                    "completed_fragments": list(batch.get("completed_fragments") or []),
                    "remaining_fragments": list(batch.get("remaining_fragments") or []),
                    "retryable_input": batch.get("retryable_input") or {},
                    "consumable": bool(dataset.get("consumable")),
                })
        items.sort(key=lambda item: (int(item.get("task_id") or 0), str(item.get("batch_id") or "")))
        return {"items": items, "total": len(items)}

    # ------------------------------------------------------------ 内部工具

    def _restore_states(self) -> None:
        for entry in store.rows(MODULE):
            self._sync(entry)
            self._annotate(entry)

    def _compute_fragments(
        self, dataset: dict[str, Any]
    ) -> tuple[list[str], list[str], list[str]]:
        fragments = dataset.setdefault("fragments", [])
        required = list(
            dataset.get("required_fragments")
            or [fragment.get("fragment_id") for fragment in fragments]
        )
        completed = [
            fragment_id for fragment_id in required
            if any(
                fragment.get("fragment_id") == fragment_id
                and fragment.get("status") == FRAGMENT_DONE
                and _text(fragment.get("summary"))
                for fragment in fragments
            )
        ]
        remaining = [fragment_id for fragment_id in required if fragment_id not in completed]
        return required, completed, remaining

    def _sync(self, entry: dict[str, Any]) -> dict[str, Any]:
        dataset = entry.get("数据集")
        if not isinstance(dataset, dict):
            entry.setdefault("归档状态", UNARCHIVED)
            return entry

        required, completed, remaining = self._compute_fragments(dataset)
        dataset["required_fragments"] = required
        dataset["completed_fragments"] = completed
        dataset["remaining_fragments"] = remaining
        dataset["dedup_key"] = entry.get("去重键") or dataset.get("dedup_key") or "id"
        dataset["output_format"] = entry.get("输出格式") or dataset.get("output_format") or "json"
        dataset["status"] = DATASET_INCOMPLETE if remaining else DATASET_COMPLETE
        dataset["consumable"] = not remaining
        if remaining:
            # 未完成的聚合数据集不可能处于归档态，历史错误状态在这里被纠正。
            dataset["archived"] = False

        entry["聚合数据集"] = dataset.get("dataset_id")
        entry["数据集状态"] = dataset["status"]
        entry["可消费"] = dataset["consumable"]
        entry["剩余片段"] = remaining
        entry["去重键"] = dataset["dedup_key"]
        entry["输出格式"] = dataset["output_format"]

        for batch in dataset.get("batches", []):
            fragment_ids = list(batch.get("fragment_ids") or required)
            batch["completed_fragments"] = [
                fragment_id for fragment_id in fragment_ids if fragment_id in completed
            ]
            batch["remaining_fragments"] = [
                fragment_id for fragment_id in fragment_ids if fragment_id not in completed
            ]
            if batch["remaining_fragments"]:
                batch["pending"] = True
                batch["abnormal"] = True
                batch["archived"] = False
                if batch.get("status") not in ("重试中", "运行中"):
                    batch["status"] = "已失败"
                if not batch.get("failure_reason"):
                    batch["failure_reason"] = entry.get("失败原因") or "批次中断，存在未完成片段"
                batch["abnormal_fragments"] = list(batch["remaining_fragments"])
                batch["retryable_input"] = self._retryable_input(
                    entry, dataset, batch["remaining_fragments"], batch
                )
            else:
                batch["status"] = "已完成"
                batch["pending"] = False
                batch["abnormal"] = False
                batch["abnormal_fragments"] = []
                batch["retryable_input"] = {}

        if remaining:
            entry["pending"] = True
            entry["abnormal"] = True
            if entry.get("status") == "已完成":
                # 中止后遗留的错误「已完成/无待处理」恢复为失败待处理。
                entry["status"] = "已失败"
            if not entry.get("失败原因"):
                failed_batch = next(
                    (batch for batch in dataset.get("batches", []) if batch.get("failure_reason")),
                    None,
                )
                if failed_batch:
                    entry["失败原因"] = failed_batch.get("failure_reason")
        else:
            entry["pending"] = entry.get("status") == "已失败"
            entry["abnormal"] = entry.get("status") == "已失败"

        entry["归档状态"] = ARCHIVED if dataset.get("archived") else UNARCHIVED
        dataset["updated_at"] = dataset.get("updated_at") or _now()
        return entry

    def _annotate(self, entry: dict[str, Any]) -> dict[str, Any]:
        dataset = entry.get("数据集")
        if isinstance(dataset, dict):
            batches = dataset.get("batches") or []
            latest = batches[-1] if batches else None
            entry["批次编号"] = latest.get("batch_id") if latest else ""
            entry["批次状态"] = latest.get("status") if latest else "无批次"
            entry["异常片段"] = (
                list(latest.get("abnormal_fragments") or [])
                if latest else list(dataset.get("remaining_fragments") or [])
            )
            entry["归档状态"] = ARCHIVED if dataset.get("archived") else UNARCHIVED
            entry["数据集状态"] = dataset.get("status")
            entry["可消费"] = bool(dataset.get("consumable"))
            entry["剩余片段"] = list(dataset.get("remaining_fragments") or [])
        else:
            entry.setdefault("归档状态", UNARCHIVED)
            entry.setdefault("批次编号", "")
            entry.setdefault("批次状态", "无批次")
        return entry

    def _new_batch(self, entry: dict[str, Any], dataset: dict[str, Any]) -> dict[str, Any]:
        required, completed, remaining = self._compute_fragments(dataset)
        batch_id = f"BATCH-{entry.get('id')}-{len(dataset.get('batches') or []) + 1}"
        batch = {
            "batch_id": batch_id,
            "task_id": entry.get("id"),
            "dataset_id": dataset.get("dataset_id"),
            "status": "已失败",
            "pending": True,
            "abnormal": True,
            "archived": False,
            "failure_reason": entry.get("失败原因") or "聚合批次中断",
            "abnormal_fragments": list(remaining),
            "completed_fragments": list(completed),
            "remaining_fragments": list(remaining),
            "fragment_ids": list(required),
            "retryable_input": {},
            "started_at": _now(),
        }
        batch["retryable_input"] = self._retryable_input(entry, dataset, remaining, batch)
        return batch

    def _mark_failed(self, entry: dict[str, Any], reason: str) -> None:
        entry["status"] = "已失败"
        entry["pending"] = True
        entry["abnormal"] = True
        entry["失败原因"] = reason
        dataset = entry.get("数据集")
        if not isinstance(dataset, dict):
            self._annotate(entry)
            return
        required, completed, remaining = self._compute_fragments(dataset)
        dataset["status"] = DATASET_INCOMPLETE if remaining else DATASET_COMPLETE
        dataset["consumable"] = not remaining
        dataset["archived"] = False
        dataset["updated_at"] = _now()
        if not remaining:
            self._sync(entry)
            self._annotate(entry)
            return
        batches = dataset.setdefault("batches", [])
        batch = batches[-1] if batches else None
        if batch is None:
            batch = self._new_batch(entry, dataset)
            batches.append(batch)
        batch["status"] = "已失败"
        batch["pending"] = True
        batch["abnormal"] = True
        batch["archived"] = False
        batch["failure_reason"] = reason
        batch["abnormal_fragments"] = list(remaining)
        batch["completed_fragments"] = list(completed)
        batch["remaining_fragments"] = list(remaining)
        batch["fragment_ids"] = list(required)
        batch["updated_at"] = _now()
        batch["retryable_input"] = self._retryable_input(entry, dataset, remaining, batch)
        self._sync(entry)
        self._annotate(entry)

    def _retryable_input(
        self,
        entry: dict[str, Any],
        dataset: dict[str, Any],
        remaining: list[str],
        batch: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not remaining:
            return {}
        fragments = dataset.get("fragments") or []
        sources = []
        for fragment_id in remaining:
            fragment = next(
                (item for item in fragments if item.get("fragment_id") == fragment_id), None
            )
            if fragment and fragment.get("source"):
                sources.append(str(fragment.get("source")))
        if not sources:
            sources = [item.strip() for item in str(entry.get("输入端点列表") or "").split(",") if item.strip()]
        return {
            "task_id": entry.get("id"),
            "dataset_id": dataset.get("dataset_id"),
            "batch_id": (batch or {}).get("batch_id"),
            "fragment_ids": list(remaining),
            "endpoint": ",".join(dict.fromkeys(sources)),
            "retry_from": "missing_fragments",
        }

    def _provided_fragments(
        self, values: dict[str, Any], remaining: list[str]
    ) -> dict[str, str]:
        raw = (
            values.get("fragments")
            or values.get("fragment_summaries")
            or values.get("片段列表")
            or values.get("片段")
        )
        items: list[Any] = []
        if isinstance(raw, dict):
            items = [
                {"fragment_id": fragment_id, "summary": summary}
                for fragment_id, summary in raw.items()
            ]
        elif isinstance(raw, list):
            items = raw
        fragments: dict[str, str] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            fragment_id = (
                item.get("fragment_id")
                or item.get("id")
                or item.get("片段编号")
            )
            summary = (
                item.get("summary")
                or item.get("摘要")
                or item.get("content")
            )
            if _text(fragment_id) and _text(summary):
                fragments[_text(fragment_id)] = _text(summary)
        single_id = values.get("fragment_id") or values.get("片段编号")
        single_summary = values.get("summary") or values.get("摘要")
        if _text(single_id) and _text(single_summary):
            fragments[_text(single_id)] = _text(single_summary)
        # 也接受 {"fragment-2": "H33"} 这种直接以片段编号为键的提交。
        for fragment_id in remaining:
            if fragment_id in values and _text(values.get(fragment_id)):
                fragments[fragment_id] = _text(values.get(fragment_id))
        return fragments
