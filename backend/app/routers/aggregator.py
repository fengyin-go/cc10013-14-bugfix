"""数据聚合接口：聚合任务、聚合数据集批次、待办台账与结果消费门禁。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.aggregator import AggregatorService

router = APIRouter(prefix="/api/aggregator", tags=["数据聚合"])

service = AggregatorService()

LIST_FIELDS = [
    "任务编号", "输入端点列表", "合并策略", "去重键", "排序字段", "输出格式", "批次大小", "调度周期",
]
STATUSES = ["草稿", "待执行", "运行中", "已完成", "已失败"]


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出数据聚合清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "aggregator", "total": total, "items": items}


@router.get("/ledger", response_model=dict)
def pending_ledger() -> dict[str, Any]:
    """聚合任务待办台账：接收失败批次，列出待处理标记、失败原因、剩余片段与可重试输入。"""
    return service.pending_ledger()


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="草稿、待执行、运行中、已完成、已失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤数据聚合列表；失败批次保持待处理，不会被显示成已归档。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}/batches/{batch_id}", response_model=dict)
def get_batch_detail(entry_id: int, batch_id: str) -> dict:
    """批次详情：读取失败原因、异常片段、已完成片段、剩余片段与恢复入口。"""
    detail = service.batch_detail(entry_id, batch_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"聚合任务 {entry_id} 不存在或已归档")
    return detail


@router.post("/{entry_id}/batches/{batch_id}/retry", response_model=dict)
def retry_batch(entry_id: int, batch_id: str, payload: EntryPayload) -> dict:
    """从缺失片段重试批次；未补齐时保持失败状态，补齐后才转为可消费。"""
    result = service.retry_batch(entry_id, batch_id, payload.values)
    if not result.get("ok"):
        return result
    return result


@router.post("/{entry_id}/fragments/{fragment_id}/corrections", response_model=dict)
def receive_correction(entry_id: int, fragment_id: str, payload: EntryPayload) -> dict:
    """接收来源更正：按来源优先级与版本判定冲突，保留胜出的片断摘要。"""
    return service.add_correction(entry_id, fragment_id, payload.values)


@router.get("/{entry_id}/result", response_model=dict)
def get_result(entry_id: int) -> dict:
    """读取聚合结果；必需片段未齐时一律拒绝，消费方看不到半成品。"""
    result = service.get_result(entry_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"聚合任务 {entry_id} 不存在或已归档")
    return result


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条聚合任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"聚合任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条聚合任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="聚合任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条聚合任务执行提交执行、启动聚合、终止聚合、归档结果；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
