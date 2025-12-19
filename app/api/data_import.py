"""Data import routes."""
from __future__ import annotations

import logging
from datetime import date
from typing import Dict
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException
from pydantic import BaseModel, Field, model_validator

from app.models.imports import ImportStatus
from app.services import (
    ImportPipeline,
    ImportRequest,
    WorkflowState,
    WorkflowStatus,
    build_default_pipeline,
)

router = APIRouter(prefix="/api/v1", tags=["data import"])
logger = logging.getLogger(__name__)

STATUSES: Dict[str, ImportStatus] = {}
PIPELINE_STATES: Dict[str, WorkflowState] = {}
pipeline: ImportPipeline = build_default_pipeline(status_store=PIPELINE_STATES, logger=logger)


class ImportJobRequest(BaseModel):
    """Payload for kicking off an import workflow."""

    domain_type: str
    domain_name: str
    cob_date: date


def _state_to_status(state: WorkflowState) -> ImportStatus:
    return ImportStatus(
        workflow_id=state.workflow_id,
        status=state.status.value if isinstance(state.status, WorkflowStatus) else str(state.status),
        detail=state.message,
    )


def _run_pipeline(workflow_id: str, payload: ImportJobRequest) -> None:
    """Run the import pipeline in the background."""
    STATUSES[workflow_id] = ImportStatus(workflow_id=workflow_id, status=WorkflowStatus.IN_PROGRESS.value)
    try:
        request = ImportRequest(
            domain_type=payload.domain_type,
            domain_name=payload.domain_name,
            cob_date=payload.cob_date,
        )
        state = pipeline.run(request, workflow_id=workflow_id)
        STATUSES[workflow_id] = _state_to_status(state)
    except Exception as exc:  # pragma: no cover - background failure logging
        logger.exception("Import workflow failed: %s", workflow_id)
        STATUSES[workflow_id] = ImportStatus(
            workflow_id=workflow_id,
            status=WorkflowStatus.FAILED.value,
            detail=str(exc),
        )


@router.post("/imports", response_model=ImportStatus)
async def create_import(
    payload: ImportJobRequest = Body(...), background_tasks: BackgroundTasks = None
) -> ImportStatus:
    workflow_id = str(uuid4())
    STATUSES[workflow_id] = ImportStatus(workflow_id=workflow_id, status=WorkflowStatus.PENDING.value)
    background_tasks.add_task(_run_pipeline, workflow_id, payload)
    return STATUSES[workflow_id]


@router.get("/imports/{workflow_id}", response_model=ImportStatus)
async def get_status(workflow_id: str) -> ImportStatus:
    status = STATUSES.get(workflow_id)
    if not status:
        raise HTTPException(status_code=404, detail="workflow_id not found")
    return status
