"""Default/general routes (health, workflow creation)."""
from __future__ import annotations

from fastapi import APIRouter

from app.models.workflows import WorkflowCreate, WorkflowCreated
from app.services import create_workflow

router = APIRouter()


@router.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@router.post("/api/v1/workflows", response_model=WorkflowCreated)
async def create_workflow_id(payload: WorkflowCreate) -> WorkflowCreated:
    return create_workflow(payload)
