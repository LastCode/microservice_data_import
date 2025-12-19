"""Workflow creation helpers."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from app.models.workflows import WorkflowCreate, WorkflowCreated


def create_workflow(payload: WorkflowCreate) -> WorkflowCreated:
    """Generate and return a new workflow."""
    workflow_id = str(uuid4())
    return WorkflowCreated(
        workflow_id=workflow_id,
        organization_name=payload.organization_name,
        application_name=payload.application_name,
        created_at=datetime.now(tz=timezone.utc),
    )
