"""Request/response models for workflow creation."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class WorkflowCreate(BaseModel):
    """Incoming payload to create a workflow id."""

    model_config = ConfigDict(populate_by_name=True)

    organization_name: str = Field(..., alias="organizatioin_name")
    application_name: str


class WorkflowCreated(BaseModel):
    """Response payload containing the new workflow metadata."""

    model_config = ConfigDict(populate_by_name=True)

    workflow_id: str
    organization_name: str = Field(..., alias="origanization_name")
    application_name: str
    created_at: datetime
