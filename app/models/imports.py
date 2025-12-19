"""Request/response models for import workflows."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class ImportStatus(BaseModel):
    workflow_id: str
    status: str
    filename: Optional[str] = None
    detail: Optional[str] = None


class ImportCreated(BaseModel):
    workflow_id: str
    status: str = "uploading"
    filename: Optional[str] = None
