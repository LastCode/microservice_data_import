"""FastAPI application entrypoint using router composition."""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict

from fastapi import APIRouter, FastAPI

import uvicorn

from app.api.data_import import router as data_import_router
from app.api.default import router as default_router
from app.api.metadata import router as metadata_router
from app.api.variance_analysis import router as variance_analysis_router
from app.config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)
task_db: Dict[str, Dict[str, Any]] = {}
task_db_lock = threading.Lock()

router.include_router(default_router)
router.include_router(data_import_router)
router.include_router(metadata_router)
router.include_router(variance_analysis_router)

settings = get_settings()
app = FastAPI(title=settings.app_name, version="0.1.0")
app.include_router(router)


if __name__ == "__main__":
    # Allow running via `python app/main.py` (useful in simple dev setups).
    uvicorn.run("app.main:app", host="0.0.0.0", port=9000, reload=True)
