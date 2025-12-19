"""Standalone FastAPI app for metadata endpoints."""
from __future__ import annotations

from fastapi import FastAPI

import uvicorn

from metadata_service.api.metadata import router as metadata_router

app = FastAPI(title="Metadata Service", version="0.1.0")
app.include_router(metadata_router)


if __name__ == "__main__":
    uvicorn.run("metadata_service.main:app", host="0.0.0.0", port=8001, reload=True)
