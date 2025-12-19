"""Metadata endpoints."""
from __future__ import annotations

from typing import List

from fastapi import APIRouter, HTTPException, Query

from app.services import list_domain_types, list_domains, list_periods

router = APIRouter(prefix="/api/v1/metadata", tags=["metadata"])


@router.get("/domain-types", response_model=List[str])
async def get_domain_types() -> List[str]:
    return list_domain_types()


@router.get("/domains", response_model=List[str])
async def get_domains(domain_type: str = Query(..., alias="domain_type")) -> List[str]:
    domains = list_domains(domain_type)
    if not domains:
        raise HTTPException(status_code=404, detail="domain_type not found")
    return domains


@router.get("/periods", response_model=List[str])
async def get_periods(
    domain_type: str = Query(..., alias="domain_type"),
    domain_name: str = Query(..., alias="domain_name"),
) -> List[str]:
    periods = list_periods(domain_type, domain_name)
    if not periods:
        raise HTTPException(status_code=404, detail="domain_type/domain_name not found")
    return periods
