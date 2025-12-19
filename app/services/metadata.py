"""Metadata helpers for dropdowns.

Replace these in-memory lookups with DB/Excel reads (e.g., data_map.xlsx) when wiring
to real sources.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List

from neo4j.exceptions import Neo4jError

from app.storage.graph import get_driver

# Domain types available to users.
DOMAIN_TYPES: List[str] = [
    "API",
    "CALCULATION",
    "CORE",
    "CURATED",
    "FEED",
    "REPORT",
    "REPORT CATALOG",
    "SCHEDULE",
]

# Map friendly domain types to Neo4j labels.
DOMAIN_TYPE_MAP: Dict[str, str] = {
    "API": "ApiDomain",
    "CALCULATION": "CalculationDomain",
    "CORE": "CoreDomain",
    "CURATED": "CuratedDomain",
    "FEED": "FeedDomain",
    "REPORT": "ReportDomain",
    "REPORT CATALOG": "ReportCatalogDomain",
    "SCHEDULE": "ScheduleDomain",
}


def list_domain_types() -> List[str]:
    """Return available domain types."""
    return DOMAIN_TYPES


def list_domains(domain_type: str) -> List[str]:
    """Fetch domain names for a domain type from Neo4j; empty list if unavailable."""
    label = DOMAIN_TYPE_MAP.get(domain_type.upper())
    if not label:
        return []

    driver = get_driver()
    if driver is None:
        return []

    cypher_query = f"""
    MATCH (d:{label})
    RETURN d.name AS domain_name
    ORDER BY domain_name
    """

    try:
        with driver.session() as session:
            records = session.run(cypher_query)
            return [record["domain_name"] for record in records if record.get("domain_name")]
    except Neo4jError:
        return []
    except Exception:
        return []

def list_periods(domain_type: str, domain_name: str) -> List[str]:
    """Return US COB dates for the past month for the given domain pair."""
    available_domains = list_domains(domain_type)
    if not available_domains:
        return []
    if domain_name not in available_domains:
        return []
    return _us_cob_dates_last_month()

def _us_cob_dates_last_month(today: date | None = None) -> List[str]:
    """Generate weekday dates (YYYY-MM-DD) for the past month, newest first."""
    today = today or date.today()
    start = today - timedelta(days=30)
    cob_dates: List[str] = []
    cursor = today
    while cursor >= start:
        if cursor.weekday() < 5:  # Monday=0, Friday=4
            cob_dates.append(cursor.strftime("%Y-%m-%d"))
        cursor -= timedelta(days=1)
    return cob_dates
