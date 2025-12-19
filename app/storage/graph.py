"""Neo4j driver helper."""
from __future__ import annotations

from typing import Optional

from neo4j import Driver, GraphDatabase

from app.config import get_settings

_DRIVER: Optional[Driver] = None


def get_driver() -> Optional[Driver]:
    """Return a cached Neo4j driver, or None if configuration is missing."""
    global _DRIVER
    if _DRIVER:
        return _DRIVER

    settings = get_settings()
    if not (settings.neo4j_uri and settings.neo4j_user and settings.neo4j_password):
        return None

    _DRIVER = GraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)
    )
    return _DRIVER
