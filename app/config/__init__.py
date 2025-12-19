import os
import sys
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel


class Neo4jConfig(BaseModel):
    url: str
    user: str
    password: str


class RedisConfig(BaseModel):
    url: str


class MinioConfig(BaseModel):
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str


class AppConfig(BaseModel):
    neo4j: Neo4jConfig
    redis: RedisConfig
    minio: MinioConfig


_config: Optional[AppConfig] = None


def load_config(environment: str) -> AppConfig:
    global _config

    config_path = Path(__file__).parent.parent.parent / "conf" / f"{environment}.yaml"

    if not config_path.exists():
        print(
            f"CRITICAL ERROR: Configuration file not found: {config_path}",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        _config = AppConfig(**config_data)
        return _config
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to load configuration: {e}", file=sys.stderr)
        sys.exit(1)


def get_config() -> AppConfig:
    global _config
    if _config is None:
        raise RuntimeError("Configuration not loaded. Call load_config() first.")
    return _config


class Settings(BaseModel):
    """Flat settings object for backward compatibility."""
    app_name: str = "Scribe Data Import Service"
    neo4j_uri: Optional[str] = None
    neo4j_user: Optional[str] = None
    neo4j_password: Optional[str] = None
    redis_url: Optional[str] = None
    minio_endpoint_url: Optional[str] = None
    minio_access_key: Optional[str] = None
    minio_secret_key: Optional[str] = None
    minio_bucket_name: Optional[str] = None


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get flat settings object, auto-loading from config if available."""
    global _settings, _config

    if _settings is not None:
        return _settings

    # Try to load from environment or return empty settings
    if _config is not None:
        _settings = Settings(
            neo4j_uri=_config.neo4j.url,
            neo4j_user=_config.neo4j.user,
            neo4j_password=_config.neo4j.password,
            redis_url=_config.redis.url,
            minio_endpoint_url=_config.minio.endpoint_url,
            minio_access_key=_config.minio.access_key,
            minio_secret_key=_config.minio.secret_key,
            minio_bucket_name=_config.minio.bucket_name,
        )
    else:
        # Return empty settings if config not loaded yet
        _settings = Settings(
            neo4j_uri=os.getenv("NEO4J_URI"),
            neo4j_user=os.getenv("NEO4J_USER"),
            neo4j_password=os.getenv("NEO4J_PASSWORD"),
            redis_url=os.getenv("REDIS_URL"),
            minio_endpoint_url=os.getenv("MINIO_ENDPOINT_URL"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY"),
            minio_bucket_name=os.getenv("MINIO_BUCKET_NAME"),
        )

    return _settings
