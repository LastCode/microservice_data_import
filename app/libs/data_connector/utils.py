"""Utility helpers for the data connector."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, MutableMapping

try:  # pragma: no cover - import guard
    import pandas as pd
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    if exc.name != "pandas":
        raise
    pd = None  # type: ignore[assignment]
    _PANDAS_IMPORT_ERROR = exc
else:
    _PANDAS_IMPORT_ERROR = None


def slugify(candidate: str) -> str:
    """Return a filesystem safe representation of *candidate*."""

    filtered = "".join(
        ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in candidate
    )
    return filtered.strip("_") or "dataset"


def ensure_directory(path: Path) -> Path:
    """Create *path* (and parents) if necessary and return it."""

    path.mkdir(parents=True, exist_ok=True)
    return path


def _require_pandas() -> Any:
    if pd is None:  # pragma: no cover - defensive branch
        assert _PANDAS_IMPORT_ERROR is not None
        raise ModuleNotFoundError(
            "Pandas is required for this operation. Install the 'pandas' extra to continue."
        ) from _PANDAS_IMPORT_ERROR
    return pd


def _is_missing_value(value: Any) -> bool:
    if value in (None, ""):
        return True
    if pd is not None and value is pd.NA:
        return True
    return False


def coerce_to_mapping(value: Any) -> dict[str, Any]:
    """Attempt to normalise ``value`` into a standard dictionary."""

    if _is_missing_value(value):
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, MutableMapping):
        return dict(value.items())
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:  # pragma: no cover - helpful context
            raise ValueError(f"Unable to parse mapping from string: {value}") from exc
        if not isinstance(parsed, Mapping):
            raise ValueError(f"Expected a JSON object when parsing mapping: {value}")
        return dict(parsed)
    raise TypeError(f"Unable to coerce value to mapping: {value!r}")


def collect_headers(
    server_config: Mapping[str, Any], overrides: Any | None
) -> dict[str, Any]:
    """Combine default API headers with user overrides."""

    headers = {
        "Authorization": server_config.get("API_SECRET")
        or server_config.get("API_KEY"),
        "X-Api-Key": server_config.get("API_KEY"),
    }
    headers = {key: value for key, value in headers.items() if value}
    headers.update(coerce_to_mapping(overrides))
    return headers


def read_file(
    path: Path,
    *,
    read_options: Mapping[str, Any] | None = None,
    encoding: str = "utf-8",
) -> pd.DataFrame:
    """Read ``path`` into a :class:`pandas.DataFrame`."""

    pandas = _require_pandas()
    read_options = dict(read_options or {})
    suffix = path.suffix.lower()
    if suffix in {".csv", ""}:
        return pandas.read_csv(path, **read_options)
    if suffix in {".xlsx", ".xls"}:
        return pandas.read_excel(path, **read_options)
    if suffix == ".json":
        return pandas.read_json(path, **read_options)
    if suffix == ".parquet":
        return pandas.read_parquet(path, **read_options)

    with path.open("r", encoding=encoding) as handle:
        content = handle.read()
    return pandas.DataFrame({"content": [content]})


def read_bytes(
    payload: bytes,
    *,
    temp_path: Path,
    read_options: Mapping[str, Any] | None = None,
    encoding: str = "utf-8",
) -> "pd.DataFrame":
    """Persist temporary bytes and delegate to :func:`read_file`."""

    temp_path.write_bytes(payload)
    try:
        frame = read_file(temp_path, read_options=read_options, encoding=encoding)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    return frame


def build_sqlalchemy_url(name: str, config: Mapping[str, Any]) -> str:
    """Return a SQLAlchemy connection URL for ``name`` using ``config``."""

    name = name.lower()
    user = config.get("USER")
    password = config.get("PASSWORD")
    host = config.get("HOST")
    port = config.get("PORT")

    if name in {"postgresql", "postgres"}:
        database = config.get("DATABASE")
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    if name in {"mssql", "sqlserver", "sql_server"}:
        database = config.get("DATABASE")
        driver = config.get("DRIVER", "ODBC Driver 17 for SQL Server")
        driver_query = driver.replace(" ", "+")
        return f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver_query}"
    if name == "oracle":
        service = config.get("SERVICE_NAME")
        return f"oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service}"
    if name == "neo4j":
        scheme = config.get("SCHEME", "neo4j")
        return f"{scheme}://{user}:{password}@{host}:{port}"
    if name == "sybase":
        database = config.get("DATABASE")
        return f"sybase+pyodbc://{user}:{password}@{host}:{port}/{database}"
    if name == "hive":
        database = config.get("DATABASE")
        return f"hive://{user}:{password}@{host}:{port}/{database}"
    if name in {"mongodb", "mongo"}:
        database = config.get("DATABASE")
        auth_source = config.get("AUTH_SOURCE")
        options = config.get("OPTIONS")

        credentials = ""
        if user and password:
            credentials = f"{user}:{password}@"
        elif user:
            credentials = f"{user}@"

        query_components: list[str] = []
        if auth_source:
            query_components.append(f"authSource={auth_source}")
        if options:
            if isinstance(options, Mapping):
                query_components.extend(
                    f"{key}={value}" for key, value in options.items()
                )
            else:
                query_components.append(str(options))
        query_string = f"?{'&'.join(query_components)}" if query_components else ""

        return f"mongodb://{credentials}{host}:{port}/{database}{query_string}"

    raise ValueError(f"Unsupported database type '{name}'")
