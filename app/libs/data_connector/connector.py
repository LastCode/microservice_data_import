"""Unified data connector implementation."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, MutableMapping

import pandas as pd
import requests

from app.config.settings import Settings

from .context import DataSetContext
from .exceptions import ConfigurationError, DataConnectorError, DataRetrievalError
from .utils import (
    build_sqlalchemy_url,
    coerce_to_mapping,
    collect_headers,
    ensure_directory,
    read_bytes,
    read_file,
)

LOGGER = logging.getLogger(__name__)


class DataConnector:
    """Coordinate data extraction based on ``settings.yaml`` metadata."""

    def __init__(
        self, settings: Settings, *, session: requests.Session | None = None
    ) -> None:
        self._settings = settings
        self._output_dir = ensure_directory(Path(settings.OUTPUT_DIR))
        self._session = session or requests.Session()
        self._data_map_cache: pd.DataFrame | None = None
        self._dispatch_table: dict[str, Callable[[DataSetContext], pd.DataFrame]] = {
            "api": self._fetch_api,
            "feed": self._fetch_feed,
            "s3": self._fetch_s3,
            "nas": self._fetch_filesystem,
            "filesystem": self._fetch_filesystem,
            "file": self._fetch_filesystem,
            "sftp": self._fetch_sftp,
            "ftp": self._fetch_sftp,
            "database": self._fetch_database,
            "db": self._fetch_database,
            "sql": self._fetch_database,
        }
        LOGGER.debug(
            "Initialised DataConnector with output directory %s", self._output_dir
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @property
    def output_directory(self) -> Path:
        """Return the directory where datasets are persisted."""

        return self._output_dir

    def load_data_map(self, *, force_reload: bool = False) -> pd.DataFrame:
        """Load the spreadsheet described by ``settings.DATA_MAP``."""

        if self._data_map_cache is not None and not force_reload:
            return self._data_map_cache.copy()

        spreadsheet = Path(self._settings.DATA_MAP)
        if not spreadsheet.exists():
            helper = "tools/create_sample_data_map.py"
            raise ConfigurationError(
                "Data map spreadsheet not found: "
                f"{spreadsheet!s}. Generate one via `{helper}` or update settings.DATA_MAP."
            )

        LOGGER.debug("Loading data map from %s", spreadsheet)
        frame = pd.read_excel(spreadsheet).fillna(value=pd.NA)
        required_columns = {"techLead", "domain_type", "domain_name", "physical_name"}
        missing = required_columns.difference(frame.columns)
        if missing:
            raise ValueError(f"Missing required columns in data map: {sorted(missing)}")

        self._data_map_cache = frame
        return frame.copy()

    def iter_contexts(self, *, force_reload: bool = False) -> Iterable[DataSetContext]:
        """Yield :class:`DataSetContext` instances for each row in the data map."""

        for row in self.load_data_map(force_reload=force_reload).to_dict(
            orient="records"
        ):
            yield self._build_context(row)

    def get_context(self, slug: str, *, force_reload: bool = False) -> DataSetContext:
        """Return the :class:`DataSetContext` matching ``slug``."""

        matched: DataSetContext | None = None
        for context in self.iter_contexts(force_reload=force_reload):
            if context.slug() != slug:
                continue
            if matched is not None:
                raise ConfigurationError(
                    f"Multiple datasets share the slug '{slug}' in data_map.xlsx"
                )
            matched = context
        if matched is None:
            raise KeyError(f"Dataset with slug '{slug}' not found in data map")
        return matched

    def get_output_path(self, context: DataSetContext) -> Path:
        """Return the filesystem path for persisted dataset outputs."""

        output_format = str(context.options.get("output_format", "csv")).lower()
        return self._output_dir / f"{context.slug()}.{output_format}"

    def fetch_all(self, *, force_reload: bool = False) -> Dict[str, pd.DataFrame]:
        """Fetch every dataset defined in the data map."""

        results: dict[str, pd.DataFrame] = {}
        for context in self.iter_contexts(force_reload=force_reload):
            slug = context.slug()
            if slug in results:
                raise ConfigurationError(
                    f"Duplicate dataset slug '{slug}' detected in data_map.xlsx"
                )
            frame = self._fetch_with_logging(context)
            results[slug] = frame
            self._persist(context, frame)
        return results

    def fetch(self, slug: str, *, force_reload: bool = False) -> pd.DataFrame:
        """Fetch a single dataset identified by ``slug`` from the data map."""

        context = self.get_context(slug, force_reload=force_reload)
        frame = self._fetch_with_logging(context)
        self._persist(context, frame)
        return frame

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_with_logging(self, context: DataSetContext) -> pd.DataFrame:
        LOGGER.info(
            "Fetching dataset for domain '%s' (%s)",
            context.domain_name,
            context.domain_type,
        )
        try:
            return self._fetch_dataset(context)
        except DataConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - defensive wrapping
            raise DataRetrievalError(str(exc)) from exc

    def _build_context(self, row: MutableMapping[str, Any]) -> DataSetContext:
        options: dict[str, Any] = {}
        for key, value in row.items():
            if key in {"techLead", "domain_type", "domain_name", "physical_name"}:
                continue
            if value is None:
                continue
            try:
                if pd.isna(value):
                    continue
            except TypeError:
                pass
            options[key] = value
        return DataSetContext(
            tech_lead=str(row.get("techLead", "")),
            domain_type=str(row.get("domain_type", "")).strip().lower(),
            domain_name=str(row.get("domain_name", "")).strip(),
            physical_name=str(row.get("physical_name", "")).strip(),
            options=coerce_to_mapping(options),
        )

    def _fetch_dataset(self, context: DataSetContext) -> pd.DataFrame:
        try:
            handler = self._dispatch_table[context.domain_type]
        except KeyError as exc:  # pragma: no cover - defensive programming
            raise ConfigurationError(
                f"Unsupported domain_type '{context.domain_type}'"
            ) from exc
        return handler(context)

    def _persist(self, context: DataSetContext, frame: pd.DataFrame) -> Path:
        destination = self.get_output_path(context)
        output_format = destination.suffix.lstrip(".").lower()
        LOGGER.debug("Persisting dataset '%s' to %s", context.slug(), destination)

        if output_format == "csv":
            frame.to_csv(destination, index=False)
        elif output_format in {"xlsx", "xls"}:
            frame.to_excel(destination, index=False)
        elif output_format == "json":
            frame.to_json(destination, orient="records", indent=2)
        elif output_format == "parquet":
            frame.to_parquet(destination, index=False)
        else:
            raise ConfigurationError(f"Unsupported output format '{output_format}'")
        return destination

    # ------------------------------------------------------------------
    # Backend fetchers
    # ------------------------------------------------------------------
    def _fetch_api(self, context: DataSetContext) -> pd.DataFrame:
        services = self._settings.API_SERVERS
        try:
            server_config = services[context.domain_name]
        except KeyError as exc:
            raise ConfigurationError(
                f"No API configuration found for '{context.domain_name}'"
            ) from exc

        base_url = server_config["BASE_URL"].rstrip("/")
        endpoint = context.physical_name.lstrip("/")
        url = f"{base_url}/{endpoint}" if endpoint else base_url

        method = str(context.options.get("method", "GET")).upper()
        headers = collect_headers(server_config, context.options.get("headers"))
        params = coerce_to_mapping(context.options.get("params"))
        payload = context.options.get("payload")
        json_payload: Any | None = None
        data_payload: Any | None = None
        if payload is not None:
            if isinstance(payload, Mapping):
                json_payload = payload
            else:
                try:
                    json_payload = json.loads(str(payload))
                except json.JSONDecodeError:
                    data_payload = payload

        timeout = float(context.options.get("timeout", 30.0))
        LOGGER.debug("Performing %s request to %s", method, url)
        try:
            response = self._session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_payload,
                data=data_payload,
                timeout=timeout,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise DataRetrievalError(f"API request failed for '{url}': {exc}") from exc

        try:
            content = response.json()
        except ValueError:
            LOGGER.debug("Response from %s is not JSON, returning raw text", url)
            return pd.DataFrame({"response": [response.text]})

        return pd.json_normalize(content)

    def _fetch_feed(self, context: DataSetContext) -> pd.DataFrame:
        url = context.physical_name
        headers = coerce_to_mapping(context.options.get("headers"))
        timeout = float(context.options.get("timeout", 30.0))
        LOGGER.debug("Fetching feed from %s", url)
        try:
            response = self._session.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise DataRetrievalError(f"Feed request failed for '{url}': {exc}") from exc
        return pd.DataFrame({"content": [response.text]})

    def _fetch_s3(self, context: DataSetContext) -> pd.DataFrame:
        config = self._settings.S3_BUCKETS
        if not config:
            raise ConfigurationError("S3 configuration is missing")

        key = context.physical_name.lstrip("/")
        LOGGER.debug(
            "Downloading object '%s' from bucket %s", key, config["BUCKET_NAME"]
        )
        try:
            import boto3
            from botocore.client import Config as BotoConfig
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise DataConnectorError("boto3 is required for S3 access") from exc

        session = boto3.session.Session(
            aws_access_key_id=config["ACCESS_KEY"],
            aws_secret_access_key=config["PRIVATE_KEY"],
        )

        client_config = BotoConfig(signature_version="s3v4")
        proxies = coerce_to_mapping(config.get("PROXY"))
        client = session.client(
            "s3",
            endpoint_url=config.get("BUCKET_ENDPOINT"),
            config=client_config,
            proxies=proxies or None,
        )
        response = client.get_object(Bucket=config["BUCKET_NAME"], Key=key)
        body = response["Body"].read()
        temp_path = self._output_dir / f".tmp_{context.slug()}"
        return read_bytes(
            body,
            temp_path=temp_path,
            read_options=context.options.get("read_options"),
            encoding=str(context.options.get("encoding", "utf-8")),
        )

    def _fetch_filesystem(self, context: DataSetContext) -> pd.DataFrame:
        base_path = context.options.get("base_path")
        path = (
            Path(base_path) / context.physical_name
            if base_path
            else Path(context.physical_name)
        )
        path = path.expanduser().resolve()
        if not path.exists():
            raise DataRetrievalError(f"File not found: {path}")

        LOGGER.debug("Loading file from filesystem: %s", path)
        return read_file(
            path,
            read_options=context.options.get("read_options"),
            encoding=str(context.options.get("encoding", "utf-8")),
        )

    def _fetch_sftp(self, context: DataSetContext) -> pd.DataFrame:
        options = context.options
        host = str(options.get("host") or context.domain_name)
        port = int(options.get("port", 22))
        username = options.get("username")
        password = options.get("password")
        key_filename = options.get("key_filename")
        remote_path = context.physical_name
        LOGGER.debug("Downloading SFTP file %s from %s:%s", remote_path, host, port)

        try:
            import paramiko
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise DataConnectorError("paramiko is required for SFTP access") from exc

        transport = paramiko.Transport((host, port))
        try:
            if key_filename:
                private_key = paramiko.RSAKey.from_private_key_file(key_filename)
                transport.connect(username=username, pkey=private_key)
            else:
                transport.connect(username=username, password=password)

            with paramiko.SFTPClient.from_transport(transport) as client:
                with client.open(remote_path, "rb") as remote_file:
                    body = remote_file.read()
        finally:
            transport.close()

        temp_path = self._output_dir / f".tmp_{context.slug()}"
        return read_bytes(
            body,
            temp_path=temp_path,
            read_options=context.options.get("read_options"),
            encoding=str(context.options.get("encoding", "utf-8")),
        )

    def _fetch_database(self, context: DataSetContext) -> pd.DataFrame:
        db_configs = self._settings.DATABASES
        try:
            db_config = db_configs[context.domain_name]
        except KeyError as exc:
            raise ConfigurationError(
                f"No database configuration for '{context.domain_name}'"
            ) from exc

        connection_url = build_sqlalchemy_url(context.domain_name, db_config)
        LOGGER.debug("Executing database query against %s", connection_url)

        try:
            from sqlalchemy import create_engine, text
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise DataConnectorError(
                "sqlalchemy is required for database access"
            ) from exc

        engine = create_engine(connection_url, future=True)
        sql = context.options.get("sql") or context.options.get("query")
        table = context.physical_name
        if not sql:
            sql = f"SELECT * FROM {table}"

        params = coerce_to_mapping(context.options.get("query_params"))
        with engine.connect() as connection:
            result = connection.execute(text(sql), params or {})
            frame = pd.DataFrame(result.fetchall(), columns=result.keys())
        return frame


__all__ = ["DataConnector"]
