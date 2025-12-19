from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import boto3
import urllib3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.config.settings import SettingsError, load_settings

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_DEFAULT_S3_CONFIG: Mapping[str, Any] | None = None


def _get_default_s3_config() -> Mapping[str, Any]:
    global _DEFAULT_S3_CONFIG
    if _DEFAULT_S3_CONFIG is None:
        try:
            _DEFAULT_S3_CONFIG = load_settings().S3_BUCKETS
        except SettingsError:
            _DEFAULT_S3_CONFIG = {}
    return _DEFAULT_S3_CONFIG


@dataclass(frozen=True)
class S3BucketSettings:
    """Structured configuration for accessing an S3 bucket."""

    endpoint_url: str
    bucket_name: str
    access_key: str
    private_key: str
    proxy: Mapping[str, str] | None = None

    @classmethod
    def from_config(cls, bucket_config: Mapping[str, Any]) -> "S3BucketSettings":
        required_keys = {
            "BUCKET_ENDPOINT",
            "BUCKET_NAME",
            "ACCESS_KEY",
            "PRIVATE_KEY",
        }
        missing = required_keys - bucket_config.keys()
        if missing:
            missing_keys = ", ".join(sorted(missing))
            raise ValueError(
                f"Missing required S3 bucket configuration values: {missing_keys}"
            )
        proxy_config = bucket_config.get("PROXY")
        if proxy_config is not None and not isinstance(proxy_config, Mapping):
            raise TypeError(
                "S3 bucket proxy configuration must be a mapping of scheme to URL"
            )
        return cls(
            endpoint_url=str(bucket_config["BUCKET_ENDPOINT"]),
            bucket_name=str(bucket_config["BUCKET_NAME"]),
            access_key=str(bucket_config["ACCESS_KEY"]),
            private_key=str(bucket_config["PRIVATE_KEY"]),
            proxy=dict(proxy_config) if proxy_config is not None else None,
        )


def _load_bucket_settings(
    bucket_alias: str | None = None,
    *,
    s3_config: Mapping[str, Any] | None = None,
) -> S3BucketSettings:
    """Resolve :class:`S3BucketSettings` from settings data."""

    config_source: Mapping[str, Any] | None = (
        s3_config if s3_config is not None else _get_default_s3_config()
    )
    if not config_source:
        raise ValueError("S3 bucket configuration is missing or empty")

    if bucket_alias is not None:
        try:
            raw_config = config_source[bucket_alias]
        except KeyError as exc:
            available = ", ".join(sorted(config_source)) or "<none>"
            raise ValueError(
                f"Unknown S3 bucket alias '{bucket_alias}'. Available: {available}"
            ) from exc
    else:
        required_keys = {
            "BUCKET_ENDPOINT",
            "BUCKET_NAME",
            "ACCESS_KEY",
            "PRIVATE_KEY",
        }
        if required_keys <= config_source.keys():
            raw_config = config_source
        elif len(config_source) == 1:
            raw_config = next(iter(config_source.values()))
        else:
            available = ", ".join(sorted(config_source)) or "<none>"
            raise ValueError(
                "Multiple S3 bucket configurations detected. Provide 'bucket_alias' "
                f"explicitly. Available aliases: {available}"
            )

    return S3BucketSettings.from_config(raw_config)


def _build_object_key(folder: str | None, file_name: str) -> str:
    prefix = (folder or "").strip()
    if not prefix:
        return file_name
    normalized = prefix.replace("\\", "/").strip("/")
    return f"{normalized}/{file_name}"


def fetch_s3_data_file_header(
    folder: str | None = None,
    file_name: str | None = None,
    enable_proxy: bool = False,
    bucket_alias: str | None = None,
    *,
    s3_config: Mapping[str, Any] | None = None,
    object_key: str | None = None,
) -> str:
    """
    Retrieve the header line from an object stored in an S3-compatible bucket.

    Parameters
    ----------
    folder:
        Folder path within the bucket (e.g. ``"/path/to/folder"``).
    file_name:
        The object name (e.g. ``"data_file.dat"``).
    enable_proxy:
        Whether to configure the S3 client with the proxy settings specified in
        the settings file.
    bucket_alias:
        Optional key identifying which configuration entry in ``S3_BUCKETS`` to
        use. When omitted, a single configured bucket or a flat configuration is
        assumed.
    object_key:
        Full object key (prefix + filename). When provided, ``folder`` and
        ``file_name`` are ignored.

    Returns
    -------
    str
        The first line of the object's contents decoded as UTF-8 text.
    """

    settings = _load_bucket_settings(bucket_alias=bucket_alias, s3_config=s3_config)

    if object_key is not None:
        key = object_key.lstrip("/")
    else:
        if file_name is None:
            raise ValueError(
                "file_name must be provided when object_key is not supplied"
            )
        key = _build_object_key(folder, file_name)

    if enable_proxy and settings.proxy:
        my_config = Config(proxies=settings.proxy)
    else:
        my_config = Config()

    session = boto3.Session(
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.private_key,
    )
    s3 = session.client(
        "s3",
        config=my_config,
        endpoint_url=settings.endpoint_url,
        verify=False,
    )

    try:
        response = s3.get_object(Bucket=settings.bucket_name, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        auth_errors = {
            "AccessDenied",
            "InvalidAccessKeyId",
            "InvalidToken",
            "SignatureDoesNotMatch",
        }
        if error_code in auth_errors:
            raise RuntimeError(f"S3 authentication failed: {error_code}") from exc
        if error_code == "NoSuchKey":
            raise FileNotFoundError(
                f"S3 object not found: {settings.bucket_name}/{key}"
            ) from exc
        raise RuntimeError(f"S3 request failed ({error_code}): {exc}") from exc

    data = response["Body"].read()
    header = data.decode("utf-8").splitlines()[0]
    return header


def build_s3_header_map(
    domain_type: str,
    domain_name: str,
    *,
    folder: str,
    file_name: str,
    bucket_alias: str | None = None,
    enable_proxy: bool = False,
    s3_config: Mapping[str, Any] | None = None,
    object_key: str | None = None,
) -> dict[str, dict[str, list[str]]]:
    """
    Produce the header mapping structure used by the SSH header fetcher helpers.
    """

    header_line = fetch_s3_data_file_header(
        folder=folder,
        file_name=file_name,
        enable_proxy=enable_proxy,
        bucket_alias=bucket_alias,
        s3_config=s3_config,
        object_key=object_key,
    )
    columns = [value.strip() for value in header_line.split(",") if value.strip()]
    return {domain_type: {domain_name: columns}}
