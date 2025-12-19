"""Utilities for loading project configuration from ``settings.yaml``."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml


class SettingsError(RuntimeError):
    """Raised when the settings file is missing or malformed."""


def _coerce_mapping(name: str, value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    raise SettingsError(
        f"Settings field '{name}' must be a mapping, got {type(value).__name__}"
    )


DEFAULT_SETTINGS_PATH = Path("conf/settings.yaml")


@dataclass(slots=True, frozen=True)
class Settings:
    """In-memory representation of ``settings.yaml``."""

    OUTPUT_DIR: str = "output"
    DATA_MAP: str = "data_map.xlsx"
    API_SERVERS: dict[str, Any] = field(default_factory=dict)
    DATABASES: dict[str, Any] = field(default_factory=dict)
    S3_BUCKETS: dict[str, Any] = field(default_factory=dict)
    LINUX_SERVERS: dict[str, Any] = field(default_factory=dict)
    _extras: dict[str, Any] = field(default_factory=dict, repr=False)

    def __getattr__(self, name: str) -> Any:
        try:
            return self._extras[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def as_mapping(self) -> dict[str, Any]:
        """Return a dictionary representation including extra keys."""

        payload: dict[str, Any] = {
            "OUTPUT_DIR": self.OUTPUT_DIR,
            "DATA_MAP": self.DATA_MAP,
            "API_SERVERS": dict(self.API_SERVERS),
            "DATABASES": dict(self.DATABASES),
            "S3_BUCKETS": dict(self.S3_BUCKETS),
            "LINUX_SERVERS": dict(self.LINUX_SERVERS),
        }
        payload.update(self._extras)
        return payload


def load_settings(path: str | Path = DEFAULT_SETTINGS_PATH) -> Settings:
    """Load configuration from ``settings.yaml`` into a :class:`Settings` object."""

    settings_path = Path(path)
    if not settings_path.exists():
        raise SettingsError(f"Settings file not found: {settings_path!s}")

    try:
        content = settings_path.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - filesystem failure
        raise SettingsError(f"Unable to read settings file: {exc}") from exc

    try:
        raw_data = yaml.safe_load(content) or {}
    except yaml.YAMLError as exc:
        raise SettingsError(f"Failed to parse settings file: {exc}") from exc

    if not isinstance(raw_data, Mapping):
        raise SettingsError("Top-level settings structure must be a mapping")

    normalized: dict[str, Any] = {}
    for key, value in raw_data.items():
        if not isinstance(key, str):
            raise SettingsError("All top-level keys in settings must be strings")
        normalized[key.upper()] = value

    extras: dict[str, Any] = {}
    for key in set(normalized) - {
        "OUTPUT_DIR",
        "DATA_MAP",
        "API_SERVERS",
        "DATABASES",
        "S3_BUCKETS",
        "LINUX_SERVERS",
    }:
        extras[key] = normalized[key]

    return Settings(
        OUTPUT_DIR=str(normalized.get("OUTPUT_DIR", "output")),
        DATA_MAP=str(normalized.get("DATA_MAP", "data_map.xlsx")),
        API_SERVERS=_coerce_mapping("API_SERVERS", normalized.get("API_SERVERS")),
        DATABASES=_coerce_mapping("DATABASES", normalized.get("DATABASES")),
        S3_BUCKETS=_coerce_mapping("S3_BUCKETS", normalized.get("S3_BUCKETS")),
        LINUX_SERVERS=_coerce_mapping("LINUX_SERVERS", normalized.get("LINUX_SERVERS")),
        _extras=extras,
    )
