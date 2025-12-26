"""File connectors for fetching data from various sources."""
from __future__ import annotations

import csv
import json
import logging
import os
import shutil
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Protocol

import yaml

logger = logging.getLogger(__name__)

# Get project root directory (parent of app/)
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONF_DIR = PROJECT_ROOT / "conf"


@dataclass
class ConnectorConfig:
    """Configuration for a data connector."""
    connector_type: str
    server_name: Optional[str] = None
    params: Dict[str, Any] = None

    def __post_init__(self):
        if self.params is None:
            self.params = {}


@dataclass
class FetchResult:
    """Result of a file fetch operation."""
    success: bool
    local_path: Optional[Path] = None
    error: Optional[str] = None
    bytes_transferred: int = 0


class BaseConnector(ABC):
    """Abstract base class for file connectors."""

    def __init__(self, config: ConnectorConfig, settings: Dict[str, Any]):
        self.config = config
        self.settings = settings

    @abstractmethod
    def fetch(self, source_path: str, destination_path: Path) -> FetchResult:
        """Fetch a file from the source to the destination."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the connection is available."""
        pass


class LinuxConnector(BaseConnector):
    """Connector for local Linux filesystem or mounted NAS."""

    def fetch(self, source_path: str, destination_path: Path) -> FetchResult:
        """Copy a file from local/mounted filesystem."""
        try:
            source = Path(source_path)
            if not source.exists():
                return FetchResult(
                    success=False,
                    error=f"Source file not found: {source_path}"
                )

            # Ensure destination directory exists
            destination_path.parent.mkdir(parents=True, exist_ok=True)

            # Copy the file
            shutil.copy2(source, destination_path)
            file_size = destination_path.stat().st_size

            logger.info(f"Successfully copied {source_path} to {destination_path} ({file_size} bytes)")
            return FetchResult(
                success=True,
                local_path=destination_path,
                bytes_transferred=file_size
            )
        except Exception as e:
            logger.error(f"Failed to copy file: {e}")
            return FetchResult(success=False, error=str(e))

    def test_connection(self) -> bool:
        """Test if the source path is accessible."""
        return True  # Local filesystem is always accessible


class SFTPConnector(BaseConnector):
    """Connector for SFTP file transfers."""

    def __init__(self, config: ConnectorConfig, settings: Dict[str, Any]):
        super().__init__(config, settings)
        self.server_settings = self._get_server_settings()

    def _get_server_settings(self) -> Dict[str, str]:
        """Get server credentials from settings."""
        server_name = self.config.server_name or self.config.params.get("server_name")
        if not server_name:
            return {}

        linux_servers = self.settings.get("LINUX_SERVERS", {})
        # Handle the typo in settings.yaml (LINUX_ SERVERS)
        if not linux_servers:
            linux_servers = self.settings.get("LINUX_ SERVERS", {})

        return linux_servers.get(server_name, {})

    def fetch(self, source_path: str, destination_path: Path) -> FetchResult:
        """Fetch a file via SFTP."""
        try:
            server_settings = self.server_settings
            if not server_settings:
                return FetchResult(
                    success=False,
                    error=f"No server settings found for {self.config.server_name}"
                )

            user = server_settings.get("user")
            host = self.config.server_name

            # Ensure destination directory exists
            destination_path.parent.mkdir(parents=True, exist_ok=True)

            # Use scp for file transfer (assumes SSH keys are set up)
            cmd = ["scp", f"{user}@{host}:{source_path}", str(destination_path)]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

            if result.returncode != 0:
                return FetchResult(
                    success=False,
                    error=f"SCP failed: {result.stderr}"
                )

            file_size = destination_path.stat().st_size
            logger.info(f"Successfully fetched {source_path} via SFTP ({file_size} bytes)")
            return FetchResult(
                success=True,
                local_path=destination_path,
                bytes_transferred=file_size
            )
        except subprocess.TimeoutExpired:
            return FetchResult(success=False, error="SFTP transfer timed out")
        except Exception as e:
            logger.error(f"SFTP fetch failed: {e}")
            return FetchResult(success=False, error=str(e))

    def test_connection(self) -> bool:
        """Test SFTP connection."""
        try:
            server_settings = self.server_settings
            if not server_settings:
                return False

            user = server_settings.get("user")
            host = self.config.server_name

            cmd = ["ssh", "-o", "ConnectTimeout=5", f"{user}@{host}", "echo", "ok"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            return result.returncode == 0
        except Exception:
            return False


class ConnectorFactory:
    """Factory for creating file connectors."""

    _connectors = {
        "linux": LinuxConnector,
        "local": LinuxConnector,
        "sftp": SFTPConnector,
        "scp": SFTPConnector,
    }

    @classmethod
    def create(cls, connector_type: str, config: ConnectorConfig, settings: Dict[str, Any]) -> BaseConnector:
        """Create a connector based on type."""
        connector_class = cls._connectors.get(connector_type.lower())
        if not connector_class:
            raise ValueError(f"Unknown connector type: {connector_type}")
        return connector_class(config, settings)

    @classmethod
    def register(cls, connector_type: str, connector_class: type):
        """Register a new connector type."""
        cls._connectors[connector_type.lower()] = connector_class


class DataMapResolver:
    """Resolve domain configuration from data_map.csv."""

    def __init__(self, data_map_path: Optional[Path] = None):
        self.data_map_path = data_map_path or (CONF_DIR / "data_map.csv")
        self._cache: Dict[str, Dict[str, Any]] = {}

    def resolve(self, domain_type: str, domain_name: str) -> Optional[Dict[str, Any]]:
        """Look up configuration for a domain."""
        cache_key = f"{domain_type}:{domain_name}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        if not self.data_map_path.exists():
            logger.error(f"Data map file not found: {self.data_map_path}")
            return None

        try:
            with self.data_map_path.open("r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row_type = (row.get("domain_type") or "").strip().upper()
                    row_name = (row.get("domain_name") or "").strip()

                    if row_type == domain_type.upper() and row_name.lower() == domain_name.lower():
                        # Parse connector params
                        params_raw = row.get("connector_params", "{}")
                        try:
                            params = json.loads(params_raw.replace('""', '"'))
                        except json.JSONDecodeError:
                            params = {}

                        config = {
                            "tech_lead": row.get("tech_lead", ""),
                            "domain_type": row_type,
                            "domain_name": row_name,
                            "physical_name": row.get("physical_name", ""),
                            "connector_type": row.get("connector_type", "linux"),
                            "connector_params": params,
                            "source_file_path_template": row.get("source_file_path_template", ""),
                        }
                        self._cache[cache_key] = config
                        return config

            logger.warning(f"No configuration found for {domain_type}/{domain_name}")
            return None
        except Exception as e:
            logger.error(f"Error reading data map: {e}")
            return None


class ColumnMapResolver:
    """Resolve column mapping configuration from column_map.yaml."""

    def __init__(self, column_map_path: Optional[Path] = None):
        self.column_map_path = column_map_path or (CONF_DIR / "column_map.yaml")
        self._config: Optional[Dict[str, Any]] = None

    def _load_config(self) -> Dict[str, Any]:
        """Load the column map configuration."""
        if self._config is not None:
            return self._config

        if not self.column_map_path.exists():
            logger.error(f"Column map file not found: {self.column_map_path}")
            return {}

        try:
            with self.column_map_path.open("r") as f:
                self._config = yaml.safe_load(f) or {}
                return self._config
        except Exception as e:
            logger.error(f"Error reading column map: {e}")
            return {}

    def resolve(self, domain_name: str) -> Optional[Dict[str, Any]]:
        """Get column configuration for a domain."""
        config = self._load_config()
        domains = config.get("domains", {})
        return domains.get(domain_name)

    def get_defaults(self) -> Dict[str, Any]:
        """Get default settings."""
        config = self._load_config()
        return config.get("defaults", {})


class SettingsLoader:
    """Load settings from settings.yaml."""

    def __init__(self, settings_path: Optional[Path] = None):
        self.settings_path = settings_path or (CONF_DIR / "settings.yaml")
        self._settings: Optional[Dict[str, Any]] = None

    def load(self) -> Dict[str, Any]:
        """Load settings from file."""
        if self._settings is not None:
            return self._settings

        if not self.settings_path.exists():
            logger.warning(f"Settings file not found: {self.settings_path}")
            return {}

        try:
            with self.settings_path.open("r") as f:
                self._settings = yaml.safe_load(f) or {}
                return self._settings
        except Exception as e:
            logger.error(f"Error loading settings: {e}")
            return {}

    def get_database_config(self, db_name: str) -> Dict[str, Any]:
        """Get database configuration."""
        settings = self.load()
        databases = settings.get("DATABASES", {})
        return databases.get(db_name, {})

    def get_server_config(self, server_name: str) -> Dict[str, Any]:
        """Get Linux server configuration."""
        settings = self.load()
        # Handle the typo in settings.yaml
        servers = settings.get("LINUX_SERVERS", {}) or settings.get("LINUX_ SERVERS", {})
        return servers.get(server_name, {})
