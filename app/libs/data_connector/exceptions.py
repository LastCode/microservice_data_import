"""Custom exceptions for the data connector."""

from __future__ import annotations


class DataConnectorError(RuntimeError):
    """Base exception for connector errors."""


class ConfigurationError(DataConnectorError):
    """Raised when configuration for a data source is missing or invalid."""


class DataRetrievalError(DataConnectorError):
    """Raised when fetching a dataset fails."""
