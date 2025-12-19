"""Top-level exports for the :mod:`data_connector` package."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .context import DataSetContext
from .exceptions import ConfigurationError, DataConnectorError, DataRetrievalError

if TYPE_CHECKING:  # pragma: no cover - typing aid only
    from .connector import DataConnector as _DataConnector

__all__ = [
    "ConfigurationError",
    "DataConnector",
    "DataConnectorError",
    "DataRetrievalError",
    "DataSetContext",
]


def __getattr__(name: str) -> Any:
    """Lazily import heavy optional dependencies when needed."""

    if name != "DataConnector":
        raise AttributeError(name)

    try:
        from .connector import DataConnector as _DataConnector  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - import guard
        if exc.name == "pandas":
            raise ModuleNotFoundError(
                "DataConnector requires the optional dependency 'pandas'. "
                "Install data_connector with its core extras to use this feature."
            ) from exc
        raise

    globals()["DataConnector"] = _DataConnector
    return _DataConnector
