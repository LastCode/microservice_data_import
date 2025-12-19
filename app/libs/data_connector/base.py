"""Base connector interface used by the package."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Optional


class ConnectionError(RuntimeError):
    """Error raised when a connector fails to establish a connection."""


@dataclass
class BaseConnector(ABC):
    """Abstract base class for all connectors.

    Subclasses must implement :meth:`connect`, :meth:`execute`, and
    :meth:`close`. The context manager protocol is implemented in the
    base class so connectors can be used with the ``with`` statement.
    """

    autoconnect: bool = True
    """Automatically call :meth:`connect` when entering the context manager."""

    connected: bool = False
    """Tracks whether the connector is currently connected."""

    def __enter__(self) -> "BaseConnector":
        if self.autoconnect and not self.connected:
            self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> Optional[bool]:
        self.close()
        return None

    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to the underlying data store."""

    @abstractmethod
    def close(self) -> None:
        """Close any open connections and release resources."""

    @abstractmethod
    def execute(
        self,
        query: str,
        parameters: Optional[Iterable[Any]] = None,
    ) -> Any:
        """Execute a query or command against the data store."""

    def ensure_connected(self) -> None:
        """Raise :class:`ConnectionError` if ``connect`` was not called."""

        if not self.connected:
            raise ConnectionError(
                "Connector must be connected before executing operations."
            )
