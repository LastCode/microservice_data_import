"""SQLite connector implementation."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterable, Sequence
from typing import Any, Optional

from .base import BaseConnector, ConnectionError


RowFactory = Callable[[sqlite3.Cursor, sqlite3.Row], Any]


class SQLiteConnector(BaseConnector):
    """Simple connector for interacting with SQLite databases."""

    def __init__(
        self,
        database: str,
        *,
        autoconnect: bool = True,
        row_factory: Optional[RowFactory] = None,
        **connect_kwargs: Any,
    ) -> None:
        """Create a connector for ``database``.

        Parameters
        ----------
        database:
            Path to the SQLite database file.
        autoconnect:
            Automatically call :meth:`connect` when entering a context manager.
        row_factory:
            Optional row factory to apply to the underlying connection. When
            provided, results returned from :meth:`execute` and
            :meth:`executemany` will use this factory.
        connect_kwargs:
            Extra keyword arguments forwarded to :func:`sqlite3.connect`.
        """

        super().__init__(autoconnect=autoconnect)
        self.database = database
        self.row_factory = row_factory
        self.connect_kwargs = connect_kwargs
        self._connection: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        if self.connected:
            return
        try:
            self._connection = sqlite3.connect(self.database, **self.connect_kwargs)
            if self.row_factory is not None:
                self._connection.row_factory = self.row_factory
        except sqlite3.Error as exc:  # pragma: no cover - pass through message
            raise ConnectionError(str(exc)) from exc
        self.connected = True

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        self.connected = False

    def execute(
        self,
        query: str,
        parameters: Optional[Iterable[Any]] = None,
    ) -> Sequence[Any]:
        self.ensure_connected()
        assert self._connection is not None  # For type checkers only.

        cursor = self._connection.cursor()
        try:
            if parameters is None:
                cursor.execute(query)
            else:
                cursor.execute(query, tuple(parameters))
            if query.strip().lower().startswith("select"):
                results = cursor.fetchall()
            else:
                self._connection.commit()
                results = []
        finally:
            cursor.close()

        return results

    def executemany(
        self,
        query: str,
        seq_of_parameters: Iterable[Iterable[Any]],
    ) -> None:
        """Execute ``query`` against every parameter sequence provided.

        The underlying connection is committed after executing the statements.
        """

        self.ensure_connected()
        assert self._connection is not None

        cursor = self._connection.cursor()
        try:
            cursor.executemany(query, (tuple(params) for params in seq_of_parameters))
            self._connection.commit()
        finally:
            cursor.close()

    def executescript(self, script: str) -> None:
        """Execute a multi-statement script in a single transaction."""

        self.ensure_connected()
        assert self._connection is not None
        self._connection.executescript(script)
        self._connection.commit()
