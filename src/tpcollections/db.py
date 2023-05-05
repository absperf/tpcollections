#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sqlite3
from collections.abc import MutableMapping
from functools import wraps
from contextlib import ExitStack, closing, contextmanager
from datetime import timedelta
from pathlib import Path
from types import TracebackType
from typing import Any, ContextManager, Generator, Iterable, Iterator, List, Optional, Reversible, Set, Tuple, Type, Union
from weakref import finalize
from enum import auto, unique, Enum

from tpcollections.util import Identifier

@contextmanager
def savepoint(
    connection: sqlite3.Connection,
    name: Identifier | str = Identifier('tpcollection'),
) -> Generator[None, None, None]:
    if isinstance(name, str):
        name = Identifier(name)

    with closing(connection.cursor()) as cursor:
        cursor.execute(f'SAVEPOINT {name}')
        try:
            yield
        except:
            cursor.execute(f'ROLLBACK TO {name}')
            raise
        finally:
            cursor.execute(f'RELEASE {name}')

@contextmanager
def transaction(
    connection: sqlite3.Connection,
    read_only: bool = False,
) -> Generator[None, None, None]:
    with closing(connection.cursor()) as cursor:
        if read_only:
            cursor.execute('BEGIN')
        else:
            cursor.execute('BEGIN IMMEDIATE')

        try:
            yield
        except:
            cursor.execute('ROLLBACK')
            raise
        else:
            cursor.execute('COMMIT')

@unique
class Mode(Enum):
    # Database may be read or written by this connection.
    READ_WRITE = auto()

    # Database may be read by this connection but not written.  Other
    # connections may write to it, and this connection will reflect those
    # changes.  This connection may still establish locks, and therefore
    # might need to write to the filesystem. This mode is mostly useful
    # for performance reasons, as various read-only connections may read in
    # parallel.
    READ_ONLY = auto()

    # The database will not have any writes or locks done to it or its
    # filesystem.  Other connections are expected to not write to the database
    # at all.
    IMMUTABLE = auto()

@contextmanager
def _connect(
    uri: str,
    read_only: bool,
    wal: bool,
    timeout: float,
) -> Generator[sqlite3.Connection, None, None]:
    with (
        closing(sqlite3.connect(uri, )) as connection,
        closing(connection.cursor()) as cursor,
    ):
        try:
            if wal and not read_only:
                cursor.execute('PRAGMA journal_mode=WAL')
                cursor.execute('PRAGMA synchronous=NORMAL')

            yield connection
        finally:
            if not read_only:
                cursor.execute('PRAGMA analysis_limit=8192')
                cursor.execute('PRAGMA optimize')


class Database:
    """
    A sqlite connection manager.

    This can be called to return a simple context manager that opens and closes
    a connection (useful for getting connection per thread), or it can be used
    directly as a context manager.

    If any nestable or concurrent use is desired, this must be called.  It is
    not otherwise re-entrant.
    """

    __slots__ = (
        '_uri',
        '_read_only',
        '_connection',
        '__weakref__'
    )

    def __init__(self,
        path: Optional[Path] = None,
        mode: Mode = Mode.READ_WRITE,
        wal: bool = False,
        timeout: float = 5.0,
        **kwargs,
    ) -> None:
        assert not (path is None and mode is not Mode.READ_WRITE), (
            'An in-memory database must be read-write'
        )

        if path is None:
            self._uri = ':memory:'
            self._read_only = False
        else:
            if path.is_absolute():
                uri = path.as_uri()
            else:
                uri = 'file:' + str(path)

            self._read_only = mode is not Mode.READ_WRITE

            if mode is Mode.READ_ONLY:
                uri += '?mode=ro'
            elif mode is Mode.IMMUTABLE:
                uri += '?immutable=1'

            self._uri = uri

    @property
    def read_only(self) -> bool:
        return self._read_only

    def __call__(self) -> ContextManager['Connection']:
        raise NotImplementedError

    def __enter__(self) -> 'Connection':
        assert not hasattr(self, '_connection'), (
            'Database is not a nestable context manager, call it instead'
        )
        self._connection = self()
        return self._connection.__enter__()

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        try:
            return self._connection.__exit__(type, value, traceback)
        finally:
            del self._connection


STRICT: str = ''
ANY: str = 'BLOB'
WITHOUT_ROWID: str = ''
_strict_without_rowid_trailers: List[str] = []

if sqlite3.sqlite_version_info >= (3, 37):
    _strict_without_rowid_trailers.append('STRICT')
    STRICT = 'STRICT'
    ANY = 'ANY'

if sqlite3.sqlite_version_info >= (3, 8, 2):
    _strict_without_rowid_trailers.append('WITHOUT ROWID')
    WITHOUT_ROWID = 'WITHOUT ROWID'

if sqlite3.sqlite_version_info >= (3, 38):
    UNIXEPOCH = 'UNIXEPOCH()'
else:
    UNIXEPOCH = "CAST(strftime('%s', 'now') AS INTEGER)"

APPLICATION_ID = -1238962565

STRICT_WITHOUT_ROWID = ', '.join(_strict_without_rowid_trailers)

del _strict_without_rowid_trailers

class Connection(MutableMapping):
    '''The actual connection object, as a MutableMapping[str, Any].

    Items are expired when a value is inserted or updated.  Deletion or
    postponement does not expire items.
    '''

    __slots__ = (
        '_connection',
        '_attachments',
        '_read_only',
        '_transactions',
        '__weakref__',
    )

    def __init__(self,
        connection: sqlite3.Connection,
        read_only: bool,
    ) -> None:
        self._connection = connection
        self._attachments: Set[str] = {'main'}
        self._read_only = read_only
        self._transactions: List[ContextManager[None]] = []

    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    @property
    def read_only(self) -> bool:
        return self._read_only

    def __enter__(self) -> None:
        if self._transactions:
            new_transaction = savepoint(self._connection)
        else:
            new_transaction = transaction(self._connection, self._read_only)

        self._transactions.append(new_transaction)

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        return self._transactions.pop().__exit__(type, value, traceback)

class Base:
    __slots__ = (
        '_connection',
        '_database',
    )

    def __init__(
        self,
        connection: Connection,
        database: Union[Identifier, str] = 'main',
    ) -> None:
        self._connection = connection
        self._database = Identifier(database)

        with closing(connection.connection.cursor()) as cursor:
            application_id = next(cursor.execute('PRAGMA application_id'))[0]
            if application_id == 0:
                cursor.execute(f'PRAGMA application_id = {APPLICATION_ID}')
            elif application_id != APPLICATION_ID:
                raise ValueError(f'illegal application ID {application_id}')

            user_version = next(cursor.execute('PRAGMA user_version'))[0]
            if user_version == 0:
                cursor.execute(f'PRAGMA user_version = {APPLICATION_ID}')
            elif user_version != APPLICATION_ID:
                raise ValueError(f'illegal application ID {user_version}')

        user_version = next(cursor.execute('PRAGMA user_version'))[0]
