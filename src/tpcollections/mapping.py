#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the Apache License, Version 2.0
#
# http://opensource.org/licenses/apache2.0.php
#
# This code was inspired by:
#  * http://code.activestate.com/recipes/576638-draft-for-an-sqlite3-based-dbm/
#  * http://code.activestate.com/recipes/526618/


import json
import sqlite3
from collections.abc import MutableMapping
from contextlib import ExitStack, closing, contextmanager
from datetime import timedelta
from types import TracebackType
from typing import Any, Generator, Iterable, Iterator, Optional, Reversible, Tuple, Type, Union
from weakref import finalize
from enum import unique, Enum

from tpcollections.util import Identifier

from . import db, serializers

@unique
class Order(str, Enum):
    '''An ordering enum for iteration methods.
    '''

    ID = 'id'
    KEY = 'key'

    def __str__(self) -> str:
        return self.value

    def __format__(self, format_spec: str) -> str:
        return self.value.__format__(format_spec)

class _Keys(Reversible, Iterable[str]):
    __slots__ = (
        '_connection',
        '_table',
        '_order',
    )

    def __init__(
        self,
        connection: sqlite3.Connection,
        table: str,
        order: Order,
    ) -> None:

        self._connection = connection
        self._table = table
        self._order = order
    
    def _iterator(self, order: str) -> Iterator[str]:
        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(
                f'SELECT key FROM "{self._table}" ORDER BY {self._order} {order}',
            ):
                yield row[0]

    def __iter__(self) -> Iterator[str]:
        return self._iterator('ASC')

    def __reversed__(self) -> Iterator[str]:
        return self._iterator('DESC')

class _Values(Reversible, Iterable[Any]):
    __slots__ = (
        '_connection',
        '_table',
        '_serializer',
        '_order',
    )

    def __init__(
        self,
        connection: sqlite3.Connection,
        table: str,
        serializer: Any,
        order: Order,
    ) -> None:

        self._connection = connection
        self._table = table
        self._serializer = serializer
        self._order = order

    def _iterator(self, order: str) -> Iterator[Any]:
        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(
                f'SELECT value FROM "{self._table}" ORDER BY {self._order} {order}',
            ):
                yield self._serializer.loads(row[0])

    def __iter__(self) -> Iterator[Any]:
        return self._iterator('ASC')

    def __reversed__(self) -> Iterator[Any]:
        return self._iterator('DESC')

class _Items(Reversible, Iterable[Tuple[str, Any]]):
    __slots__ = (
        '_connection',
        '_table',
        '_serializer',
        '_order',
    )

    def __init__(
        self,
        connection: sqlite3.Connection,
        table: str,
        serializer: Any,
        order: Order,
    ) -> None:
        self._connection = connection
        self._table = table
        self._serializer = serializer
        self._order = order
    
    def _iterator(self, order: str) -> Iterator[Tuple[str, Any]]:
        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'''
                SELECT key, value FROM "{self._table}"
                    ORDER BY {self._order} {order}
            '''):
                yield row[0], self._serializer.loads(row[1])

    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        return self._iterator('ASC')

    def __reversed__(self) -> Iterator[Tuple[str, Any]]:
        return self._iterator('DESC')

class Mapping(db.Base, MutableMapping):
    '''A database mapping.
    '''

    __slots__ = (
        '_key_serializer',
        '_value_serializer',
    )

    def __init__(self,
        connection: db.Connection,
        database: Union[Identifier, str] = 'main',
        table: Union[Identifier, str] = 'mapping',
        key_serializer: serializers.Serializer = serializers.deterministic_json,
        value_serializer: serializers.Serializer = serializers.pickle,
    ) -> None:
        super().__init__(connection, database, table, 'mapping')

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

            user_version = next(cursor.execute('PRAGMA user_version'))[0]

            if user_version < 1:
                # Attempt to migrate, because of pre-6.1 versions.  We can't
                # otherwise tell the difference between a fresh database and a
                # pre-set one.
                cursor.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (self._table,),
                )
                migrate = bool(cursor.fetchall())

                if migrate:
                    cursor.execute(f'''
                        DROP INDEX IF EXISTS "{self._safe_table}_expire_index"
                    ''')
                    cursor.execute(f'''
                        DROP TRIGGER IF EXISTS "{self._safe_table}_insert_trigger"
                    ''')
                    cursor.execute(f'''
                        DROP TRIGGER IF EXISTS "{self._safe_table}_update_trigger"
                    ''')
                    cursor.execute(f'''
                        ALTER TABLE "{self._safe_table}"
                        RENAME TO "{self._safe_table}_v0"
                    ''')


                cursor.execute(f'''
                    CREATE TABLE "{self._safe_table}" (
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        key TEXT UNIQUE NOT NULL,
                        expire INTEGER NOT NULL,
                        value {_valuetype} NOT NULL){_trailer}
                ''')

                cursor.execute(f'''
                    CREATE INDEX "{self._safe_table}_expire_index"
                     ON "{self._safe_table}" (expire)
                ''')

                cursor.execute(
                    f'''
                    CREATE TRIGGER "{self._safe_table}_insert_trigger"
                        AFTER INSERT ON "{self._safe_table}"
                    BEGIN
                        DELETE FROM "{self._safe_table}" WHERE expire <= {_unixepoch};
                    END
                    '''
                )

                cursor.execute(
                    f'''
                    CREATE TRIGGER "{self._safe_table}_update_trigger"
                        AFTER UPDATE OF value ON "{self._safe_table}"
                    BEGIN
                        DELETE FROM "{self._safe_table}" WHERE expire <= {_unixepoch};
                    END
                    '''
                )

                if migrate:
                    cursor.execute(f'''
                        INSERT INTO "{self._safe_table}"
                            (key, expire, value)
                        SELECT key, expire, value
                            FROM "{self._safe_table}_v0"
                    ''')
                    cursor.execute(f'DROP TABLE "{self._safe_table}_v0"')

                cursor.execute('PRAGMA user_version = 1')

                user_version = 1

            if user_version > 1:
                raise ValueError(
                    'this version of tpcollections is not'
                    ' compatible with this schema version'
                )
    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    @property
    def lifespan(self) -> timedelta:
        '''The current lifespan.

        Changing this will change the calculated expiration time of future set
        items.  It will not retroactively apply to existing items unless you explicitly
        postpone them.
        '''
        return timedelta(seconds=self._lifespan)

    @lifespan.setter
    def lifespan(self, value: timedelta) -> None:
        self._lifespan = value.total_seconds()

    def __len__(self) -> int:
        '''Get the count of keys in the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'SELECT COUNT(*) FROM "{self._safe_table}"'):
                return row[0]
        return 0

    def __bool__(self) -> bool:
        '''Check if the table is not empty.'''

        return len(self) > 0

    def keys(self, order: Order = Order.ID) -> _Keys:
        '''Iterate over keys in the table.
        '''

        return _Keys(
            connection=self._connection,
            table=self._safe_table,
            order=order,
        )

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __reversed__(self) -> Iterator[str]:
        return reversed(self.keys())

    def values(self, order: Order = Order.ID) -> _Values:
        '''Iterate over values in the table.
        '''

        return _Values(
            connection=self._connection,
            table=self._safe_table,
            serializer=self._serializer,
            order=order,
        )

    def items(self, order: Order = Order.ID) -> _Items:
        '''Iterate over keys and values in the table.
        '''

        return _Items(
            connection=self._connection,
            table=self._safe_table,
            serializer=self._serializer,
            order=order,
        )

    def __contains__(self, key: str) -> bool:
        '''Check if the table contains the given key.
        '''

        with closing(self._connection.cursor()) as cursor:
            for _ in cursor.execute(
                f'SELECT 1 FROM "{self._safe_table}" WHERE key = ?',
                (key,),
            ):
                return True
        return False

    def __getitem__(self, key: str) -> Any:
        '''Fetch the key.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(
                f'SELECT value FROM "{self._safe_table}" WHERE key = ?', (key,)
            ):
                return self._serializer.loads(row[0])
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        '''Set or replace the item.

        This also triggers cleaning up expired values.
        '''

        with closing(self._connection.cursor()) as cursor:
            if sqlite3.sqlite_version_info >= (3, 24):
                cursor.execute(f'''
                        INSERT INTO "{self._safe_table}" (key, expire, value)
                            VALUES (?, {_unixepoch} + ?, ?)
                            ON CONFLICT (key) DO UPDATE
                            SET value=excluded.value, expire=excluded.expire
                    ''',
                    (key, self._lifespan, self._serializer.dumps(value)),
                )
            elif key in self:
                cursor.execute(f'''
                        UPDATE "{self._safe_table}"
                            SET expire={_unixepoch} + ?,
                                value=?
                            WHERE key=?
                    ''',
                    (self._lifespan, self._serializer.dumps(value), key),
                )
            else:
                cursor.execute(f'''
                        INSERT INTO "{self._safe_table}" (key, expire, value)
                            VALUES (?, {_unixepoch} + ?, ?)
                    ''',
                    (key, self._lifespan, self._serializer.dumps(value)),
                )

    def __delitem__(self, key: str) -> None:
        '''Delete an item from the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            cursor.execute(f'DELETE FROM "{self._safe_table}" WHERE key=?', (key,))
            if cursor.rowcount != 1:
                raise KeyError(key)

    def clear(self) -> None:
        '''Delete all items from the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            cursor.execute(f'DELETE FROM "{self._safe_table}"')

    def postpone(self, key: str) -> None:
        '''Push back the expiration date of the given entry, if it exists.
        '''
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                f'UPDATE "{self._safe_table}" SET expire={_unixepoch} + ? WHERE key=?',
                (self._lifespan, key),
            )

    def postpone_all(self) -> None:
        '''Push back the expiration date of all entries at once.
        '''
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                f'UPDATE "{self._safe_table}" SET expire={_unixepoch} + ?',
                (self._lifespan,),
            )
