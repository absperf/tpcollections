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
        '_database',
        '_table',
        '_serializer',
        '_order',
    )

    def __init__(
        self,
        connection: sqlite3.Connection,
        database: Identifier,
        table: Identifier,
        serializer: serializers.Serializer,
        order: Order,
    ) -> None:

        self._connection = connection
        self._database = database
        self._table = table
        self._serializer = serializer
        self._order = order
    
    def _iterator(self, order: str) -> Iterator[str]:
        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(
                f'SELECT key FROM {self._database}.{self._table} ORDER BY {self._order} {order}',
            ):
                yield self._serializer.loads(row[0])

    def __iter__(self) -> Iterator[str]:
        return self._iterator('ASC')

    def __reversed__(self) -> Iterator[str]:
        return self._iterator('DESC')

class _Values(Reversible, Iterable[Any]):
    __slots__ = (
        '_connection',
        '_database',
        '_table',
        '_serializer',
        '_order',
    )

    def __init__(
        self,
        connection: sqlite3.Connection,
        database: Identifier,
        table: Identifier,
        serializer: serializers.Serializer,
        order: Order,
    ) -> None:

        self._connection = connection
        self._database = database
        self._table = table
        self._serializer = serializer
        self._order = order

    def _iterator(self, order: str) -> Iterator[Any]:
        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(
                f'SELECT value FROM {self._database}.{self._table} ORDER BY {self._order} {order}',
            ):
                yield self._serializer.loads(row[0])

    def __iter__(self) -> Iterator[Any]:
        return self._iterator('ASC')

    def __reversed__(self) -> Iterator[Any]:
        return self._iterator('DESC')

class _Items(Reversible, Iterable[Tuple[str, Any]]):
    __slots__ = (
        '_connection',
        '_database',
        '_table',
        '_key_serializer',
        '_value_serializer',
        '_order',
    )

    def __init__(
        self,
        connection: sqlite3.Connection,
        database: Identifier,
        table: Identifier,
        key_serializer: serializers.Serializer,
        value_serializer: serializers.Serializer,
        order: Order,
    ) -> None:
        self._connection = connection
        self._database = database
        self._table = table
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._order = order
    
    def _iterator(self, order: str) -> Iterator[Tuple[str, Any]]:
        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'''
                SELECT key, value FROM {self._database}.{self._table}
                    ORDER BY {self._order} {order}
            '''):
                yield (
                    self._key_serializer.loads(row[0]),
                    self._value_serializer.loads(row[1]),
                )

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

        version = self._version
        previous_version = version

        with closing(self._connection.connection.cursor()) as cursor:
            if version < 1:
                cursor.execute(f'''
                    CREATE TABLE {self._database}.{self._table} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        key {db.ANY} UNIQUE NOT NULL,
                        value {db.ANY} NOT NULL) {db.STRICT}
                ''')
                version = 1

            if version > 1:
                raise ValueError('tpcollections is not forward compatible')

            if version != previous_version:
                self._version = version

            cursor.execute(
                f'''
                CREATE TRIGGER temp.{self._table + "_insert_trigger"}
                    AFTER INSERT ON {self._database}.{self._table}
                BEGIN
                    DELETE FROM {self._database}.{self._table} WHERE expire <= {db.UNIXEPOCH};
                END
                '''
            )

            cursor.execute(
                f'''
                CREATE TRIGGER temp.{self._table + "_update_trigger"}
                    AFTER UPDATE OF value ON {self._database}.{self._table}
                BEGIN
                    DELETE FROM {self._database}.{self._table} WHERE expire <= {db.UNIXEPOCH};
                END
                '''
            )

    def __bool__(self) -> bool:
        '''Check if the table is not empty.'''

        return len(self) > 0

    def keys(self, order: Order = Order.ID) -> _Keys:
        '''Iterate over keys in the table.
        '''

        return _Keys(
            connection=self._connection.connection,
            database=self._database,
            table=self._table,
            serializer=self._key_serializer,
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
            connection=self._connection.connection,
            database=self._database,
            table=self._table,
            serializer=self._value_serializer,
            order=order,
        )

    def items(self, order: Order = Order.ID) -> _Items:
        '''Iterate over keys and values in the table.
        '''

        return _Items(
            connection=self._connection.connection,
            database=self._database,
            table=self._table,
            key_serializer=self._key_serializer,
            value_serializer=self._value_serializer,
            order=order,
        )

    def __contains__(self, key: Any) -> bool:
        '''Check if the table contains the given key.
        '''

        with closing(self._connection.connection.cursor()) as cursor:
            cursor.execute(
                f'SELECT 1 FROM {self._database}.{self._table} WHERE key = ?',
                (self._key_serializer.dumps(key),),
            )
            return cursor.fetchone() is not None

    def __getitem__(self, key: str) -> Any:
        '''Fetch the key.
        '''

        with closing(self._connection.connection.cursor()) as cursor:
            for row in cursor.execute(
                f'SELECT value FROM {self._database}.{self._table} WHERE key = ?',
                (self._key_serializer.dumps(key),),
            ):
                return self._value_serializer.loads(row[0])
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        '''Set or replace the item.

        This also triggers cleaning up expired values.
        '''

        with closing(self._connection.connection.cursor()) as cursor:
            if sqlite3.sqlite_version_info >= (3, 24):
                cursor.execute(f'''
                        INSERT INTO {self._database}.{self._table} (key, value)
                            VALUES (?, ?)
                            ON CONFLICT (key) DO UPDATE
                            SET value=excluded.valu
                    ''',
                    (
                        self._key_serializer.dumps(key),
                        self._value_serializer.dumps(value),
                    ),
                )
            elif key in self:
                cursor.execute(f'''
                        UPDATE {self._database}.{self._table}
                            SET value=?2
                            WHERE key=?1
                    ''',
                    (
                        self._key_serializer.dumps(key),
                        self._value_serializer.dumps(value),
                    ),
                )
            else:
                cursor.execute(f'''
                        INSERT INTO {self._database}.{self._table} (key, value)
                            VALUES (?, ?)
                    ''',
                    (
                        self._key_serializer.dumps(key),
                        self._value_serializer.dumps(value),
                    ),
                )

    def __delitem__(self, key: str) -> None:
        '''Delete an item from the table.
        '''

        with closing(self._connection.connection.cursor()) as cursor:
            cursor.execute(
                f'DELETE FROM {self._database}.{self._table} WHERE key=?',
                (self._key_serializer.dumps(key),),
            )
            if cursor.rowcount != 1:
                raise KeyError(key)

    def clear(self) -> None:
        '''Delete all items from the table.
        '''

        with closing(self._connection.connection.cursor()) as cursor:
            cursor.execute(f'DELETE FROM {self._database}.{self._table}')
