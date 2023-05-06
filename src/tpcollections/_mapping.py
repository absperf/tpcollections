import sqlite3
from typing import (
    Any,
    Generic,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Reversible,
    Tuple,
    TypeVar,
    ValuesView,
    MutableMapping,
)
from enum import unique, Enum

from ._util import Identifier

from . import _db, _serializers

Item = TypeVar('Item')
Key = TypeVar('Key')
Value = TypeVar('Value')

class _ViewsBase(Reversible[Item], Iterable[Item]):
    __slots__ = ()

    _connection: _db.Connection
    _database: Identifier
    _table : Identifier

    def _iterator(self, order: str) -> Iterator[Item]:
        raise NotImplementedError

    def __len__(self) -> int:
        with self._connection.cursor() as cursor:
            len, = cursor.execute(f'SELECT COUNT(*) FROM {self._database}.{self._table}').fetchone()
            return len

    def __iter__(self) -> Iterator[Item]:
        return self._iterator('ASC')

    def __reversed__(self) -> Iterator[Item]:
        return self._iterator('DESC')

class Keys(_ViewsBase[Key], KeysView[Key]):
    __slots__ = (
        '_connection',
        '_database',
        '_table',
        '_serializer',
        '_order',
    )

    def __init__(
        self,
        connection: _db.Connection,
        database: Identifier,
        table: Identifier,
        serializer: _serializers.Serializer,
        order: str,
    ) -> None:
        self._connection = connection
        self._database = database
        self._table = table
        self._serializer = serializer
        self._order = order
    
    def _iterator(self, order: str) -> Iterator[Key]:
        with self._connection.cursor() as cursor:
            for key, in cursor.execute(
                f'SELECT key FROM {self._database}.{self._table} ORDER BY {self._order} {order}',
            ):
                yield self._serializer.loads(key)

class Values(_ViewsBase[Any], ValuesView[Value]):
    __slots__ = (
        '_connection',
        '_database',
        '_table',
        '_serializer',
        '_order',
    )

    def __init__(
        self,
        connection: _db.Connection,
        database: Identifier,
        table: Identifier,
        serializer: _serializers.Serializer,
        order: str,
    ) -> None:
        self._connection = connection
        self._database = database
        self._table = table
        self._serializer = serializer
        self._order = order

    def _iterator(self, order: str) -> Iterator[Value]:
        with self._connection.cursor() as cursor:
            for value, in cursor.execute(
                f'SELECT value FROM {self._database}.{self._table} ORDER BY {self._order} {order}',
            ):
                yield self._serializer.loads(value)

class Items(_ViewsBase[Tuple[Key, Value]], ItemsView[Key, Value]):
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
        connection: _db.Connection,
        database: Identifier,
        table: Identifier,
        key_serializer: _serializers.Serializer,
        value_serializer: _serializers.Serializer,
        order: str,
    ) -> None:
        self._connection = connection
        self._database = database
        self._table = table
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._order = order
    
    def _iterator(self, order: str) -> Iterator[Tuple[Key, Value]]:
        with self._connection.cursor() as cursor:
            for key, value in cursor.execute(f'''
                SELECT key, value FROM {self._database}.{self._table}
                    ORDER BY {self._order} {order}
            '''):
                yield (
                    self._key_serializer.loads(key),
                    self._value_serializer.loads(value),
                )

class _MappingBase(_db._Base, MutableMapping[Key, Value]):
    '''A database mapping.
    '''

    __slots__ = (
        '_key_serializer',
        '_value_serializer',
    )
    def __init__(self,
        connection: _db.Connection,
        database: Identifier,
        table: Identifier,
        key_serializer,
        value_serializer,
        type: str,
    ) -> None:
        super().__init__(connection, Identifier(database), Identifier(table), type)

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

    def __len__(self) -> int:
        '''Get the count of keys in the table.
        '''

        with self._connection.cursor() as cursor:
            len, = cursor.execute(f'SELECT COUNT(*) FROM {self._database}.{self._table}').fetchone()
            return len


    def __bool__(self) -> bool:
        '''Check if the table is not empty.'''

        return len(self) > 0

    def keys(self, order: str) -> Keys[Key]:
        '''Iterate over keys in the table.
        '''

        return Keys(
            connection=self._connection,
            database=self._database,
            table=self._table,
            serializer=self._key_serializer,
            order=order,
        )

    def values(self, order: str) -> Values[Value]:
        '''Iterate over values in the table.
        '''

        return Values(
            connection=self._connection,
            database=self._database,
            table=self._table,
            serializer=self._value_serializer,
            order=order,
        )

    def items(self, order: str) -> Items[Key, Value]:
        '''Iterate over keys and values in the table.
        '''

        return Items(
            connection=self._connection,
            database=self._database,
            table=self._table,
            key_serializer=self._key_serializer,
            value_serializer=self._value_serializer,
            order=order,
        )

    def __contains__(self, key: Key) -> bool:
        '''Check if the table contains the given key.
        '''

        with self._connection.cursor() as cursor:
            cursor.execute(
                f'SELECT 1 FROM {self._database}.{self._table} WHERE key = ?',
                (self._key_serializer.dumps(key),),
            )
            return cursor.fetchone() is not None

    def __getitem__(self, key: Key) -> Value:
        '''Fetch the key.
        '''

        with self._connection.cursor() as cursor:
            for row in cursor.execute(
                f'SELECT value FROM {self._database}.{self._table} WHERE key = ?',
                (self._key_serializer.dumps(key),),
            ):
                return self._value_serializer.loads(row[0])
        raise KeyError(key)

    def __setitem__(self, key: Key, value: Value) -> None:
        '''Set or replace the item.

        This also triggers cleaning up expired values.
        '''

        with self._connection.cursor() as cursor:
            if sqlite3.sqlite_version_info >= (3, 24):
                cursor.execute(f'''
                        INSERT INTO {self._database}.{self._table} (key, value)
                            VALUES (?, ?)
                            ON CONFLICT (key) DO UPDATE
                            SET value=excluded.value
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

    def __delitem__(self, key: Key) -> None:
        '''Delete an item from the table.
        '''

        with self._connection.cursor() as cursor:
            cursor.execute(
                f'DELETE FROM {self._database}.{self._table} WHERE key=?',
                (self._key_serializer.dumps(key),),
            )
            if cursor.rowcount != 1:
                raise KeyError(key)

    def clear(self) -> None:
        '''Delete all items from the table.
        '''

        with self._connection.cursor() as cursor:
            cursor.execute(f'DELETE FROM {self._database}.{self._table}')

class OrderedMapping(_MappingBase[Key, Value]):
    '''A database mapping.
    '''

    __slots__ = ()

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

    def __init__(self,
        connection: _db.Connection,
        database: str = 'main',
        table: str = 'orderedmapping',
        key_serializer: _serializers.Serializer = _serializers.deterministic_json,
        value_serializer: _serializers.Serializer = _serializers.pickle,
    ) -> None:

        super().__init__(
            connection=connection,
            database=Identifier(database),
            table=Identifier(table),
            type='orderedmapping',
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )

        version = self._version
        previous_version = version

        if version < 1:
            with self._connection.cursor() as cursor:
                cursor.execute(f'''
                    CREATE TABLE {self._database}.{self._table} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        key {_db.ANY} UNIQUE NOT NULL,
                        value {_db.ANY} NOT NULL) {_db.STRICT}
                ''')
                version = 1

        if version > 1:
            raise ValueError('tpcollections is not forward compatible')

        if version != previous_version:
            self._version = version

    def keys(self, order: Order = Order.ID) -> Keys[Key]:
        '''Iterate over keys in the table.
        '''
        return super().keys(order)

    def __iter__(self) -> Iterator[Key]:
        return iter(self.keys())

    def __reversed__(self) -> Iterator[Key]:
        return reversed(self.keys())

    def values(self, order: Order = Order.ID) -> Values[Value]:
        '''Iterate over values in the table.
        '''
        return super().values(order)

    def items(self, order: Order = Order.ID) -> Items[Key, Value]:
        '''Iterate over keys and values in the table.
        '''
        return super().items(order)

class Mapping(_MappingBase[Key, Value]):
    '''A database mapping ordered by key.

    The table is smaller than OrderedMapping, which needs to keep a separate
    index for the keys and can't use WITHOUT ROWID.  This does not remember
    insertion order, and is always ordered by key insertion order.
    '''

    __slots__ = ()

    def __init__(self,
        connection: _db.Connection,
        database: str = 'main',
        table: str = 'mapping',
        key_serializer: _serializers.Serializer = _serializers.deterministic_json,
        value_serializer: _serializers.Serializer = _serializers.pickle,
    ) -> None:

        super().__init__(
            connection=connection,
            database=Identifier(database),
            table=Identifier(table),
            type='mapping',
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )

        version = self._version
        previous_version = version

        if version < 1:
            with self._connection.cursor() as cursor:
                cursor.execute(f'''
                    CREATE TABLE {self._database}.{self._table} (
                        key {_db.ANY} PRIMARY KEY UNIQUE NOT NULL,
                        value {_db.ANY} NOT NULL) {_db.STRICT_WITHOUT_ROWID}
                ''')
                version = 1

        if version > 1:
            raise ValueError('tpcollections is not forward compatible')

        if version != previous_version:
            self._version = version

    def keys(self) -> Keys[Key]:
        '''Iterate over keys in the table.
        '''
        return super().keys('key')

    def __iter__(self) -> Iterator[Key]:
        return iter(self.keys())

    def __reversed__(self) -> Iterator[Key]:
        return reversed(self.keys())

    def values(self) -> Values[Value]:
        '''Iterate over values in the table.
        '''
        return super().values('key')

    def items(self) -> Items[Key, Value]:
        '''Iterate over keys and values in the table.
        '''
        return super().items('key')
