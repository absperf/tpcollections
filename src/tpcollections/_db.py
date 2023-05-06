import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path
from types import TracebackType
from typing import ContextManager, Generator, List, Optional, Set, Type
from enum import auto, unique, Enum
import warnings

from ._util import Identifier

@contextmanager
def _savepoint(
    connection: sqlite3.Connection,
    name: Identifier = Identifier('tpcollection'),
) -> Generator[None, None, None]:
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
def _transaction(
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
    # filesystem.  Only use this if you know the database will not be written to
    # by any other process at all.
    IMMUTABLE = auto()

def _uri(
    path: Optional[Path] = None,
    mode: Mode = Mode.READ_WRITE,
) -> str:
    if path is None:
        return ':memory:'
    else:
        if path.is_absolute():
            uri = path.as_uri()
        else:
            uri = 'file:' + str(path)

        if mode is Mode.READ_ONLY:
            uri += '?mode=ro'
        elif mode is Mode.IMMUTABLE:
            uri += '?immutable=1'
        else:
            uri += '?mode=rwc'

        return uri

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
        '_mode',
        '_connection',
        '_timeout',
        '_memory',
        '__weakref__'
    )

    def __init__(self,
        path: Optional[Path] = None,
        mode: Mode = Mode.READ_WRITE,
        timeout: float = 5.0,
        **kwargs,
    ) -> None:
        if path is None:
            self._mode = mode.READ_WRITE
            self._memory = False
        else:
            self._mode = mode
            self._memory = True

        self._uri = _uri(path, mode)
        self._timeout = timeout

    @property
    def read_only(self) -> bool:
        return self._mode is not Mode.READ_WRITE

    @property
    def memory(self) -> bool:
        return self._memory

    def __call__(self) -> ContextManager['Connection']:
        return _connect(
            uri=self._uri,
            mode=self._mode,
            timeout=self._timeout,
            memory=self._memory,
        )

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

if sqlite3.sqlite_version_info >= (3, 37):
    STRICT = 'STRICT'
    ANY = 'ANY'

if sqlite3.sqlite_version_info >= (3, 8, 2):
    WITHOUT_ROWID = 'WITHOUT ROWID'

if sqlite3.sqlite_version_info >= (3, 38):
    UNIXEPOCH = 'UNIXEPOCH()'
else:
    UNIXEPOCH = "CAST(strftime('%s', 'now') AS INTEGER)"

_APPLICATION_ID = -1238962565

STRICT_WITHOUT_ROWID = ', '.join(part for part in (STRICT, WITHOUT_ROWID) if part)

class Connection:
    '''The actual connection object, as a MutableMapping[str, Any].

    Items are expired when a value is inserted or updated.  Deletion or
    postponement does not expire items.
    '''

    __slots__ = (
        '_connection',
        '_attachments',
        '_mode',
        '_transactions',
        '_has_attachments',
        '_memory',
        '__weakref__',
    )

    def __init__(self,
        connection: sqlite3.Connection,
        mode: Mode,
        memory: bool,
    ) -> None:
        self._connection = connection
        self._attachments: Set[str] = {'main'}
        self._mode = mode
        self._transactions: List[ContextManager[None]] = []
        self._has_attachments = False
        self._memory = memory
        self._init(Identifier('main'))

    def _init(self, database: Identifier) -> None:
        '''Initialize a database or attachment.
        '''
        with self.cursor() as cursor:
            application_id = next(cursor.execute('PRAGMA application_id'))[0]
            if application_id == 0:
                cursor.execute(f'PRAGMA {database}.application_id = {_APPLICATION_ID}')
            elif application_id != _APPLICATION_ID:
                raise ValueError(f'illegal application ID {application_id}')

            user_version = next(cursor.execute(f'PRAGMA {database}.user_version'))[0]
            if user_version == 0:
                cursor.execute(f'PRAGMA {database}.user_version = 1')
            elif user_version != 1:
                raise ValueError(f'user_version for {database} was {user_version}')

            if not self.read_only:
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {database}.tpcollections (
                        name TEXT PRIMARY KEY NOT NULL,
                        type TEXT NOT NULL,
                        version INTEGER NOT NULL
                    ) {STRICT_WITHOUT_ROWID}
                ''')

    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    @contextmanager
    def cursor(self) -> Generator[sqlite3.Cursor, None, None]:
        with closing(self._connection.cursor()) as cursor:
            yield cursor

    @property
    def read_only(self) -> bool:
        return self._mode is not Mode.READ_WRITE

    def __enter__(self) -> None:
        if self._transactions:
            new_transaction = _savepoint(self._connection)
        else:
            new_transaction = _transaction(self._connection, self.read_only)

        new_transaction.__enter__()
        self._transactions.append(new_transaction)

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        return self._transactions.pop().__exit__(type, value, traceback)

    def attach(
        self,
        database: str,
        path: Optional[Path] = None,
    ) -> None:
        uri = _uri(path, self._mode)
        database_id = Identifier(database)

        with closing(self._connection.cursor()) as cursor:
            if __debug__ and self._transactions:
                warnings.warn(
                    'Attaching a database inside a transaction can prevent '
                    'transactions from being atomic.'
                )

            if not (self._memory or self._has_attachments or self.read_only):
                # disable WAL mode when attaching databases
                cursor.execute('PRAGMA main.journal_mode=DELETE')
                cursor.execute('PRAGMA main.synchronous=FULL')
                self._has_attachments = True

            cursor.execute(f'ATTACH ? AS {database_id}', (uri,))
            try:
                if not (self.read_only or self._memory):
                    cursor.execute(f'PRAGMA {database_id}.journal_mode=DELETE')
                    cursor.execute(f'PRAGMA {database_id}.synchronous=FULL')

                self._init(database_id)
            except:
                cursor.execute(f'DETACH {database_id}')
                raise

@contextmanager
def _connect(
    uri: str,
    mode: Mode,
    timeout: float,
    memory: bool,
) -> Generator[Connection, None, None]:
    with closing(sqlite3.connect(
        uri,
        timeout=timeout,
        isolation_level=None,
        check_same_thread=__debug__,
        uri=True,
        cached_statements=1024,
    )) as connection, closing(connection.cursor()) as cursor:
        try:
            if not memory and mode is Mode.READ_WRITE:
                cursor.execute('PRAGMA main.journal_mode=WAL')
                cursor.execute('PRAGMA main.synchronous=NORMAL')

            yield Connection(
                connection=connection,
                mode=mode,
                memory=memory,
            )
        finally:
            if not memory and mode is Mode.READ_WRITE:
                cursor.execute('PRAGMA analysis_limit=8192')
                cursor.execute('PRAGMA optimize')

class _Base:
    __slots__ = (
        '_connection',
        '_database',
        '_table',
    )

    def __init__(
        self,
        connection: Connection,
        database: Identifier,
        table: Identifier,
        type: str,
    ) -> None:
        self._connection = connection
        self._database = database
        self._table = table

        with closing(connection.connection.cursor()) as cursor:
            cursor.execute(f'''
                SELECT type
                    FROM {database}.tpcollections
                    WHERE name = ?
            ''', (table.value,))
            row = cursor.fetchone()
            if row is None:
                cursor.execute(
                    f'SELECT 1 FROM {database}.sqlite_master WHERE name = ?',
                    (table.value,)
                )
                if cursor.fetchone() is not None:
                    raise NameError(f'table {table} already exists')
                
                cursor.execute(f'''
                    INSERT INTO {database}.tpcollections 
                        (name, type, version)
                        VALUES (?, ?, ?)
                ''', (table.value, type, 0))
            else:
                existing_type, = row

                if type != existing_type:
                    raise ValueError(f'Tried to open {database}.{table}'
                        f' as {type}, but it already existed as {existing_type}')
    @property
    def database(self) -> str:
        return self._database.value

    @property
    def table(self) -> str:
        return self._table.value

    @property
    def _version(self) -> int:
        with closing(self._connection.connection.cursor()) as cursor:
            cursor.execute(f'''
                SELECT version
                    FROM {self._database}.tpcollections
                    WHERE name = ?
            ''', (self._table.value,))
            version, = cursor.fetchone()
            return version

    @_version.setter
    def _version(self, value: int) -> None:
        assert not self._connection.read_only
        with closing(self._connection.connection.cursor()) as cursor:
            cursor.execute(f'''
                UPDATE {self._database}.tpcollections
                    SET version = ?
                    WHERE name = ?
            ''', (value, self._table.value))

