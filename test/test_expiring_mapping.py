from time import time
from datetime import timedelta
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
from tpcollections import Database, ExpiringMapping

class TestExpiringDict(unittest.TestCase):
    def test_simple(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with Database(db_path) as db, db:
                d = ExpiringMapping(db, lifespan=timedelta(seconds=10))
                d.now_function(lambda: 10)
                d['alpha'] = 1
                d.now_function(lambda: 15)
                d['foo'] = 'bar'
                d.now_function(lambda: 20)
                d['baz'] = 1337

                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('baz', 'foo'))
                self.assertEqual(tuple(d.keys()), ('baz', 'foo'))
                self.assertEqual(tuple(d.items()), (('baz', 1337), ('foo', 'bar')))
                self.assertEqual(tuple(d.values()), (1337, 'bar'))
                self.assertEqual(len(d), 2)

                self.assertEqual(tuple(reversed(d)), ('foo', 'baz'))
                self.assertEqual(tuple(reversed(d.keys())), ('foo', 'baz'))
                self.assertEqual(
                    tuple(reversed(d.items())),
                    (('foo', 'bar'), ('baz', 1337)),
                )
                self.assertEqual(tuple(reversed(d.values())), ('bar', 1337))

            with Database(db_path) as db, db:
                d = ExpiringMapping(
                    db,
                    table='natural',
                    lifespan=timedelta(seconds=2),
                )
                d.now_function(lambda: int(time()) - 2)
                d['alpha'] = 1
                d.now_function(None)
                d['foo'] = 'bar'
                d['baz'] = 1337

                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('baz', 'foo'))
                self.assertEqual(tuple(d.keys()), ('baz', 'foo'))
                self.assertEqual(tuple(d.items()), (('baz', 1337), ('foo', 'bar')))
                self.assertEqual(tuple(d.values()), (1337, 'bar'))
                self.assertEqual(len(d), 2)

                self.assertEqual(tuple(reversed(d)), ('foo', 'baz'))
                self.assertEqual(tuple(reversed(d.keys())), ('foo', 'baz'))
                self.assertEqual(
                    tuple(reversed(d.items())),
                    (('foo', 'bar'), ('baz', 1337)),
                )
                self.assertEqual(tuple(reversed(d.values())), ('bar', 1337))

if __name__ == '__main__':
    unittest.main()
