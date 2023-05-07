from contextlib import suppress
from typing import Union
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
from tpcollections import Database, OrderedMapping

class TestExpiringDict(unittest.TestCase):
    def test_simple(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with Database(db_path) as db, db:
                d: OrderedMapping[str, Union[str, int]] = OrderedMapping(db)
                self.assertFalse(bool(d))
                self.assertEqual(tuple(d), ())
                self.assertEqual(tuple(d.keys()), ())
                self.assertEqual(tuple(d.items()), ())
                self.assertEqual(tuple(d.values()), ())
                self.assertEqual(len(d), 0)
                d['foo'] = 'bar'
                d['baz'] = 1337

            with Database(db_path) as db, db:
                d: OrderedMapping[str, Union[str, int]] = OrderedMapping(db)
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('foo', 'baz'))
                self.assertEqual(tuple(d.keys()), ('foo', 'baz'))
                self.assertEqual(tuple(d.items()), (('foo', 'bar'), ('baz', 1337)))
                self.assertEqual(tuple(d.values()), ('bar', 1337))
                self.assertEqual(len(d), 2)

                self.assertEqual(tuple(reversed(d)), ('baz', 'foo'))
                self.assertEqual(tuple(reversed(d.keys())), ('baz', 'foo'))
                self.assertEqual(
                    tuple(reversed(d.items())),
                    (('baz', 1337), ('foo', 'bar')),
                )
                self.assertEqual(tuple(reversed(d.values())), (1337, 'bar'))

                self.assertEqual(
                    tuple(d.keys(OrderedMapping.Order.KEY)),
                    ('baz', 'foo'),
                )
                self.assertEqual(
                    tuple(d.items(OrderedMapping.Order.KEY)),
                    (('baz', 1337), ('foo', 'bar')),
                )
                self.assertEqual(
                    tuple(d.values(OrderedMapping.Order.KEY)),
                    (1337, 'bar'),
                )

                self.assertEqual(
                    tuple(reversed(d.keys(OrderedMapping.Order.KEY))),
                    ('foo', 'baz'),
                )
                self.assertEqual(
                    tuple(reversed(d.items(OrderedMapping.Order.KEY))),
                    (('foo', 'bar'), ('baz', 1337)),
                )
                self.assertEqual(
                    tuple(reversed(d.values(OrderedMapping.Order.KEY))),
                    ('bar', 1337),
                )

            with Database(db_path) as db, db:
                d: OrderedMapping[str, Union[str, int]] = OrderedMapping(db)
                d['foo'] = 'barbar'

            with Database(db_path) as db, db:
                d: OrderedMapping[str, Union[str, int]] = OrderedMapping(db)
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('foo', 'baz'))
                self.assertEqual(tuple(d.keys()), ('foo', 'baz'))
                self.assertEqual(tuple(d.items()), (('foo', 'barbar'), ('baz', 1337)))
                self.assertEqual(tuple(d.values()), ('barbar', 1337))
                self.assertEqual(len(d), 2)

            with Database(db_path) as db, db:
                d: OrderedMapping[str, Union[str, int]] = OrderedMapping(db)
                del d['foo']

            with Database(db_path) as db, db:
                d: OrderedMapping[str, Union[str, int]] = OrderedMapping(db)
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('baz',))
                self.assertEqual(tuple(d.keys()), ('baz',))
                self.assertEqual(tuple(d.items()), (('baz', 1337),))
                self.assertEqual(tuple(d.values()), (1337,))
                self.assertEqual(len(d), 1)

            with self.assertRaises(KeyError):
                with Database(db_path) as db, db:
                    d: OrderedMapping[str, Union[str, int]] = OrderedMapping(db)
                    del d['foo']

            
            with Database(db_path) as db, db:
                d: OrderedMapping[str, Union[str, int]] = OrderedMapping(db)
                d['foo'] = 'spam'

            with Database(db_path) as db, db:
                d: OrderedMapping[str, Union[str, int]] = OrderedMapping(db)
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('baz', 'foo'))
                self.assertEqual(tuple(d.keys()), ('baz', 'foo'))
                self.assertEqual(tuple(d.items()), (('baz', 1337), ('foo', 'spam')))
                self.assertEqual(tuple(d.values()), (1337, 'spam'))
                self.assertEqual(len(d), 2)

    def test_transactions(self):
        with Database() as db:
            d: OrderedMapping[str, Union[str, int]] = OrderedMapping(db)
            d['foo'] = 'bar'
            with suppress(RuntimeError):
                with db:
                    d['baz'] = 1337
                    self.assertIn('baz', d)
                    self.assertEqual(d['baz'], 1337)
                    with suppress(RuntimeError):
                        with db:
                            del d['baz']
                            d['foo'] = 'beta'
                            self.assertNotIn('baz', d)
                            self.assertEqual(d['foo'], 'beta')
                            with suppress(RuntimeError):
                                with db:
                                    d['baz'] = 1338
                                    del d['foo']
                                    self.assertNotIn('foo', d)
                                    self.assertEqual(d['baz'], 1338)
                                    raise RuntimeError
                            self.assertNotIn('baz', d)
                            self.assertEqual(d['foo'], 'beta')
                            raise RuntimeError

                    self.assertIn('baz', d)
                    self.assertEqual(d['baz'], 1337)
                    self.assertEqual(d['foo'], 'bar')
                    d.clear()
                    self.assertNotIn('foo', d)
                    self.assertNotIn('baz', d)
                    self.assertFalse(d)
                    raise RuntimeError
            self.assertTrue(d)
            self.assertNotIn('baz', d)
            self.assertEqual(d['foo'], 'bar')

    def test_attachments(self):
        with TemporaryDirectory() as dir:
            dir = Path(dir)

            with Database(dir / 'alpha.db') as connection:
                connection.attach('beta', dir / 'beta.db')

                alpha: OrderedMapping[str, Union[str, int]] = OrderedMapping(
                    connection,
                    table='gamma',
                )
                beta: OrderedMapping[str, Union[str, int]] = OrderedMapping(
                    connection,
                    database='beta',
                    table='delta',
                )

                with connection:
                    alpha['epsilon'] = 'zeta'
                    beta['eta'] = 'theta'
                    with suppress(RuntimeError):
                        with connection:
                            alpha['epsilon'] = 'iota'
                            beta['eta'] = 'kappa'
                            raise RuntimeError

            with Database(dir / 'beta.db') as connection:
                connection.attach('alpha', dir / 'alpha.db')

                alpha: OrderedMapping[str, Union[str, int]] = OrderedMapping(
                    connection,
                    database='alpha',
                    table='gamma',
                )
                beta: OrderedMapping[str, Union[str, int]] = OrderedMapping(
                    connection,
                    table='delta',
                )

                self.assertEqual(alpha['epsilon'], 'zeta')
                self.assertEqual(beta['eta'], 'theta')

            

if __name__ == '__main__':
    unittest.main()
