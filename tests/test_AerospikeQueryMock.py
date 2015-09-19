import unittest

from AerospikeClientMock import AerospikeClientMock, AerospikePredicatesMock


class TestAerospikeQueryMock(unittest.TestCase):
    def setUp(self):
        self.asm = AerospikeClientMock()
        self.asm.put(("a", "b", 1), {"a": 1, "b": 1})
        self.asm.put(("a", "b", 2), {"a": 2, "b": 2})
        self.asm.put(("a", "b", 3), {"a": 3, "b": 3})
        self.asm.put(("a", "c", 4), {"a": 4, "b": 4})

    def test_query(self):
        query = self.asm.query('a', 'b')
        query.select('a', 'c')
        self.assertEqual(
            [(('a', 'b', 3), {'a': 3, 'c': None}, {'gen': 1, 'ttl': 0}),
             (('a', 'b', 2), {'a': 2, 'c': None}, {'gen': 1, 'ttl': 0}),
             (('a', 'b', 1), {'a': 1, 'c': None}, {'gen': 1, 'ttl': 0})],
            query.results())

    def test_query_namespace(self):
        query = self.asm.query('a')
        query.select('a', 'b')
        self.assertEqual(
            [(('a', 'b', 3), {'a': 3, 'b': 3}, {'gen': 1, 'ttl': 0}),
             (('a', 'b', 2), {'a': 2, 'b': 2}, {'gen': 1, 'ttl': 0}),
             (('a', 'c', 4), {'a': 4, 'b': 4}, {'gen': 1, 'ttl': 0}),
             (('a', 'b', 1), {'a': 1, 'b': 1}, {'gen': 1, 'ttl': 0})],
            query.results())

    def test_query_callback(self):
        result = []

        def callback(data):
            (key, meta, bins) = data
            result.append((key, meta, bins))

        query = self.asm.query('a', 'b')
        query.select('a', 'c')
        query.foreach(callback)
        self.assertEqual(
            [((('a', 'b', 3), {'a': 3, 'c': None}, {'gen': 1, 'ttl': 0}),
              (('a', 'b', 2), {'a': 2, 'c': None}, {'gen': 1, 'ttl': 0}),
              (('a', 'b', 1), {'a': 1, 'c': None}, {'gen': 1, 'ttl': 0}))],
            result)

    def test_query_equals(self):
        query = self.asm.query('a', 'b')
        query.select('a', 'c')
        query.where(AerospikePredicatesMock().equals("a", 1))
        self.assertEqual(
            [(('a', 'b', 1), {'a': 1, 'c': None}, {'gen': 1, 'ttl': 0})],
            query.results())

    def test_query_between(self):
        query = self.asm.query('a', 'b')
        query.select('a', 'c')
        query.where(AerospikePredicatesMock().between("a", 1, 4))
        self.assertEqual(
            [(('a', 'b', 3), {'a': 3, 'c': None}, {'gen': 1, 'ttl': 0}),
             (('a', 'b', 2), {'a': 2, 'c': None}, {'gen': 1, 'ttl': 0})],
            query.results())

    def test_query_contains(self):
        self.asm.put(("a", "l", 1), {"list": [1, 2, 3, 4, 5]})
        self.asm.put(("a", "l", 2), {"list": [1, 3, 4, 5]})
        query = self.asm.query('a', 'l')
        query.select('list')
        query.where(AerospikePredicatesMock().contains("list", list, 2))
        self.assertEqual(
            [(('a', 'l', 1), {'list': [1, 2, 3, 4, 5]}, {'gen': 1, 'ttl': 0})],
            query.results())

    def test_query_range(self):
        self.asm.put(("a", "l", 1), {"list": [1, 2, 3, 4, 5]})
        self.asm.put(("a", "l", 2), {"list": [4, 5]})
        query = self.asm.query('a', 'l')
        query.select('list')
        query.where(AerospikePredicatesMock().range("list", list, 1, 3))
        self.assertEqual(
            [(('a', 'l', 1), {'list': [1, 2, 3, 4, 5]}, {'gen': 1, 'ttl': 0})],
            query.results())


if __name__ == '__main__':
    unittest.main()