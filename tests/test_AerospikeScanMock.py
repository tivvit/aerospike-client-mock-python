import unittest
from AerospikeClientMock import AerospikeClientMock


class TestAerospikeScanMock(unittest.TestCase):
    def setUp(self):
        self.asm = AerospikeClientMock()
        self.asm.put(("a", "b", 1), {"a": 1, "b": 1})
        self.asm.put(("a", "b", 2), {"a": 2, "b": 2})
        self.asm.put(("a", "b", 3), {"a": 3, "b": 3})
        self.asm.put(("a", "c", 4), {"a": 4, "b": 4})

    def test_scan(self):
        scan = self.asm.scan('a', 'b')
        scan.select('a', 'c')
        self.assertEqual(
            [
                (('a', 'b', 1), {'a': 1, 'c': None}, {'gen': 1, 'ttl': 0}),
                (('a', 'b', 2), {'a': 2, 'c': None}, {'gen': 1, 'ttl': 0}),
                (('a', 'b', 3), {'a': 3, 'c': None}, {'gen': 1, 'ttl': 0})
            ], scan.results())

    def test_scan_namespace(self):
        scan = self.asm.scan('a')
        scan.select('a', 'b')
        self.assertEqual(
            [
                (('a', 'b', 1), {'a': 1, 'b': 1}, {'gen': 1, 'ttl': 0}),
                (('a', 'b', 2), {'a': 2, 'b': 2}, {'gen': 1, 'ttl': 0}),
                (('a', 'b', 3), {'a': 3, 'b': 3}, {'gen': 1, 'ttl': 0}),
                (('a', 'c', 4), {'a': 4, 'b': 4}, {'gen': 1, 'ttl': 0}),
            ], scan.results())

    def test_scan_callback(self):
        result = []

        def callback(data):
            (key, meta, bins) = data
            result.append((key, meta, bins))

        scan = self.asm.scan('a', 'b')
        scan.select('a', 'c')
        scan.foreach(callback)
        self.assertEqual(
            [
                ((('a', 'b', 1), {'a': 1, 'c': None}, {'gen': 1, 'ttl': 0}),
                 (('a', 'b', 2), {'a': 2, 'c': None}, {'gen': 1, 'ttl': 0}),
                 (('a', 'b', 3), {'a': 3, 'c': None}, {'gen': 1, 'ttl': 0}))
            ],
            result)


if __name__ == '__main__':
    unittest.main()