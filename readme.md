# Aerospike client mock for python

* This mock supports all standard Aerospike python client operations except operations listed in todo section
* Scan and query with where predicates also supported
* Support for dumping cluster state to string or to dict

### Example

```python
from AerospikeClientMock import AerospikeClientMock

asm = AerospikeClientMock()
key = ("a", "b", "c")
asm.put(key, {"a": 1})
print asm.get(key)
asm.increment(key, "a", 2)
print asm.get(key)

# use string conversion for testing cluster state
print str(asm)
# or use to dict dump
print asm.dump()
```
#### With TTL
TTL has to be provided via constructor, because TTL is set on server side in standard client.

```python
from AerospikeClientMock import AerospikeClientMock
import time

asm = AerospikeClientMock(default_ttl=2)
key = ("a", "b", "c")
asm.put(key, {"a": 1})
print asm.get(key)
time.sleep(4)
print asm.exists(key)
```

#### Query example
```python
from AerospikeClientMock import AerospikeClientMock
from AerospikePredicatesMock import AerospikePredicatesMock

asm = AerospikeClientMock()
asm.put(("a", "b", 1), {"a": 1, "b": 1})
asm.put(("a", "b", 2), {"a": 2, "b": 2})
asm.put(("a", "b", 3), {"a": 3, "b": 3})
asm.put(("a", "c", 4), {"a": 4, "b": 4})
query = asm.query('a', 'b')
query.select('a', 'c')
query.where(AerospikePredicatesMock().equals("a", 1))
print query.results()
```

### Todo
* support for UDF scripts
* llist

### Development

Feel free to contribute.

### Copyright and License

&copy; 2015 [Vít Listík](http://tivvit.cz)

Released under [MIT licence](https://github.com/tivvit/aerospike-client-mock-python/blob/master/LICENSE)