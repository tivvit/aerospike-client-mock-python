#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
#  aerospike mockserver


from .AerospikeData import AerospikeData
from .AerospikeQueryMock import AerospikeQueryMock
from .AerospikeScanMock import AerospikeScanMock


class AerospikeClientMock(object):
    def __init__(self, config=None, default_ttl=0):
        self.storage = {}
        self.default_ttl = default_ttl

    def connect(self, username=None, password=None):
        pass

    def is_connected(self):
        return True

    def close(self):
        pass

    def get(self, key, policy=None):
        exists = self.exists(key)[0]
        return key, self.storage[key].meta if exists else None, \
               self.storage[key] if exists else None

    def select(self, key, bins, policy=None):
        result = self.get(key)
        result_value = result[2]
        for bin in bins:
            if not bin in result_value:
                result_value[bin] = None
        result = (key,
                  result[1],
                  dict((bin, value) for bin, value in result_value.iteritems()
                       if bin in bins),
                  )
        return result

    def exists(self, key, policy=None):
        exists = key in self.storage
        valid = True
        if exists:
            valid = self.storage[key].valid
        if exists and not valid:
            self.remove(key)
            exists = False
        return exists, self.storage[key].meta if exists else None

    def put(self, key, bins, meta=None, policy=None, serializer=None):
        if key in self.storage:
            for bin, val in bins.items():
                self.storage[key].update(bin, val,
                        ttl=self.__get_ttl(meta))
        else:
            self.storage[key] = AerospikeData(bins, ttl=self.__get_ttl(meta))

    def touch(self, key, val=0, meta=None, policy=None):
        self.storage[key].touch(val if val else self.__get_ttl(meta))

    def remove(self, key, policy=None):
        del self.storage[key]

    def get_key_digest(self, ns, set, key):
        raise NotImplementedError

    def remove_bin(self, key, list, meta=None, policy=None):
        for k in list:
            del self.storage[key][k]

        self.storage[key].generation += 1
        self.storage[key].set_ttl(self.__get_ttl(meta))

    def append(self, key, bin, val, meta=None, policy=None):
        if not self.exists(key)[0]:
            self.put(key, {bin: val}, meta={"ttl": self.__get_ttl(meta)})
        else:
            if self.__get_ttl(meta):
                self.storage[key].update(bin, self.storage[key][bin] + val,
                                         ttl=self.__get_ttl(meta))
            else:
                self.storage[key][bin] += val

    def prepend(self, key, bin, val, meta=None, policy=None):
        if not self.exists(key)[0]:
            self.put(key, {bin: val}, meta={"ttl": self.__get_ttl(meta)})
        else:
            if self.__get_ttl(meta):
                self.storage[key].update(bin, val + self.storage[key][bin],
                                         ttl=self.__get_ttl(meta))
            else:
                self.storage[key][bin] = val + self.storage[key][bin]

    def increment(self, key, bin, offset, meta=None, policy=None):
        if not (self.exists(key)[0] and bin in self.storage[key]):
            self.put(key, {bin: offset}, meta={"ttl": self.__get_ttl(meta)})
        else:
            if self.__get_ttl(meta):
                self.storage[key].update(bin, self.storage[key][bin] + offset,
                                         ttl=self.__get_ttl(meta))
            else:
                self.storage[key][bin] += offset

    def operate(key, list, meta=None, policy=None):
        raise NotImplementedError

    # https://pythonhosted.org/aerospike/client.html#aerospike.Client.get_many
    # Warning The return type changed to list starting with version 1.0.50.
    def get_many(self, keys, policy=None):
        result = []
        for key in keys:
            result.append(self.get(key))
        return result

    def exists_many(self, keys, policy=None):
        result = []
        for key in keys:
            result.append((key, self.exists(key)[1]))
        return result

    def select_many(self, keys, bins, policy=None):
        result = []
        for id, key in enumerate(keys, 1):
            result.append(
                self.select(key, bins) if self.exists(key)[0] else None)
        return result

    def scan(self, namespace, set=None):
        return AerospikeScanMock(self, namespace, set)

    def query(self, namespace, set=None):
        return AerospikeQueryMock(self, namespace, set)

    def udf_put(self, filename, udf_type=1, policy=None):
        raise NotImplementedError

    def udf_remove(self, module, policy=None):
        raise NotImplementedError

    def udf_list(policy=None):
        raise NotImplementedError

    def udf_get(self, module, language=1, policy=None):
        raise NotImplementedError

    def apply(self, key, module, function, args, policy=None):
        raise NotImplementedError

    def scan_apply(ns, set, module, function, args=None, policy=None,
                   options=None):
        raise NotImplementedError

    def scan_info(self, scan_id):
        return {}

    def index_string_create(self, ns, set, bin, index_name, policy=None):
        pass

    def index_integer_create(self, ns, set, bin, index_name, policy=None):
        pass

    def index_list_create(self, ns, set, bin, index_datatype, index_name,
                          policy=None):
        pass

    def index_map_keys_create(self, ns, set, bin, index_datatype, index_name,
                              policy=None):
        pass

    def index_map_values_create(self, ns, set, bin, index_datatype, index_name,
                                policy=None):
        pass

    def index_remove(self, ns, index_name, policy=None):
        pass

    def get_nodes(self):
        return []

    def info(self, command, hosts=None, policy=None):
        return {}

    def info_node(self, command, host, policy=None):
        return ""

    def llist(self, key, bin, module=None):
        raise NotImplementedError

    # todo admin functions

    def __repr__(self):
        return str(self.dump())

    def dump(self):
        # refresh keys
        for k in self.storage:
            self.exists(k)
        return dict(
            [(key, value.dump()) for key, value in self.storage.items()])

    def __get_ttl(self, meta):
        if meta and "ttl" in meta:
            return meta["ttl"]
        return self.default_ttl
