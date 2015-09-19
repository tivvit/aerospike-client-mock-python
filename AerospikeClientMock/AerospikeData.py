import time


class AerospikeData(object):
    def __init__(self, bins, ttl=0):
        self.generation = 1
        self.ttl = 0
        self.set_ttl(ttl)
        self._data = bins

    @property
    def meta(self):
        return {"gen": self.generation, "ttl": self.ttl}

    @property
    def valid(self):
        return not (self.ttl and self.ttl < time.time())

    def touch(self, ttl=0):
        self.generation += 1
        self.set_ttl(ttl)

    def set_ttl(self, ttl):
        self.ttl = int(time.time()) + ttl if ttl else ttl

    def __setitem__(self, key, value):
        self._data[key] = value
        self.generation += 1

    def update(self, key, value, ttl=0):
        self[key] = value
        self.set_ttl(ttl)

    def __getitem__(self, item):
        return self._data[item]

    def __contains__(self, item):
        return item in self._data

    def __delitem__(self, key):
        del self._data[key]

    def __repr__(self):
        return str(self.dump())

    def __eq__(self, other):
        if isinstance(other, dict):
            return self.dump() == other

        raise NotImplementedError("Cannot compare to %s" % type(other))

    def dump(self):
        if self.valid:
            return self._data
        else:
            return None

    def iteritems(self):
        for k, v in self._data.items():
            yield k, v
