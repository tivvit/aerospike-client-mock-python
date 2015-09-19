class AerospikeQueryMock(object):
    def __init__(self, client, namespace, as_set=None):
        self.client = client
        self.namespace = namespace
        self.set = as_set
        self.results_list = []

    def select(self, *args):
        for key, entry in self.client.storage.items():
            if key[0] == self.namespace and \
                (self.set is None or key[1] == self.set):
                self.results_list.append(self.client.select(key, list(args)))

        # dict order is not persistent across python versions - sort by keys
        self.results_list = sorted(self.results_list, key=lambda x: x[0])

    def where(self, predicate):
        self.results_list = predicate(self.results_list)

    def results(self, policy=None):
        return self.results_list

    def foreach(self, callback, policy=None, options=None):
        callback(self.results_list)

    def apply(self, module, fucntion, arguments=None):
        raise NotImplementedError