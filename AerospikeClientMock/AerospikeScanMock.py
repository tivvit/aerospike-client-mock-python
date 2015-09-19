class AerospikeScanMock(object):
    def __init__(self, client, namespace, as_set=None):
        self.client = client
        self.namespace = namespace
        self.set = as_set
        self.results_list = []

    def select(self, *args):
        for key, entry in self.client.storage.items():
            if key[0] == self.namespace and \
                (self.set == None or key[1] == self.set):
                self.results_list.append(self.client.select(key, list(args)))

    def results(self, policy=None):
        return self.results_list

    def foreach(self, callback, policy=None, options=None):
        callback(self.results_list)