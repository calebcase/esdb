from collections import Mapping, MutableMapping
from rawes import Elastic
from rawes.elastic_exception import ElasticException
from time import sleep


def refresh_interval(es, index):
    '''
    Ensure that the refresh interval has been specified. If not, it
    is set to the default of 1 second. This is necessary for the
    _refresh API calls to function properly. If it is not set, then
    _refresh API calls have no affect.
    '''

    es_i = es[index]
    if 'index.refresh_interval' not in es_i.get('_settings')[index]['settings']:
        es_i.put('_settings', data={
            'index': {
                'refresh_interval': '1s'
            }
        })

class ESDB(MutableMapping):
    '''Top level dictionary into ES providing access to indices.'''

    def __init__(self, host='localhost', port='9200'):
        self.es = Elastic("{}:{}".format(host, port), except_on_error=True)

    def __len__(self):
        try:
            return self.es.get('_count')['count']
        except ElasticException:
            return 0

    def __iter__(self):
        return self.es.get('_status')['indices'].iterkeys()

    def __getitem__(self, item):
        try:
            return Index(item, self.es) 
        except ElasticException, e:
            raise KeyError(item)

    def __contains__(self, item):
        try:
            return self.es.head(item)
        except ElasticException:
            return False

    def __setitem__(self, index_name, types):
        index = self[index_name]
        for type_name, documents in types.iteritems():
            t = index[type_name]
            for key, doc in documents.iteritems():
                t[key] = doc

    def __delitem__(self, item):
        try:
            self.es.delete(item)
        except ElasticException:
            raise KeyError(item)

    def __repr__(self):
        return repr({key: value for (key, value) in self.iteritems()})

    def clear(self):
        self.es.delete('_all')

    def refresh(self):
        for index in self.keys():
            refresh_interval(self.es, index)
        self.es.post('_refresh')


class Index(MutableMapping):
    '''Index dictionary providing access to types.'''

    def __init__(self, name, es):
        self.name = name
        self.es = es

    def __len__(self):
        try:
            return self.es[self.name].get('_count')['count']
        except ElasticException:
            return 0

    def __iter__(self):
        try:
            self.refresh()
            return self.es[self.name].get('_mapping')[self.name].iterkeys()
        except ElasticException:
            return {}.iterkeys()

    def __getitem__(self, item):
        try:
            return Type(item, self.name, self.es) 
        except ElasticException:
            raise KeyError(item)

    def __contains__(self, item):
        try:
            return self.es[self.name].head(item)
        except ElasticException:
            return False

    def __setitem__(self, type_name, documents):
        t = self[type_name]
        for key, doc in documents.iteritems():
            t[key] = doc

    def __delitem__(self, item):
        try:
            self.es[self.name].delete(item)
        except ElasticException:
            raise KeyError(item)

    def __repr__(self):
        return repr({key: value for (key, value) in self.iteritems()})

    def clear(self):
        self.es[self.name].delete('_all')

    def refresh(self):
        refresh_interval(self.es, self.name)
        self.es[self.name].post('_refresh')


class Type(MutableMapping):
    '''Type dictionary providing access to documents.'''

    def __init__(self, name, index, es):
        self.name = name
        self.index = index
        self.es = es

    def __len__(self):
        try:
            return self.es[self.index][self.name].get('_count')['count']
        except ElasticException:
            return 0

    def __iter__(self):
        def gen(self=self):
            try:
                self.refresh()
                scroll = self.es[self.index][self.name].get('_search?scroll=5m&search_type=scan', data={
                    'fields': ['_id'],
                    'query': {
                        'match_all': {}
                    }
                })

                if '_scroll_id' in scroll:
                    self.scroll_id = scroll['_scroll_id']

                while True:
                    found = self.es.get('_search/scroll?scroll=5m', data=self.scroll_id)

                    hits = found['hits']['hits']
                    if len(hits) == 0:
                        raise StopIteration()
                    for hit in hits:
                        yield hit['_id']
                    self.scroll_id = found['_scroll_id']
            except ElasticException:
                raise StopIteration()
        return gen()

    def __getitem__(self, item):
        try:
            return self.es[self.index][self.name].get(item)['_source']
        except ElasticException:
            raise KeyError(item)

    def __contains__(self, item):
        try:
            return self.es[self.index][self.name].head(item)
        except ElasticException:
            return False

    def __setitem__(self, item, value):
        self.es[self.index][self.name].put(item, data=value)

    def __delitem__(self, item):
        try:
            self.es[self.index][self.name].delete(item)
        except ElasticException:
            raise KeyError(item)

    def __repr__(self):
        return repr({key: value for (key, value) in self.iteritems()})

    def clear(self):
        self.es[self.index][self.name].delete('_all')

    def refresh(self):
        refresh_interval(self.es, self.index)
        self.es[self.index].post('_refresh')
