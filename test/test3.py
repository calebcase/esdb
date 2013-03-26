import esdb
from time import sleep

es = esdb.ESDB()

print 'Method 1:'

es.clear()

foo = es['foo']
bar = foo['bar']
bar['baz'] = {
    'fun': 'times'
}

es.es.put('_settings', data={'index': {'refresh_interval': '1s'}})
foo.refresh()

print es
print foo
print bar

print 'Method 2:'

es.clear()

es['foo']['bar']['baz'] = {
    'fun': 'times'
}

es.refresh()

print es

print 'Method 3:'

es.clear()

es['foo'] = {
    'bar': {
        'baz': {
            'fun': 'times'
        }
    }
}

es.refresh()

print es
