from esdb import ESDB

es = ESDB(port='9200')

print 'Method 1:'

es.clear()

foo = es['foo']
bar = foo['bar']
bar['baz'] = {
    'fun': 'times'
}

print es
print foo
print bar

print 'Method 2:'

es.clear()

es['foo']['bar']['baz'] = {
    'fun': 'times'
}

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

print es
