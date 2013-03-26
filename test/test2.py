import esdb

es = esdb.ESDB()
foo = es['foo']
bar = foo['bar']
print bar.keys()
print bar
print foo
print es
