from esdb import ESDB


es = ESDB()
print es

es['test'] = {}
print es

test = es['test']
print test

test['foo'] = {}
print test

del es['test']
print es
