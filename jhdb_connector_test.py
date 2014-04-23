#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: gaopenghigh<gaopenghigh@gmail.com>
import time
import threading
import jhdb_connector as jhdb

def test_Jhdb():
    testdb = {
       'host': '127.0.0.1',
       'port': 3306,
       'user': 'root',
       'password': '123156',
       'database': 'jop',
       }
    db = jhdb.Jhdb(**testdb)
    rets = db.select('select * from clusters')
    print(rets)
    for r in rets:
        print r['varz']
    print(db.get('clusters', {'varz':'test:test'}))
    print(db.get('clusters', {'id':3}))
    print(db.update('clusters', {'id':3}, {'varz':'abcdefg'}))
    print(db.get('clusters', {'id':3}))
    print(db.insert('clusters', {'name':'testname', 'varz':'haha'}))
    print(db.get('clusters'))
    print(db.delete('clusters', {'name':'testname', 'varz':'haha'}))
    print(db.get('clusters'))


def query_func(dbpool, name):
    ret = dbpool.get('clusters', {'varz':'test:test'})
    print('%s, resul = %s, sleep 5s' % (name, ret))
    time.sleep(5)


def test_JhdbPool():
    testdb = {
       'host': '127.0.0.1',
       'port': 3306,
       'user': 'root',
       'password': '123156',
       'database': 'jop',
       'pool_name': 'mypool',
       'pool_size': 1,
       }
    dbpool = jhdb.JhdbPool(**testdb)
    for i in range(5):
        name = 'thread-' + str(i)
        threading.Thread(target=query_func, args=(dbpool,name),
                name=name).start()

if __name__ == '__main__':
    test_Jhdb()
#    test_JhdbPool()



