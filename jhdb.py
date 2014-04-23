#!/usr/bin/python
# -*- coding: utf-8 -*-
# a simple MySQL class

import MySQLdb
import traceback

class Jhdb:
    def __init__(self, host, port, user, passwd, db):
        try:
            self.conn = MySQLdb.connect(host=host, port=int(port), user=user, \
                                      passwd=passwd, db=db)
            self.conn.autocommit(True)
            self.conn.set_character_set('utf8')
            self.cursor = self.conn.cursor()
        except Exception, e:
            print("Jhdb connectiong error, %s" % str(e))
            traceback.print_exc()


    def close(self):
        self.cursor.close()
        self.conn.close()

    def select(self, sql):
        '''
        return a List of Dict : [{},{},...{}]
        '''
        ret = []
        try:
            self.cursor.execute(sql)
        except Exception, e:
            print("Jhdb error %s, sql = %s" % (str(e), sql))
            return ret
        cols = [desc[0] for desc in self.cursor.description]
        for d in self.cursor.fetchall():
            dic = {}.fromkeys(cols)
            for (name, value) in zip(cols, d):
                dic[name] = value
            ret.append(dic)
        return ret

    def update(self, sql):
        ''' return True or False '''
        try:
            self.cursor.execute(sql)
            return True
        except Exception, e:
            print("Jhdb error %s, sql = %s" % (str(e), sql))
            return False

    def insert(self, sql):
        return self.update(sql)

    def delete(self, sql):
        return self.update(sql)

    def _parse_conditions(self, conditions):
        ''' conditions is a dict of {condition_filed:condition_value} '''
        return ' AND '.join(
                ["`%s` = '%s'" % (k,v) for k,v in conditions.items()])

    def delete_by_field(self, table, field, v):
        sql = "DELETE FROM %s WHERE `%s` = '%s'" % (table, field, v)
        return self.delete(sql)

    def update_field(self, table, condition_k, condition_v, field, v):
        sql = "UPDATE %s SET `%s` = '%s' WHERE `%s` = '%s'"
        sql = sql % (table, field, v, condition_k, condition_v)
        return self.update(sql)

    def get_by_conditions(self, table, conditions, orderby=None, limit=None):
        sql = "SELECT * FROM %s WHERE %s"
        sql = sql % (table, self._parse_conditions(conditions))
        if orderby is not None:
            sql = "%s ORDER BY `%s`" % (sql, orderby)
        if limit is not None:
            sql = "%s LIMIT %s" % (sql, limit)
        print(sql)
        return self.select(sql)


def test_parse_conditions(conditions):
    ''' conditions is a dict of {condition_filed:condition_value} '''
    return ' AND '.join(
            ["`%s` = '%s'" % (k,v) for k,v in conditions.items()])


def test():
    conditions = {'id':123, 'f1':'asdfasef', 'f4':'asfies'}
    print(test_parse_conditions(conditions))


if __name__ == "__main__":
    test()

