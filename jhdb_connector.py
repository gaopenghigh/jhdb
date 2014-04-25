''' a simple MySQL class'''
# -*- coding: utf-8 -*-

import mysql.connector
from mysql.connector import errorcode


class JhdbError(Exception):
    ''' General Exception '''
    def __init__(self, value):
        self.value = value
        Exception.__init__(self)

    def __str__(self):
        return repr(self.value)


class JhdbExcutor(object):
    ''' a helper class for JhdbPool and Jhdb, do the real MySQL work '''
    def __init__(self, cursor):
        self.cursor = cursor

    def _execute(self, sql, values=None):
        ''' do SQL query '''
        if not values:
            values = []
        try:
            self.cursor.execute(sql, values)
        except Exception, exc:
            raise JhdbError("execute sql error:%s, sql = %s" % (str(exc), sql))

    def select(self, sql, values=None):
        ''' return a List of Dict : [{},{},...{}] '''
        if not values:
            values = []
        ret = []
        self._execute(sql, values)
        cols = self.cursor.column_names
        for record in self.cursor.fetchall():
            ret.append(dict(zip(cols, record)))
        return ret

    def run(self, sql, values=None):
        ''' return True or False '''
        if not values:
            values = []
        try:
            self._execute(sql, values)
        except JhdbError, exc:
            print str(exc)
            return False
        return True

    def update(self, table, conditions, data):
        ''' update record according to some conditions '''
        sql_start = "UPDATE `%s` SET " % table
        values, sql_update, sql_where = [], [], []
        for col, val in data.items():
            sql_update.append("`{0}` = %s".format(col))
            values.append(val)
        sql_update = ', '.join(sql_update)
        for col, val in conditions.items():
            sql_where.append("`{0}` = %s".format(col))
            values.append(val)
        sql_where = ' WHERE ' + ' AND '.join(sql_where)
        sql = sql_start + sql_update + sql_where
        return self.run(sql, values)

    def insert(self, table, data):
        ''' insert data '''
        sql = "INSERT INTO `%s` (%s) VALUES (%s)"
        values, sql_cols, sql_vals = [], [], []
        for col, val in data.items():
            sql_cols.append('`%s`' % col)
            sql_vals.append('%s')
            values.append(val)
        sql_cols = ', '.join(sql_cols)
        sql_vals = ', '.join(sql_vals)
        sql = sql % (table, sql_cols, sql_vals)
        return self.run(sql, values)

    def delete(self, table, conditions):
        ''' conditons cannot be {} to prevent mistaken deleting '''
        values, sql_where = [], []
        sql_start = "DELETE FROM `%s` WHERE " % table
        for col, val in conditions.items():
            sql_where.append("`{0}` = %s".format(col))
            values.append(val)
        sql_where = ' AND '.join(sql_where)
        sql = sql_start + sql_where
        return self.run(sql, values)

    def get(self, table, conditions=None, orderby=None, limit=None):
        ''' conditions is a dict of {condition_filed:condition_value} '''
        if not conditions:
            conditions = {}
        values = []
        if not conditions:
            sql = "SELECT * FROM %s" % table
        else:
            parsed_conditions = []
            for col, val in conditions.items():
                parsed_conditions.append('`{0}` = %s'.format(col))
                values.append(val)
            parsed_conditions = ' AND '.join(parsed_conditions)
            sql = "SELECT * FROM `%s` WHERE %s" % (table, parsed_conditions)
        if orderby is not None:
            sql = "%s ORDER BY `%s`" % (sql, orderby)
        if limit is not None:
            sql = "%s LIMIT %s" % (sql, limit)
        return self.select(sql, values)


class Jhdb(JhdbExcutor):
    ''' MySQL class '''
    def __init__(self, configs):
        try:
            self.cnx = mysql.connector.connect(
                    host=configs['host'],
                    port=configs['port'],
                    user=configs['user'],
                    password=configs['password'],
                    database=configs['database'],
                    autocommit=True,
                    charset='utf8')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                raise JhdbError("Access denied")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                raise JhdbError("Database %s not exist" % configs['database'])
            else:
                raise
        self.cursor = self.cnx.cursor()
        JhdbExcutor.__init__(self, self.cursor)

    def close(self):
        ''' close connections '''
        self.cursor.close()
        self.cnx.close()

