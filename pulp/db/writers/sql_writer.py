import os
import MySQLdb

class SqlWriter(object):

    def __init__(self, host, user, password, db, test_host=None, test_db=None, tablename=None,
                    tablelike=None, batchsize=1, auto_commit=False, is_testing=False):
        self.host = host if not is_testing else test_host
        self.user = user
        self.password = password
        self.db = db if not is_testing else test_db
        self.auto_commit = auto_commit
        self.tablename = tablename
        self.tablelike = tablelike
        self.batchsize = batchsize

        self.tb_exists_list = []

    def open(self):
        self.conn = MySQLdb.connect(self.host, self.user, self.password, self.db)
        self.cursor = self.conn.cursor()

        if self.auto_commit:
            self.conn.autocommit(self.auto_commit)
        return self.conn

    def _create_table(self, tbname):
        if not self.tablelike:
            raise Exception("Cannot Create Table: 'tablelike' not given!")

        q = "CREATE TABLE %s LIKE %s" % (tbname, self.tablelike)
        self.cursor.execute(q)

        self.tb_exists_list.append(tbname)

    def _table_exists(self, tbname):
        q_check = "SHOW TABLES LIKE '%s'" % (tbname, )

        if tbname in self.tb_exists_list:
            return True

        if self.cursor.execute(q_check):
            self.tb_exists_list.append(tbname)
            return True
        return False

    def _write_one(self, obj):
        if not self._table_exists(self.tablename):
            self._create_table(self.tablename)
        
        placeholders = ', '.join(['%s'] * len(obj))
        columns = ', '.join(obj.keys())

        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (self.tablename, columns, placeholders)
        
        self.cursor.execute(sql, obj.values())

    def write(self, obj_array):
        error_log = []

        for obj in obj_array:
            try:
                data = obj.get('data')
                
                target_attrs = obj.get('target_attrs', {})
                if 'tablename' in target_attrs:
                    self.tablename = target_attrs['tablename']
                
                self._write_one(data)
            except Exception as ex:
                error_log.append(ex)
        self.conn.commit()

        return error_log

    def close(self):
        self.conn.close()