import MySQLdb
from itertools import izip

class SqlReader(object):

    def __init__(self, host, user, password, db, tablename, test_host=None, test_db=None, is_testing=False, **kwargs):
        self.host = host if not is_testing else test_host
        self.user = user
        self.password = password
        self.db = db if not is_testing else test_db

        self.query = "SELECT * FROM %s "
        self.tablename = tablename
        self.count = None
        self.conn = None
        self.eof = False
        
    def open(self):
        self.conn = MySQLdb.connect(self.host, self.user, self.password, self.db)
        self.cursor = self.conn.cursor()

        return self._run_query()

    def _run_query(self):
        self.count = self.cursor.execute(self.query % self.tablename)

        if not self.count:
            return None

        self.columns = [d[0].lower() for d in self.cursor.description]
        return self.conn

    def close(self):
        self.conn.close()

    def read_one(self):
        placeholders = self.cursor.fetchone()
        if not placeholders:
            self.eof = True
            return None
        else:
            return dict(izip(self.columns, placeholders))