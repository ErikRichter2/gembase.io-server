import csv
from io import StringIO
from os import write
import pymysql

from pymysql import converters
from typing import Optional

from server_core.db.db_exception import DbException
from src.env_data import ENV_DATA


class DbConnection:

    credentials = {}

    def __init__(self, env: str = None, is_remote: bool = False):

        if env not in self.credentials:
            env_data = ENV_DATA
            if env is not None and env != env_data.app_env:
                env_data = env_data.override(env=env)
                
            credentials = {
                'user': env_data.db_user,
                'password': env_data.db_pass,
                'host': env_data.db_host,
                'port': env_data.db_port
            }
            self.credentials[env] = credentials

        self.mydb = pymysql.connect(**self.credentials[env])
        self.query("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")

    def __enter__(self):
        #ttysetattr etc goes here before opening and returning the file object
        return self
    
    def __exit__(self, exception_type, exception_value, exception_traceback):
        #Exception handling here
        self.close()
        
    def connection_id(self):
        return self.mydb.connection_id

    def values_arr_to_db_in(self, arr: list, int_values=False) -> str:
        db_arr = []
        for it in arr:
            val = converters.escape_string(str(it))
            db_arr.append(f"{val}" if int_values else f"'{val}'")
        res = ",".join(db_arr)
        return res

    @staticmethod
    def s_query(query: str, params: Optional[list] = None):
        conn = DbConnection()
        conn.query(query, params)
        conn.commit()
        conn.close()

    @staticmethod
    def s_select_all(query: str, params: Optional[list] = None):
        conn = DbConnection()
        rows = conn.select_all(query, params)
        conn.close()
        return rows

    def set_read_uncommitted(self):
        self.query("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")

    def close(self):
        try:
            self.mydb.close()
        except pymysql.err.Error:
            pass

    def commit(self, close: bool = False):
        self.mydb.commit()
        if close:
            self.close()

    def rollback(self, close: bool = False):
        self.mydb.rollback()
        if close:
            self.close()

    def unlock_tables(self, commit=True):
        if commit:
            self.commit()
        else:
            self.rollback()
        self.query("UNLOCK TABLES")

    def query(self, query: str, params: Optional[list] = None):
        cur = self.mydb.cursor()
        cur.execute(query, params)
        cur.close()

    def bulk(self, query: str, data):
        cur = self.mydb.cursor()
        cur.executemany(query, data)
        cur.close()
        
    def insert_many(self, query: str, data):
        if '##__##' not in query:
            raise Exception('Missing ##__##')
        
        symbol_len = len('##__##')
        
        idx_start = query.index('##__##')
        idx_end = query.index('##__##', idx_start + symbol_len)
        
        values_query = query[idx_start+symbol_len:idx_end]
        
        cur = self.mydb.cursor()
        
        values = []
        for it in data:
            escaped = cur.mogrify(values_query, it)
            values.append(escaped)
            
        final_query = query[:idx_start] + ' ' + ' ,'.join(values) + ' ' + query[idx_end+symbol_len:]
        
        cur.execute(final_query)
        row_id = cur.lastrowid
        cur.close()
        return row_id
    
    def insert(self, query: str, params: Optional[list] = None) -> int:
        cur = self.mydb.cursor()
        cur.execute(query, params)
        row_id = cur.lastrowid
        cur.close()
        return row_id

    def analyze(self, table: str):
        cur = self.mydb.cursor()
        cur.execute(f"ANALYZE TABLE {table}")
        cur.fetchall()
        cur.close()

    def query_safe(self, query: str, params: Optional[list] = None):
        cur = self.mydb.cursor()
        try:
            cur.execute(query, params)
        except Exception:
            pass
        finally:
            cur.close()

    @staticmethod
    def private_select_to_dict(cur):
        columns = cur.description
        result = [{columns[index][0]: column for index, column in enumerate(value)} for value in cur.fetchall()]
        cur.close()
        return result
    
    @staticmethod
    def private_select_to_csv(cur):
        column_names = [i[0] for i in cur.description]
        csv_output = StringIO()
        writer = csv.writer(csv_output, quoting=csv.QUOTE_NONNUMERIC, lineterminator="\n")
        writer.writerow(column_names)
        writer.writerows(cur.fetchall())
        cur.close()
        return csv_output.getvalue()

    def select_one(self, query: str, params: Optional[list] = None):
        cur = self.mydb.cursor()
        cur.execute(query, params)
        rows = DbConnection.private_select_to_dict(cur)

        if len(rows) == 0:
            raise DbException(DbException.SELECTED_0_EXPECTED_1)
        if len(rows) > 1:
            raise DbException(DbException.SELECTED_MORE_THAN_1_EXPECTED_1)

        return dict(rows[0])

    def select_one_or_none(self, query: str, params: Optional[list] = None):
        cur = self.mydb.cursor()
        cur.execute(query, params)
        rows = DbConnection.private_select_to_dict(cur)

        if len(rows) > 1:
            raise DbException(DbException.SELECTED_MORE_THAN_1_EXPECTED_1)

        if len(rows) == 0:
            return None
        return dict(rows[0])

    def select_all(self, query: str, params: Optional[list] = None):
        cur = self.mydb.cursor()
        cur.execute(query, params)
        return DbConnection.private_select_to_dict(cur)
        
    def select_all_csv(self, query: str, params: Optional[list] = None):
        cur = self.mydb.cursor()
        cur.execute(query, params)
        return DbConnection.private_select_to_csv(cur)        