from typing import Optional

import mysql.connector
from mysql.connector.conversion import MySQLConverter
from mysql.connector.cursor import MySQLCursor
from mysql.connector.types import ParamsSequenceOrDictType

from gembase_server_core.db.db_exception import DbException
from gembase_server_core.environment.runtime_constants import rr
from gembase_server_core.private_data.private_data_model import PrivateDataModel


class DbConnection:

    credentials = {}

    def __init__(self, env: str = None, is_remote: bool = False):

        if env is None:
            env = rr.ENV

        if env not in self.credentials:
            if rr.is_debug() or is_remote or rr.IS_REMOTE:
                self.credentials[env] = PrivateDataModel.get_private_data(env)['mysql']['credentials_remote']
            else:
                self.credentials[env] = PrivateDataModel.get_private_data(env)['mysql']['credentials']

        self.mydb = mysql.connector.connect(**self.credentials[env])
        self.query("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")

    def connection_id(self):
        return self.mydb.connection_id

    def values_arr_to_db_in(self, arr: [], int_values=False) -> str:
        db_arr = []
        for it in arr:
            val = MySQLConverter.escape(it)
            db_arr.append(f"{val}" if int_values else f"'{val}'")
        res = ",".join(db_arr)
        return res

    @staticmethod
    def s_query(query: str, params: Optional[ParamsSequenceOrDictType] = None):
        conn = DbConnection()
        conn.query(query, params)
        conn.commit()
        conn.close()

    @staticmethod
    def s_select_all(query: str, params: Optional[ParamsSequenceOrDictType] = None):
        conn = DbConnection()
        rows = conn.select_all(query, params)
        conn.close()
        return rows

    def set_read_uncommitted(self):
        self.query("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")

    def close(self):
        self.mydb.close()

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

    def query(self, query: str, params: Optional[ParamsSequenceOrDictType] = None):
        cur = self.mydb.cursor()
        cur.execute(query, params)
        cur.close()

    def bulk(self, query: str, data: []):
        cur = self.mydb.cursor()
        cur.executemany(query, data)
        cur.close()

    def insert(self, query: str, params: Optional[ParamsSequenceOrDictType] = None) -> int:
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

    def query_safe(self, query: str, params: Optional[ParamsSequenceOrDictType] = None):
        cur = self.mydb.cursor()
        try:
            cur.execute(query, params)
        except Exception:
            pass
        finally:
            cur.close()

    @staticmethod
    def private_select_to_dict(cur: MySQLCursor):
        columns = cur.description
        result = [{columns[index][0]: column for index, column in enumerate(value)} for value in cur.fetchall()]
        cur.close()
        return result

    def select_one(self, query: str, params: Optional[ParamsSequenceOrDictType] = None):
        cur = self.mydb.cursor()
        cur.execute(query, params)
        rows = DbConnection.private_select_to_dict(cur)

        if len(rows) == 0:
            raise DbException(DbException.DB001)
        if len(rows) > 1:
            raise DbException(DbException.DB002)

        return dict(rows[0])

    def select_one_or_none(self, query: str, params: [] = None):
        cur = self.mydb.cursor()
        cur.execute(query, params)
        rows = DbConnection.private_select_to_dict(cur)

        if len(rows) > 1:
            raise DbException(DbException.DB002)

        if len(rows) == 0:
            return None
        return dict(rows[0])

    def select_zero(self, query: str, params: [] = None):
        cur = self.mydb.cursor()
        cur.execute(query, params)
        rows = DbConnection.private_select_to_dict(cur)

        if len(rows) != 0:
            raise DbException(DbException.DB003)

    def is_zero(self, query: str, params: [] = None) -> bool:
        cur = self.mydb.cursor()
        cur.execute(query, params)
        rows = DbConnection.private_select_to_dict(cur)
        return len(rows) == 0

    def select_all(self, query: str, params: [] = None):
        cur = self.mydb.cursor()
        cur.execute(query, params)
        rows = DbConnection.private_select_to_dict(cur)
        return rows
