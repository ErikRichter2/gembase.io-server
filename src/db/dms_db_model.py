import io
import json
from typing import Optional, TypeVar
import uuid

import flask
import pandas as pd
from werkzeug.datastructures import FileStorage

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel

DATA_TYPE = TypeVar('DATA_TYPE')

class DmsDbModel(BaseDbModel):

    MIME_APPLICATION_JSON = 'application/json'
    MIME_SUB_DATA_FRAME = "pd.DataFrame"

    def __init__(self, conn: DbConnection):
        super(DmsDbModel, self).__init__(conn=conn)

    def save_df_to_dms(self, df: pd.DataFrame, guid, file_type='pd.DataFrame'):
        buf = io.BytesIO()
        df.to_feather(buf)
        buf.seek(0)
        c = buf.read()
        self.insert_or_update_dms(c, guid, file_type, 'application/octet-stream', 1, mime_subtype=DmsDbModel.MIME_SUB_DATA_FRAME)

    def save_json_to_dms(self, data: dict, guid: str | None = None, file_type: str = 'JSON') -> int:
        return self.insert_or_update_dms(json.dumps(data), guid, file_type, DmsDbModel.MIME_APPLICATION_JSON, 0)

    def save_file_to_dms(self, user_id: int, mime: str, file_type: str, is_binary: bool, file: FileStorage):
        guid = str(uuid.uuid4())
        file_name = file.filename

        dms_id = self.conn.insert("""
            INSERT INTO gb_app.dms(guid, file_name, file_type, file_desc, mime, is_binary, user_id) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, [guid, file_name, file_type, "", mime, is_binary, user_id])

        c = file.read()
        self.conn.query("""
        INSERT INTO gb_dms.dms (id, dms_data) VALUES (%s, %s)
        """, [dms_id, c])

        return self.conn.select_one("SELECT * FROM gb_app.dms WHERE id = %s", [dms_id])

    def update_dms_metadata(self, dms_id: int, data: dict):

        row = self.conn.select_one("SELECT * FROM gb_app.dms WHERE id = %s", [dms_id])

        for it in data:
            if it in row:
                row[it] = data[it]

        self.conn.query("""
        UPDATE gb_app.dms SET file_name = %s, file_desc = %s, file_type = %s WHERE id = %s
        """, [row['file_name'], row['desc'], row['file_type'], dms_id])

        return row

    def upload_new_version_of_file_to_dms(self, file: FileStorage, dms_id: int):

        self.conn.query("""
        UPDATE gb_app.dms SET version = version + 1 WHERE id = %s
        """, [dms_id])

        c = file.read()
        self.conn.query("""
        UPDATE gb_dms.dms SET dms_data = %s WHERE id = %s
        """, [c, dms_id])

    def upload_new_version_of_data_to_dms(self, dms_id: int, data: object):

        self.conn.query("""
        UPDATE gb_app.dms SET version = version + 1 WHERE id = %s
        """, [dms_id])

        self.conn.query("""
        UPDATE gb_dms.dms SET dms_data = %s WHERE id = %s
        """, [data, dms_id])

    def insert_or_update_dms(self, dms_data, guid, file_type, mime, is_binary, mime_subtype=None) -> int:
        if guid is not None:
            row = self.conn.select_one_or_none("SELECT id FROM gb_app.dms WHERE guid = %s", [guid])
        else:
            row = None
            guid = str(uuid.uuid4())

        if row is not None:
            dms_id = row['id']
            self.conn.query("UPDATE gb_dms.dms SET dms_data = %s WHERE id = %s", [dms_data, dms_id])
            self.conn.query("UPDATE gb_app.dms SET version = version + 1, mime_subtype = %s WHERE id = %s", [mime_subtype, dms_id])
        else:
            dms_id = self.conn.insert("""
            INSERT INTO gb_app.dms (file_type, mime, guid, is_binary, mime_subtype) VALUES (%s, %s, %s, %s, %s)
            """, [file_type, mime, guid, is_binary, mime_subtype])
            self.conn.query("INSERT INTO gb_dms.dms (id, dms_data) VALUES (%s, %s)", [dms_id, dms_data])

        return dms_id

    def get_dms_data(self, type: type[DATA_TYPE], guid: str | None = None, dms_id: int | None = None) -> Optional[DATA_TYPE]:
        if dms_id is not None:
            row = self.conn.select_one_or_none("""
            SELECT d.dms_data 
              FROM gb_dms.dms d
             WHERE d.id = %s
            """, [dms_id])
        else:
            row = self.conn.select_one_or_none("""
            SELECT d.dms_data 
              FROM gb_app.dms a,
                   gb_dms.dms d
             WHERE a.id = d.id
               AND a.guid = %s
            """, [guid])

        if row is None:
            return None

        return row['dms_data']

    def get_dms_data_to_json(self, guid: str | None = None, dms_id: int | None = None):
        dms_data = self.get_dms_data(str, guid, dms_id)

        if dms_data is None:
            return None

        return json.loads(dms_data)

    def get_dms_data_to_df(self, guid: str | None = None, dms_id: int | None = None):
        dms_data = self.get_dms_data(dict, guid, dms_id)

        if dms_data is None:
            return None

        return Utils.db_data_to_df(dms_data)

    def delete(self, dms_id: int):
        self.conn.query("DELETE FROM gb_app.dms WHERE id = %s", [dms_id])
        self.conn.query("DELETE FROM gb_dms.dms WHERE id = %s", [dms_id])

    def select_all(self):
        return self.conn.select_all("SELECT * FROM gb_app.dms")

    def read_df_from_external_data(self, guid: str):
        row = self.conn.select_one("""
        SELECT d.data
          FROM external_data.dms d
         WHERE d.guid = %s
        """, [guid])
        df = Utils.db_data_to_df(row["data"])
        return df

    def write_df_to_external_data(self, guid: str, df: pd.DataFrame):
        buf = io.BytesIO()
        df.to_feather(buf)
        buf.seek(0)
        c = buf.read()
        self.conn.query("DELETE FROM external_data.dms WHERE guid = %s", [guid])
        self.conn.query("INSERT INTO external_data.dms (guid, data) VALUES (%s, %s)", [guid, c])


class DmsCache:

    __cache: dict = {}
    __dms_guid_to_id: dict = {}

    @staticmethod
    def get_from_cache(conn: DbConnection, guid: str | None = None, dms_id: int | None = None) -> dict:

        if guid is None and dms_id is None:
            raise Exception(f"guid and dms_id are none")
        if guid is not None and dms_id is not None:
            raise Exception(f"guid and dms_id: one must be none. guid: {guid}, dms_id: {dms_id}")

        if dms_id is None:
            if guid in DmsCache.__dms_guid_to_id:
                dms_id = DmsCache.__dms_guid_to_id[guid]
            else:
                return DmsCache.__refresh_cache(conn, guid, dms_id)

        if dms_id not in DmsCache.__cache:
            return DmsCache.__refresh_cache(conn, guid, dms_id)

        cached = DmsCache.__cache[dms_id]

        row = conn.select_one_or_none("""
        SELECT version FROM gb_app.dms WHERE id = %s
        """, [dms_id])

        if row is None:
            raise Exception(f"DMS not found {dms_id}")
        if row['version'] != cached['v']:
            return DmsCache.__refresh_cache(conn, guid, dms_id)

        return {
            "data": cached['d'],
            "version": cached['v'],
            "is_new_version": False
        }        
        
    @staticmethod
    def __refresh_cache(conn: DbConnection, guid: str | None = None, dms_id: int | None = None) -> dict:

        if dms_id is not None:
            row = conn.select_one_or_none("""
            SELECT a.guid, a.id, a.version, a.mime, d.dms_data, a.mime_subtype 
              FROM gb_app.dms a,
                   gb_dms.dms d
             WHERE a.id = d.id
               AND a.id = %s
            """, [dms_id])
        else:
            row = conn.select_one_or_none("""
            SELECT a.guid, a.id, a.version, a.mime, d.dms_data , a.mime_subtype
              FROM gb_app.dms a,
                   gb_dms.dms d
             WHERE a.id = d.id
               AND a.guid = %s
            """, [guid])

        if row is None:
            return {
                "data": None,
                "version": -1,
                "is_new_version": True
            }

        dms_id = row['id']
        guid = row['guid']
        mime = row['mime']
        mime_subtype = row["mime_subtype"]
        version = row['version']
        dms_data = row['dms_data']

        if mime == DmsDbModel.MIME_APPLICATION_JSON:
            dms_data = json.loads(dms_data)
        elif mime_subtype == DmsDbModel.MIME_SUB_DATA_FRAME:
            dms_data = Utils.db_data_to_df(dms_data)

        DmsCache.__cache[dms_id] = {
            'v': version,
            'd': dms_data,
            'guid': guid
        }

        DmsCache.__dms_guid_to_id[guid] = dms_id

        return {
            "data": dms_data,
            "version": version,
            "is_new_version": True
        }        

    @staticmethod
    def get_json(conn: DbConnection, guid: str, create_copy=False) -> dict:
        res = DmsCache.get_from_cache(conn, guid=guid)
        data = res["data"]
        if create_copy:
            data = json.loads(json.dumps(data))
        return data

    @staticmethod
    def clear():
        __cache: dict = {}
        __dms_guid_to_id: dict = {}

    @staticmethod
    def get_df(conn: DbConnection, guid: str) -> pd.DataFrame:
        res = DmsCache.get_from_cache(conn, guid=guid)
        return res["data"]

    @staticmethod
    def get_json_from_session_cache(conn: DbConnection, dms_guid: str):
        data = getattr(flask.g, dms_guid, None)
        if data is None:
            data = DmsCache.get_json(conn, dms_guid)
            setattr(flask.g, dms_guid, data)
        return data
