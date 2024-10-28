import io
import json
import uuid

import flask
import pandas as pd
from werkzeug.datastructures import FileStorage

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.exception.base_app_exception import BaseAppException
from src.utils.gembase_utils import GembaseUtils

DMS999 = "DMS999"

ERROR_MESSAGES = {
    DMS999: "Unexpected error ! Please contact gembase.io support.",
}


class DmsModel:
    MIME_APPLICATION_JSON = 'application/json'

    MIME_SUB_DATA_FRAME = "pd.DataFrame"

    @staticmethod
    def save_df_to_dms(conn: DbConnection, df: pd.DataFrame, guid, file_type='pd.DataFrame'):
        buf = io.BytesIO()
        df.to_feather(buf)
        buf.seek(0)
        c = buf.read()
        DmsModel.insert_or_update_dms(conn, c, guid, file_type, 'application/octet-stream', 1, mime_subtype=DmsModel.MIME_SUB_DATA_FRAME)

    @staticmethod
    def save_json_to_dms(conn: DbConnection, data: {}, guid: str | None = None, file_type: str = 'JSON') -> int:
        return DmsModel.insert_or_update_dms(conn, json.dumps(data), guid, file_type, DmsModel.MIME_APPLICATION_JSON, 0)

    @staticmethod
    def save_file_to_dms(conn: DbConnection, user_id: int, mime: str, file_type: str, is_binary: bool, file: FileStorage):
        guid = str(uuid.uuid4())
        file_name = file.filename

        dms_id = conn.insert("""
            INSERT INTO app.dms(guid, file_name, file_type, file_desc, mime, is_binary, user_id) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, [guid, file_name, file_type, "", mime, is_binary, user_id])

        c = file.read()
        conn.query("""
        INSERT INTO dms.dms (id, dms_data) VALUES (%s, %s)
        """, [dms_id, c])

        return conn.select_one("SELECT * FROM app.dms WHERE id = %s", [dms_id])

    @staticmethod
    def update_dms_metadata(conn: DbConnection, dms_id: int, data: {}):

        row = conn.select_one("SELECT * FROM app.dms WHERE id = %s", [dms_id])

        for it in data:
            if it in row:
                row[it] = data[it]

        conn.query("""
        UPDATE app.dms SET file_name = %s, file_desc = %s, file_type = %s WHERE id = %s
        """, [row['file_name'], row['desc'], row['file_type'], dms_id])

        return row

    @staticmethod
    def upload_new_version_of_file_to_dms(conn: DbConnection, file: FileStorage, dms_id: int):

        conn.query("""
        UPDATE app.dms SET version = version + 1 WHERE id = %s
        """, [dms_id])

        c = file.read()
        conn.query("""
        UPDATE dms.dms SET dms_data = %s WHERE id = %s
        """, [c, dms_id])

    @staticmethod
    def upload_new_version_of_data_to_dms(conn: DbConnection, dms_id: int, data: any):

        conn.query("""
        UPDATE app.dms SET version = version + 1 WHERE id = %s
        """, [dms_id])

        conn.query("""
        UPDATE dms.dms SET dms_data = %s WHERE id = %s
        """, [data, dms_id])

    @staticmethod
    def insert_or_update_dms(conn: DbConnection, dms_data, guid, file_type, mime, is_binary, mime_subtype=None) -> int:
        if guid is not None:
            row = conn.select_one_or_none("SELECT id FROM app.dms WHERE guid = %s", [guid])
        else:
            row = None
            guid = str(uuid.uuid4())

        if row is not None:
            dms_id = row['id']
            conn.query("UPDATE dms.dms SET dms_data = %s WHERE id = %s", [dms_data, dms_id])
            conn.query("UPDATE app.dms SET version = version + 1, mime_subtype = %s WHERE id = %s", [mime_subtype, dms_id])
        else:
            dms_id = conn.insert("""
            INSERT INTO app.dms (file_type, mime, guid, is_binary, mime_subtype) VALUES (%s, %s, %s, %s, %s)
            """, [file_type, mime, guid, is_binary, mime_subtype])
            conn.query("INSERT INTO dms.dms (id, dms_data) VALUES (%s, %s)", [dms_id, dms_data])

        return dms_id

    @staticmethod
    def get_dms_data(conn: DbConnection, guid: str | None = None, dms_id: int | None = None) -> any:
        if dms_id is not None:
            row = conn.select_one_or_none("""
            SELECT d.dms_data 
              FROM dms.dms d
             WHERE d.id = %s
            """, [dms_id])
        else:
            row = conn.select_one_or_none("""
            SELECT d.dms_data 
              FROM app.dms a,
                   dms.dms d
             WHERE a.id = d.id
               AND a.guid = %s
            """, [guid])

        if row is None:
            return None

        return row['dms_data']

    @staticmethod
    def get_dms_data_to_json(conn: DbConnection, guid: str | None = None, dms_id: int | None = None):
        dms_data = DmsModel.get_dms_data(conn, guid, dms_id)

        if dms_data is None:
            return None

        return json.loads(dms_data)

    @staticmethod
    def get_dms_data_to_df(conn: DbConnection, guid: str | None = None, dms_id: int | None = None):
        dms_data = DmsModel.get_dms_data(conn, guid, dms_id)

        if dms_data is None:
            return None

        return GembaseUtils.db_data_to_df(dms_data)

    @staticmethod
    def delete(conn: DbConnection, dms_id: int):
        conn.query("DELETE FROM app.dms WHERE id = %s", [dms_id])
        conn.query("DELETE FROM dms.dms WHERE id = %s", [dms_id])

    @staticmethod
    def select_all(conn: DbConnection):
        return conn.select_all("SELECT * FROM app.dms")

    @staticmethod
    def read_df_from_external_data(conn: DbConnection, guid: str):
        row = conn.select_one("""
        SELECT d.data
          FROM external_data.dms d
         WHERE d.guid = %s
        """, [guid])
        df = GembaseUtils.db_data_to_df(row["data"])
        return df

    @staticmethod
    def write_df_to_external_data(conn: DbConnection, guid: str, df: pd.DataFrame):
        buf = io.BytesIO()
        df.to_feather(buf)
        buf.seek(0)
        c = buf.read()
        conn.query("DELETE FROM external_data.dms WHERE guid = %s", [guid])
        conn.query("INSERT INTO external_data.dms (guid, data) VALUES (%s, %s)", [guid, c])


class DmsCache:

    __cache: {} = {}
    __dms_guid_to_id: {} = {}

    @staticmethod
    def __refresh_cache(conn: DbConnection, guid: str | None = None, dms_id: int | None = None) -> (any, int, bool):

        if dms_id is not None:
            row = conn.select_one_or_none("""
            SELECT a.guid, a.id, a.version, a.mime, d.dms_data, a.mime_subtype 
              FROM app.dms a,
                   dms.dms d
             WHERE a.id = d.id
               AND a.id = %s
            """, [dms_id])
        else:
            row = conn.select_one_or_none("""
            SELECT a.guid, a.id, a.version, a.mime, d.dms_data , a.mime_subtype
              FROM app.dms a,
                   dms.dms d
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

        if mime == DmsModel.MIME_APPLICATION_JSON:
            dms_data = json.loads(dms_data)
        elif mime_subtype == DmsModel.MIME_SUB_DATA_FRAME:
            dms_data = GembaseUtils.db_data_to_df(dms_data)

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
    def get_json(conn: DbConnection, guid: str, create_copy=False) -> any:
        res = DmsCache.get_from_cache(conn, guid=guid)
        data = res["data"]
        if create_copy:
            data = json.loads(json.dumps(data))
        return data

    @staticmethod
    def clear():
        __cache: {} = {}
        __dms_guid_to_id: {} = {}

    @staticmethod
    def get_df(conn: DbConnection, guid: str) -> pd.DataFrame:
        res = DmsCache.get_from_cache(conn, guid=guid)
        return res["data"]

    @staticmethod
    def get_from_cache(conn: DbConnection, guid: str | None = None, dms_id: int | None = None) -> (any, int, bool):

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
        SELECT version FROM app.dms WHERE id = %s
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
    def get_json_from_session_cache(conn: DbConnection, dms_guid: str):
        data = getattr(flask.g, dms_guid, None)
        if data is None:
            data = DmsCache.get_json(conn, dms_guid)
            setattr(flask.g, dms_guid, data)
        return data


class DmsException(BaseAppException):
    def __init__(self, error_id, **kwargs):
        super(DmsException, self).__init__("dms", error_id, ERROR_MESSAGES, **kwargs)
