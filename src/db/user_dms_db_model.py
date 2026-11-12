from typing import Optional
import uuid

from werkzeug.datastructures import FileStorage

from server_core.db.db_connection import DbConnection
from src.db.base.base_user_db_model import BaseUserDbModel


class UserDmsDbModel(BaseUserDbModel):

    def __init__(self, conn: DbConnection, user_id: int):
        super(UserDmsDbModel, self).__init__(conn=conn, user_id=user_id)
        
    def save_file_to_dms(self, mime: Optional[str], file_type: str, is_binary: bool, file: FileStorage):
        guid = str(uuid.uuid4())
        file_name = file.filename

        dms_id = self.conn.insert("""
            INSERT INTO gb_app.dms(guid, file_name, file_type, file_desc, mime, is_binary, user_id) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, [guid, file_name, file_type, "", mime, is_binary, self.user_id])

        c = file.read()
        self.conn.query("""
        INSERT INTO gb_dms.dms (id, dms_data) VALUES (%s, %s)
        """, [dms_id, c])

        return self.conn.select_one("SELECT * FROM gb_app.dms WHERE id = %s", [dms_id])
