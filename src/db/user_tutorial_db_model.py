import json

from server_core.db.db_connection import DbConnection
from src.db.base.base_user_db_model import BaseUserDbModel
from src.models.user_obfuscator import UserObfuscator


class UserTutorialDbModel(BaseUserDbModel):
    
    def __init__(self, conn: DbConnection, user_id: int):
        super(UserTutorialDbModel, self).__init__(conn=conn, user_id=user_id)
    
    def get_client_data(self):
        return {
            "tutorial": self.__get_tutorial_data(),
            "app_id_int": 0,
            "gaps": {
                UserObfuscator.TAG_IDS_INT: [
                    37,  # builder
                    31,  # combat
                    231  # adventure
                ]
            }
        }        

    def __get_tutorial_data(self):
        row = self.conn.select_one_or_none("""
        SELECT ut.tutorial
          FROM gb_app.users_tutorial ut
         WHERE ut.user_id = %s
        """, [self.user_id])

        if row is None or row["tutorial"] is None:
            return {}
        else:
            return json.loads(row["tutorial"])
        