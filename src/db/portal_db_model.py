from typing import Optional

from server_core.db.db_connection import DbConnection
from src.db.base.base_db_model import BaseDbModel
from src.db.games_db_model import GamesDbModel
from src.db.user_db_model import UserDbModel
from src.models.user_obfuscator import UserObfuscator


class PortalDbModel(BaseDbModel):
    
    def __init__(self, conn: DbConnection):
        super(PortalDbModel, self).__init__(conn=conn)
        self.games_db = GamesDbModel(conn=conn)
        
    
