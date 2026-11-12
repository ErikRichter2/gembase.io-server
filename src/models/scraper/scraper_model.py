from server_core.db.db_connection import DbConnection
from src.db.base.base_db_model import BaseDbModel


class ScraperModel(BaseDbModel):
    
    def __init__(self, conn: DbConnection):
        super(ScraperModel, self).__init__(conn=conn)
    
