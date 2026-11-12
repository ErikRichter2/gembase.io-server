from server_core.db.db_connection import DbConnection
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.quota.user_quota_context import UserQuotaContext

class UserQuotaDbModel(BaseUserDbModel):
    
    def __init__(self, conn: DbConnection, user_id: int, quota_type: str, context: object):
        super(UserQuotaDbModel, self).__init__(conn=conn, user_id=user_id)
        self.__quota_type = quota_type
        self.__quota_context = UserQuotaContext(conn=conn, user_id=user_id, quota_type=quota_type, context=context)

    def quota_type(self):
        return self.__quota_type
    
    def quota_context(self):
        return self.__quota_context