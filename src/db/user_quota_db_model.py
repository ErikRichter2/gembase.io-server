from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.quota.user_quota_context import UserQuotaContext
from src.db.user_db_model import UserDbModel

class UserQuotaDbModel(BaseUserDbModel):
    
    def __init__(self, user_db: UserDbModel, quota_type: str, context: object):
        super(UserQuotaDbModel, self).__init__(user_db=user_db)
        self.__quota_type = quota_type
        self.__quota_context = UserQuotaContext(user_db=user_db, quota_type=quota_type, context=context)

    def quota_type(self):
        return self.__quota_type
    
    def quota_context(self):
        return self.__quota_context