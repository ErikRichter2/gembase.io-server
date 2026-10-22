from src.db.base.base_db_model import BaseDbModel
from src.db.user_db_model import UserDbModel


class BaseUserDbModel(BaseDbModel):

    def __init__(self, user_db: UserDbModel):
        super(BaseUserDbModel, self).__init__(conn=user_db.conn())
        self.__user_db = user_db

    def user_db(self):
        return self.__user_db

    def user_id(self):
        return self.__user_db.user_id()
