from typing import TypeVar

from server_core.db.db_connection import DbConnection
from src.db.base.base_db_model import BaseDbModel

T = TypeVar('T', bound='BaseUserDbModel')

class BaseUserDbModel(BaseDbModel):

    def __init__(self, conn: DbConnection, user_id: int):
        super(BaseUserDbModel, self).__init__(conn=conn)
        self.__user_id = user_id
        self.__db_models = {}

    def __get_or_create_db_model(self, model: type[T], **kwargs) -> T:
        if model not in self.__db_models:
            self.__db_models[model] = model(conn=self.conn, user_id=self.user_id, **kwargs)
        return self.__db_models[model]        

    @property
    def user_id(self):
        return self.__user_id

    @property
    def user_db(self):
        from src.db.user_db_model import UserDbModel
        return self.__get_or_create_db_model(UserDbModel)

    @property
    def portal_db(self):
        from src.db.user_portal_db_model import UserPortalDbModel
        return self.__get_or_create_db_model(UserPortalDbModel)
        
    @property
    def billing_db(self):
        from src.db.user_billing_db_model import UserBillingDbModel
        return self.__get_or_create_db_model(UserBillingDbModel)
    
    @property
    def platform_db(self):
        from src.db.user_platform_db_model import UserPlatformDbModel
        return self.__get_or_create_db_model(UserPlatformDbModel)
    
    @property
    def tutorial_db(self):
        from src.db.user_tutorial_db_model import UserTutorialDbModel
        return self.__get_or_create_db_model(UserTutorialDbModel)
    
    @property
    def organization_db(self):
        from src.db.user_organization_db_model import UserOrganizationDbModel
        return self.__get_or_create_db_model(UserOrganizationDbModel)

    @property
    def games_db(self):
        from src.db.user_games_db_model import UserGamesDbModel
        return self.__get_or_create_db_model(UserGamesDbModel)

    @property
    def dms_db(self):
        from src.db.user_dms_db_model import UserDmsDbModel
        return self.__get_or_create_db_model(UserDmsDbModel)

    @property
    def studies_db(self):
        from src.db.user_studies_db_model import UserStudiesDbModel
        return self.__get_or_create_db_model(UserStudiesDbModel)
