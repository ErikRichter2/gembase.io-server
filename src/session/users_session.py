from typing import cast

from overrides import override

from server_core.commands.command_data import CommandData
from src.db.user_billing_db_model import UserBillingDbModel
from src.db.user_db_model import UserDbModel
from src.db.user_organization_db_model import UserOrganizationDbModel
from src.db.user_platform_db_model import UserPlatformDbModel
from src.db.user_portal_db_model import UserPortalDbModel
from src.db.user_tutorial_db_model import UserTutorialDbModel
from src.db.users_db_model import UsersDbModel
from src.exceptions.auth_exception import AuthException
from src.session.public_session import PublicSession


class UsersSession(PublicSession):

    def __init__(self):
        self.user_id = None
        super(UsersSession, self).__init__()
        
    def get_user_id(self, try_api_key=False):
        user_id = self.get_user_id_from_token()
        if user_id is None and try_api_key:
            user_id = self.get_user_id_from_api_key()
        return user_id

    @override                
    def initialize_from_request(self):
        super(UsersSession, self).initialize_from_request()
        self.user_id = self.get_user_id()
        if self.user_id is None:
            raise AuthException(AuthException.USER_IS_NOT_LOGGED)

    @override
    def validate_command(self, command_data: CommandData):
        if command_data.get_metadata("role") == "admin":
            assert self.user_db().is_admin()

    def users_db(self) -> UsersDbModel:
        m = self.get_or_create_db_model(UsersDbModel)
        return cast(UsersDbModel, m)
    
    def user_db(self) -> UserDbModel:
        m = self.get_or_create_db_model(UserDbModel, user_id=self.user_id)
        return cast(UserDbModel, m)

    def user_portal_db(self) -> UserPortalDbModel:
        m = self.get_or_create_db_model(UserPortalDbModel, user_db=self.user_db())
        return cast(UserPortalDbModel, m)
    
    def user_billing_db(self) -> UserBillingDbModel:
        m = self.get_or_create_db_model(UserBillingDbModel, user_db=self.user_db())
        return cast(UserBillingDbModel, m)
    
    def user_platform_db(self) -> UserPlatformDbModel:
        m = self.get_or_create_db_model(UserPlatformDbModel, user_db=self.user_db())
        return cast(UserPlatformDbModel, m)
    
    def user_tutorial_db(self) -> UserTutorialDbModel:
        m = self.get_or_create_db_model(UserTutorialDbModel, user_db=self.user_db())
        return cast(UserTutorialDbModel, m)
    
    def user_organization_db(self) -> UserOrganizationDbModel:
        m = self.get_or_create_db_model(UserOrganizationDbModel, user_db=self.user_db())
        return cast(UserOrganizationDbModel, m)
