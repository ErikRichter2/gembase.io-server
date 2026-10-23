from typing import cast

from overrides import override

from server_core.commands.command_data import CommandData
from src.db.user_db_model import UserDbModel
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
            assert self.user_db.is_admin()

    @property
    def users_db(self) -> UsersDbModel:
        m = self.get_or_create_db_model(UsersDbModel)
        return cast(UsersDbModel, m)
    
    @property
    def user_db(self) -> UserDbModel:
        m = self.get_or_create_db_model(UserDbModel, user_id=self.user_id)
        return cast(UserDbModel, m)
