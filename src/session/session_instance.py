from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.jwt_token import TokenData
from src.session.auth_exception import AuthException
from src.server.models.session.gb_session_models import GbSessionModels
from src.server.models.user.user_model import UserModel
from src.session.session_helper import GbSessionHelper
from src.utils.gembase_utils import GembaseUtils


class GbSessionInstance:

    def __init__(self, client_token: str | None = None, ip_address: str | None = None):
        self.__conn: DbConnection | None = None
        self.__user: UserModel | None = None
        self.__client_token: str | None = client_token
        self.__ip_address: str | None = ip_address
        self.__models: GbSessionModels | None = None

    def conn(self) -> DbConnection:
        if self.__conn is None:
            self.__conn = DbConnection()
        return self.__conn

    def login(self, email: str, password: str, recaptcha_token: str | None = None):

        self.__user = None

        if recaptcha_token is not None:
            GbSessionHelper.validate_recaptcha(recaptcha_token=recaptcha_token)

        user_id = GbSessionHelper.get_user_id_by_credentials(
            conn=self.conn(),
            email=email,
            password=password,
        )

        expire_days = 14
        token_guid = GembaseUtils.get_guid()

        self.conn().query("""
        INSERT INTO app_temp_data.users_login_tokens (guid, user_id, ip_address, expire_days) 
        VALUES (%s, %s, %s, %s)
        """, [token_guid, user_id, self.__ip_address, expire_days])

        self.__client_token = TokenData.encode(token_guid, password)

        return {
            "user_data": self.user().get_client_data(),
            "client_token": self.__client_token
        }

    def logout(self):
        if self.__user is None:
            return
        self.conn().query("DELETE FROM app_temp_data.users_login_tokens WHERE user_id = %s", [self.__user.get_id()])
        self.__user = None

    def user_id(self) -> int:
        return self.user().get_id()

    def user(self) -> UserModel:
        if self.__user is None:

            user_id = GbSessionHelper.get_user_id_from_token(
                conn=self.conn(),
                token=self.__client_token
            )

            if user_id is None:
                raise AuthException(AuthException.AUTH007)

            user = UserModel(
                conn=self.conn(),
                user_id=user_id
            )

            if user.is_admin():
                user = user.get_user_who_is_fake_logged_by_this_user()

            self.__user = user

        return self.__user

    def models(self) -> GbSessionModels:
        if self.__models is None:
            self.__models = GbSessionModels(self)
        return self.__models

    def set_data_local(self, conn: DbConnection | None, user: UserModel):
        self.__conn = conn
        self.__user = user

    def logged_user_id(self) -> int | None:
        return GbSessionHelper.get_user_id_from_token(
            conn=self.conn(),
            token=self.__client_token
        )

    def is_logged(self):
        return self.logged_user_id() is not None

    def is_admin(self):
        if self.is_logged() and self.user().is_admin():
            return True
        return False
