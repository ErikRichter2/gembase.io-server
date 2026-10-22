from typing import cast

import flask

from server_core.utils.flask_utils import FlaskUtils
from src.db.users_db_model import UsersDbModel
from src.session.base.base_session import BaseSession


class PublicSession(BaseSession):

    def users_db(self) -> UsersDbModel:
        m = self.get_or_create_db_model(UsersDbModel)
        return cast(UsersDbModel, m)
    
    def login(self, email: str, password_raw: str):
        return self.users_db().login(
            email=email,
            password_raw=password_raw
        )

    def logout(self):
        user_id = self.get_user_id_from_token()
        if user_id is not None:
            self.users_db().logout(user_id=user_id)

    def get_user_id_from_token(self):
        auth_token = FlaskUtils.get_auth_token()
        if auth_token is not None:
            return self.users_db().get_user_id_from_auth_token(auth_token=auth_token)
        return None
    
    def get_user_id_from_api_key(self):
        if "X-API-KEY" in flask.request.headers:
            return self.users_db().get_user_id_from_api_key(api_key=flask.request.headers["X-API-KEY"])
        api_key = flask.request.args.get('apiKey')
        if api_key is not None:
            return self.users_db().get_user_id_from_api_key(api_key=api_key)
        api_key = flask.request.args.get('api_key')
        if api_key is not None:
            return self.users_db().get_user_id_from_api_key(api_key=api_key)
        return None
