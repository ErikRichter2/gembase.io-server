import json

import flask
import requests

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.db.db_exception import DbException
from gembase_server_core.environment.runtime_constants import rr
from gembase_server_core.private_data.private_data_model import PrivateDataModel
from gembase_server_core.jwt_token import TokenData
from src.server.models.user.user_data import UserData
from src.session.auth_exception import AuthException
from src.utils.hash_utils import sha256


class GbSessionHelper:

    @staticmethod
    def __compare_hash(value, secret, compare_to_hash):
        my_hash, secret = sha256(value, secret)
        return my_hash == compare_to_hash

    @staticmethod
    def validate_recaptcha(recaptcha_token: str):
        if rr.is_debug() or not rr.is_prod():
            return

        secret_key = getattr(flask.g, "secret_key", None)
        if secret_key is None:
            secret_key = PrivateDataModel.get_private_data()["google"]["recaptcha"]["secret_key"]
            setattr(flask.g, "secret_key", secret_key)

        recaptcha_result = requests.post("https://www.google.com/recaptcha/api/siteverify", data={
            "secret": secret_key,
            "response": recaptcha_token,
            "remoteip": flask.request.remote_addr
        })

        if recaptcha_result.status_code != 200:
            raise AuthException(AuthException.AUTH001)

        content = json.loads(recaptcha_result.text)

        if not content["success"] or content["score"] < 0.5:
            raise AuthException(AuthException.AUTH001)

    @staticmethod
    def get_user_id_by_credentials(conn: DbConnection, email: str, password: str) -> int:

        user_id = UserData.get_user_id_from_email(conn=conn, email=email)

        if user_id == 0:
            raise AuthException(AuthException.AUTH002)

        row = conn.select_one_or_none("""
        SELECT password, secret FROM app.users WHERE id = %s AND blocked IS NULL
        """, [user_id])

        if row is None or not GbSessionHelper.__compare_hash(password, row["secret"], row["password"]):
            raise AuthException(AuthException.AUTH002)

        return user_id

    @staticmethod
    def get_user_id_from_token(conn: DbConnection, token: str | None) -> int | None:
        if token is None:
            return None

        try:
            parsed_token = TokenData.decode(token)
        except Exception:
            return None

        try:
            row = conn.select_one("""
                SELECT t.user_id, u.password, u.secret
                  FROM app_temp_data.users_login_tokens t,
                       app.users u
                 WHERE t.guid = %s
                   AND t.user_id = u.id
                   AND DATE_ADD(t.created, INTERVAL t.expire_days DAY)  > NOW()  
                   AND u.blocked IS NULL
            """, [parsed_token['token_guid']])
        except DbException:
            return None

        if not GbSessionHelper.__compare_hash(parsed_token['password'], row["secret"], row["password"]):
            return None

        return row["user_id"]
