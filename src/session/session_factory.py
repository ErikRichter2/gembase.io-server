import flask

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.user.user_model import UserModel
from src.session.session_instance import GbSessionInstance


class GbSessionFactory:

    @staticmethod
    def create_session_local(user: UserModel, conn: DbConnection | None = None) -> GbSessionInstance:
        session = GbSessionInstance(
            client_token="",
            ip_address=""
        )

        session.set_data_local(
            user=user,
            conn=conn
        )

        return session

    @staticmethod
    def get_or_create_session_from_flask_request(create_if_not_exists=False) -> GbSessionInstance | None:
        cached = getattr(flask.g, "gb_session", None)
        if cached is None and create_if_not_exists:
            client_token = None
            if 'Authorization' in flask.request.headers:
                if flask.request.headers['Authorization'] is not None:
                    client_token = flask.request.headers['Authorization'][6:]
                    if client_token == 'null':
                        client_token = None
            cached = GbSessionInstance(
                client_token=client_token,
                ip_address=flask.request.remote_addr
            )
            setattr(flask.g, "gb_session", cached)
        return cached
