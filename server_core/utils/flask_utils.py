from typing import Optional

import flask

from server_core.db.db_connection import DbConnection


class FlaskUtils:

    @staticmethod
    def get_ip_address() -> str:
        return flask.request.remote_addr

    @staticmethod
    def get_auth_token() -> Optional[str]:
        client_token = None
        if 'Authorization' in flask.request.headers:
            if flask.request.headers['Authorization'] is not None:
                client_token = flask.request.headers['Authorization'][6:]
                if client_token == 'null':
                    client_token = None
        return client_token

    @staticmethod
    def get_or_create_session_conn() -> DbConnection:
        conn = getattr(flask.g, "session_conn", None)
        if conn is None:
            conn = DbConnection()
            setattr(flask.g, "session_conn", conn)
        return conn

    @staticmethod
    def close_session_conn():
        conn: Optional[DbConnection] = getattr(flask.g, "session_conn", None)
        if conn is not None:
            conn.close()
            setattr(flask.g, "session_conn", None)
