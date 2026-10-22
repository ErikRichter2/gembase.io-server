from typing import Optional, cast

from server_core.commands.command_data import CommandData
from server_core.db.db_connection import DbConnection
from server_core.utils.flask_utils import FlaskUtils
from src.db.user_db_model import UserDbModel


class BaseSession:

    def __init__(self):
        self.__conn: Optional[DbConnection] = None
        self.__db_models = {}
        self.__cache_conn_in_flask = False
        
    def cache_conn_in_flask(self):
        self.__cache_conn_in_flask = True
        
    def initialize_from_request(self):
        pass

    def validate_command(self, command_data: CommandData):
        pass

    def rollback_and_close(self):
        if self.__conn is not None:
            self.conn().rollback()
            self.conn().close()
            self.__conn = None

    def commit_and_close(self):
        if self.__conn is not None:
            self.conn().commit()
            self.conn().close()
            self.__conn = None

    def conn(self):
        if self.__conn is None:
            if self.__cache_conn_in_flask:
                self.__conn = FlaskUtils.get_or_create_session_conn()
            else:
                self.__conn = DbConnection()
        return self.__conn

    def get_or_create_db_model(self, model, ignore_cache=False, **kwargs) -> object:
        if ignore_cache:
            return model(conn=self.conn(), **kwargs)
        if model not in self.__db_models:
            self.__db_models[model] = model(conn=self.conn(), **kwargs)
        return self.__db_models[model]
