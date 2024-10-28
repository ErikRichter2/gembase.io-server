from __future__ import annotations
from typing import TYPE_CHECKING

from gembase_server_core.db.db_connection import DbConnection

if TYPE_CHECKING:
    from src.session.session_instance import GbSessionInstance


class BaseSessionModel:

    def __init__(self, session: GbSessionInstance):
        self.__session = session

    def session(self) -> GbSessionInstance:
        return self.__session

    def conn(self) -> DbConnection:
        return self.__session.conn()

    def user_id(self) -> int:
        return self.__session.user().get_id()
