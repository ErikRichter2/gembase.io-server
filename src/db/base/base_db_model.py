from server_core.db.db_connection import DbConnection


class BaseDbModel:

    def __init__(self, conn: DbConnection):
        self.__conn: DbConnection = conn

    @property
    def conn(self) -> DbConnection:
        return self.__conn
