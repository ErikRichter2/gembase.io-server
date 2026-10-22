from server_core.db.db_connection import DbConnection


class BaseDbModel:

    def __init__(self, conn: DbConnection):
        self.__conn: DbConnection = conn
        self.__db_models = {}

    def conn(self) -> DbConnection:
        return self.__conn
    
    def get_or_create_db_model(self, model, **kwargs) -> object:
        if model not in self.__db_models:
            self.__db_models[model] = model(conn=self.conn(), **kwargs)
        return self.__db_models[model]
