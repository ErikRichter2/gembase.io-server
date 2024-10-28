from gembase_server_core.exception.base_app_exception import BaseAppException


class DbException(BaseAppException):

    DB001 = "DB001"
    DB002 = "DB002"
    DB003 = "DB003"
    DB999 = "DB999"

    ERROR_MESSAGES = {
        DB001: "Selected 0 rows, expected 1",
        DB002: "Selected more than 1 rows, expected 1",
        DB003: "Selected rows, expected 0",
        DB999: "%msg%",
    }

    def __init__(self, error_id: str, **kwargs):
        super(DbException, self).__init__("db", error_id, DbException.ERROR_MESSAGES, **kwargs)
