from dataclasses import dataclass
from server_core.exception.server_exception import ServerError, ServerException


class DbException(ServerException):
    SELECTED_0_EXPECTED_1 = ServerError(id="DB001", message="Selected 0 rows, expected 1")
    SELECTED_MORE_THAN_1_EXPECTED_1 = ServerError(id="DB002", message="Selected more than 1 rows, expected 1")
