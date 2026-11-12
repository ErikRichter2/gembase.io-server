from server_core.exception.server_exception import ServerError, ServerException


class AuthException(ServerException):
    INVALID_USERNAME_OR_PASSWORD = ServerError(id="AUTH001", message="Invalid username or password !")
    USER_IS_NOT_LOGGED = ServerError(id="AUTH002", message="User is not logged !")
