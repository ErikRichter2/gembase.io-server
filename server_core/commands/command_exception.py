from server_core.exception.server_exception import ServerError, ServerException

class CommandException(ServerException):
    COMMAND_ID_ALREADY_REGISTERED = ServerError(id="CMD001", message="Command %command_id% already registered")
    COMMAND_HANDLER_ALREADY_REGISTERED = ServerError(id="CMD002", message="Function with name %function_name% already registered")
