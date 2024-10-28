from gembase_server_core.exception.base_app_exception import BaseAppException


class CommandException(BaseAppException):

    CMD001 = "CMD001"
    CMD002 = "CMD002"
    CMD003 = "CMD003"

    ERROR_MESSAGES = {
        CMD001: "Permission exception",
        CMD002: "Command %command_id% already registered",
        CMD003: "Function with name %function_name% already registered",
    }

    def __init__(self, error_id, **kwargs):
        super(CommandException, self).__init__("commands", error_id, CommandException.ERROR_MESSAGES, **kwargs)
