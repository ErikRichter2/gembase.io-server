from gembase_server_core.commands.command_exception import CommandException

registered_commands = {}

__used_names: list[str] = []


def command(command_id: str, permissions: [] = None):
    def decorator(f):
        if command_id in registered_commands:
            raise CommandException(CommandException.CMD002, command_id=command_id)
        function_name = f.__name__
        if function_name in __used_names:
            raise CommandException(CommandException.CMD003, function_name=function_name)
        __used_names.append(function_name)
        registered_commands[command_id] = {
            "f": f,
            "permissions": permissions
        }
        return f
    return decorator
