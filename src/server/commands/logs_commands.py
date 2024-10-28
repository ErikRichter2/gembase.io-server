from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from src.server.models.logs.logs_model import LogsModel
from src.session.session import gb_session


@command("log:client_error")
def log__client_error(data: CommandData):
    user_id = gb_session().logged_user_id()
    message = data.payload["message"][:10000]
    LogsModel.client_error_log(
        user_id=user_id if user_id is not None else 0,
        message=message
    )
