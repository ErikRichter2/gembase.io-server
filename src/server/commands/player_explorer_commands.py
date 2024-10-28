from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from src.session.session import is_logged, gb_session


@command("portal:get_player_explorer_data", [is_logged])
def portal__get_player_explorer_data(data: CommandData):
    input_filters = None
    if "filters" in data.payload:
        input_filters = data.payload["filters"]

    show = None
    if "show" in data.payload:
        show = data.payload["show"]

    return gb_session().models().player_explorer().get_data(
        input_filters=input_filters,
        show=show
    )
