from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from src.session.session import gb_session, is_admin, is_logged
from src.utils.gembase_utils import GembaseUtils


@command("tutorial:reset", [is_admin])
def tutorial__reset():
    gb_session().models().admin().reset_tutorial()


@command("tutorial:module_seen", [is_logged])
def tutorial__module_seen(data: CommandData):
    assert GembaseUtils.is_int(data.payload["module_id"])

    gb_session().models().tutorial().set_module_seen(
        module_id=data.payload["module_id"]
    )
    return gb_session().user().get__tutorial_finished()
