from server_core.commands.command_data import CommandData
from server_core.commands.commands_handler import CommandsHandler
from server_core.utils.utils import Utils
from src.session.users_session import UsersSession


users_commands_handler: CommandsHandler = CommandsHandler(
    session_class=UsersSession
)


@users_commands_handler.register("users:change_password")
def users_change_password(session: UsersSession, data: CommandData):
    session.users_db().change_password(
        user_id=data.ensure('user_id', Utils.is_int),
        password_raw=data.ensure('password', lambda x: Utils.is_string(x, max_length=100))
    )


@users_commands_handler.register("users:delete", role="admin")
def users_delete(session: UsersSession, data: CommandData):
    user_id = data.ensure("user_id", Utils.is_int)
    deleted_by_user_id = session.user_id

    assert user_id != deleted_by_user_id

    return session.users_db().delete(
        user_id=user_id,
        deleted_by_user_id=deleted_by_user_id
    )

@users_commands_handler.register("users:update_user")
def update_user(session: UsersSession, data: CommandData):
    session.user_db().update(
        first_name=data.ensure('first_name', lambda x: Utils.is_string(x, 100)),
        last_name=data.ensure('last_name', lambda x: Utils.is_string(x, 100)),
        position_area=data.ensure('position_area', Utils.is_int),
        position_role=data.ensure('position_role', Utils.is_int))

