from server_core.commands.command_data import CommandData
from server_core.commands.commands_handler import CommandsHandler
from server_core.utils.utils import Utils
from src.db.user_db_model import UserDbModel
from src.exceptions.auth_exception import AuthException
from server_core.logs.logs_db_model import LogsDbModel
from src.session.public_session import PublicSession


public_commands_handler = CommandsHandler(
    session_class=PublicSession
)


@public_commands_handler.register("public:login_credentials")
def public_login_credentials(session: PublicSession, data: CommandData):
    
    email = data.ensure('email', lambda x: Utils.is_string(x, 100))
    password_raw = data.ensure('password_raw', Utils.is_bool)
    
    try:
        return session.login(
            email=email,
            password_raw=password_raw
        )
    except AuthException as e:
        return {
            "error_id": e.error_id
        }


@public_commands_handler.register("public:is_logged")
def public_is_logged(session: PublicSession):

    user_id = session.get_user_id_from_token()
    role = None
    if user_id is not None:
        role = UserDbModel(conn=session.conn(), user_id=user_id).get_role()
    return {
        'user_id': user_id,
        'role': role
    }


@public_commands_handler.register("public:logout")
def public_logout(session: PublicSession):
    session.logout()


@public_commands_handler.register("public:log_client_error")
def public_log_client_error(session: PublicSession, data: CommandData):
    user_id = session.get_user_id_from_token()
    LogsDbModel.client_error_log(
        user_id=user_id if user_id is not None else 0,
        message=data.ensure('message', lambda x: Utils.is_string(x, 10000))
    )

# @public_commands_handler.register("public:confirm_client_pending_registration")
# def public_confirm_client_pending_registration(session: PublicSession, data: CommandData):
#     session.clients_db().confirm_client_pending_registration(
#         uuid=data.get('uuid'), 
#         password_raw=data.get('password_raw')
#     )

# @public_commands_handler.register("public:request_password_change")
# def request_password_change(session: PublicSession, data: CommandData):
#     assert Utils.is_email(data.payload["email"])
    
#     users_db = UsersDbModel(session.conn())
#     user_id = users_db.get_user_id_from_email(email=data.payload['email'])
    
#     if user_id != 0:
#         users_db.request_password_change(user_id=user_id)
#         UserModel(
#             conn=gb_session().conn(),
#             user_id=user_id
#         ).request_password_change()
