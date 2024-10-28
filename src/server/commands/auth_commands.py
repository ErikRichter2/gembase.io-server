from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from src.session.auth_exception import AuthException
from src.session.session import gb_session, is_logged


@command("auth:login_token")
def auth_login_token():
    if not gb_session().is_logged():
        return {
            "state": "error"
        }

    return {
        "state": "ok",
        "user_data": gb_session().user().get_client_data(),
    }


@command("auth:login_credentials")
def auth_login_credentials(data: CommandData):

    if 'recaptcha_token' not in data.payload:
        raise(AuthException(AuthException.AUTH001))

    login_data = gb_session().login(
        email=data.payload['email'],
        password=data.payload['password'],
        recaptcha_token=data.payload['recaptcha_token']
    )

    return login_data


@command("auth:logout", [is_logged])
def auth_logout():
    gb_session().logout()


@command("auth:fake_logout", [is_logged])
def auth__fake_logout():
    if not gb_session().is_logged():
        return

    if gb_session().user().is_fake_logged():
        gb_session().user().fake_logout()
