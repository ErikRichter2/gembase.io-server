# init commands
import flask

# noinspection PyUnresolvedReferences
import src.server.commands  # noqa: E402

from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_response_file import CommandResponseFile
from gembase_server_core.commands.commands_model import CommandsModel
from gembase_server_core.environment.runtime_constants import rr
from src.app.app_utils import AppUtils


class AppInitCommands:

    def __init__(self, app):

        def __command_before(command: CommandData):
            from src.session.session import gb_session

            if gb_session().is_logged():
                gb_session().user().obfuscator().to_server(command.payload)

            if rr.is_debug():
                return

            if gb_session().is_logged() and not gb_session().user().is_fake_logged() and not gb_session().user().is_admin():
                user_id = gb_session().user().get_id()
                conn = gb_session().conn()

                conn.query("""
                UPDATE app.users u
                   SET u.session_t = NOW()
                 WHERE u.id = %s
                """, [user_id])
                conn.commit()

                cnt = conn.select_one("""
                SELECT count(1) as cnt
                  FROM app_temp_data.users_commands_calls u
                 WHERE u.user_id = %s
                   AND DATE_ADD(u.t, INTERVAL 1 MINUTE ) >= NOW()
                """, [user_id])["cnt"]

                if cnt >= 1000:
                    raise Exception(f"Requests limit quota reached")

                conn.query("""
                INSERT INTO app_temp_data.users_commands_calls (user_id, command) VALUES (%s, %s)
                """, [user_id, command.id])
                conn.commit()

        def __command_after(command_data: CommandData, command_response: any):
            from src.session.session import gb_session

            gb_session().conn().commit()

            res = {
                "state": "ok"
            }

            user = None
            if gb_session().is_logged():
                user = gb_session().user()

            if command_response is not None:
                if isinstance(command_response, CommandResponseFile):
                    if user is not None:
                        user.obfuscator().to_client(command_response.payload)
                    res["payload"] = command_response.payload
                    res["file_name"] = command_response.filename
                    res["mimetype"] = command_response.mime
                else:
                    if user is not None:
                        user.obfuscator().to_client(command_response)
                    res["payload"] = command_response

            add_credits_to_response = getattr(flask.g, "add_credits_to_response", False)
            if add_credits_to_response and user is not None:
                res["credits"] = user.get_credits()

            return res

        CommandsModel.set_endpoint(
            app=app,
            route="/api/endpoint",
            route_files="/api/endpoint/files",
            create_response_from_exception_callback=AppUtils.create_response_from_exception
        )
        CommandsModel.on_before_command_callback = __command_before
        CommandsModel.on_after_command_callback = __command_after
