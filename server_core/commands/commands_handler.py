import inspect

import flask
from flask import Response, send_file

from server_core.commands.command_data import CommandData
from server_core.commands.command_exception import CommandException
from src.app.app_exception_response import create_response_from_exception
from src.env_data import ENV_DATA
from src.session.base.base_session import BaseSession
from src.session.public_session import PublicSession
from src.session.users_session import UsersSession

used_commands_ids: set[str] = set()
used_commands_handlers: set[str] = set()


class CommandsHandler:

    def __init__(self, session_class):
        self.__session_class = session_class
        self.__commands: dict[str, dict] = {}

    def register(self, command_id: str, **kwargs):
        def decorator(f):
            if command_id in used_commands_ids:
                raise CommandException(CommandException.COMMAND_ID_ALREADY_REGISTERED, command_id=command_id)
            function_name = f.__name__
            if function_name in used_commands_handlers:
                raise CommandException(CommandException.COMMAND_HANDLER_ALREADY_REGISTERED, function_name=function_name)
            used_commands_ids.add(command_id)
            used_commands_handlers.add(function_name)
            self.__commands[command_id] = {
                "f": f,
                "metadata": kwargs
            }
            return f

        return decorator

    def create_command_data_from_request(self) -> CommandData:
        post_data = flask.request.get_json()['data']

        if 'id' not in post_data:
            raise Exception("Missing id in command data")
        if post_data['id'] not in self.__commands:
            raise Exception("Unknown command id: " + post_data['id'])

        command_data = CommandData()
        command_data.id = post_data['id']
        if 'payload' in post_data:
            command_data.payload = post_data['payload']
        return command_data

    def process_request(self) -> tuple[Response, int]:
        last_command_id = None
        session = None
        try:
            session = self.__session_class()
            session.cache_conn_in_flask()
            session.initialize_from_request()
            
            command_data = self.create_command_data_from_request()
            command_data.metadata = self.__commands[command_data.id]["metadata"]

            session.validate_command(command_data=command_data)

            last_command_id = command_data.id

            print(f"Command: {command_data.id}")
            
            self.__before_command_execution(session=session, command_data=command_data)

            f = self.__commands[command_data.id]["f"]

            args_len = len(inspect.getfullargspec(f).args)

            if args_len == 0:
                command_response = f()
            elif args_len == 1:
                command_response = f(session)
            elif args_len == 2:
                command_response = f(session, command_data)
            else:
                raise Exception(f"Unknown arguments for command {command_data.id}")

            self.__after_command_execution(session=session, command_response=command_response)
            
            session.commit_and_close()

            if isinstance(command_response, Response):
                return command_response, 200
            else:
                return flask.jsonify({
                    "state": "ok",
                    "payload": command_response
                }), 200
        except Exception as err:
            if session is not None:
                session.rollback_and_close()
                
            session = PublicSession()
            user_id = session.get_user_id_from_token()
            if user_id is None:
                user_id = session.get_user_id_from_api_key()
            session.rollback_and_close()
            return create_response_from_exception(
                e=err,
                user_id=user_id,
                last_command_id=last_command_id
            )


    def __before_command_execution(self, session: BaseSession, command_data: CommandData):
        
        user_db = None
        if isinstance(session, UsersSession):
            user_db = session.user_db
        
        if user_db is not None:
            user_db.obfuscator.to_server(command_data.payload)

        if ENV_DATA.is_debug():
            return

        if user_db is not None:
            user_id = user_db.user_id
            conn = user_db.conn

            conn.query("""
            UPDATE gb_app.users u
                SET u.session_t = NOW()
                WHERE u.id = %s
            """, [user_id])
            conn.commit()

            cnt = conn.select_one("""
            SELECT count(1) as cnt
                FROM gb_app_temp_data.users_commands_calls u
                WHERE u.user_id = %s
                AND DATE_ADD(u.t, INTERVAL 1 MINUTE ) >= NOW()
            """, [user_id])["cnt"]

            if cnt >= 1000:
                raise Exception(f"Requests limit quota reached")

            conn.query("""
            INSERT INTO gb_app_temp_data.users_commands_calls (user_id, command) VALUES (%s, %s)
            """, [user_id, command_data.id])
            conn.commit()


    def __after_command_execution(self, session: BaseSession, command_response: dict):

        res: dict = {
            "state": "ok"
        }

        user_db = None
        if isinstance(session, UsersSession):
            user_db = session.user_db

        if command_response is not None:
            if user_db is not None:
                user_db.obfuscator.to_client(command_response)
            res["payload"] = command_response

        add_credits_to_response = getattr(flask.g, "add_credits_to_response", False)
        if add_credits_to_response and user_db is not None:
            res["credits"] = user_db.get_credits()

        return res
