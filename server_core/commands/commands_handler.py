import inspect

import flask
from flask import Response, send_file

from server_core.commands.command_data import CommandData
from server_core.commands.command_exception import CommandException
from src.app.app_exception_response import create_response_from_exception
from src.session.public_session import PublicSession

registered_commands = {}
used_commands_names: list[str] = []


class CommandsHandler:

    def __init__(self, session_class):
        self.__session_class = session_class

    def register(self, command_id: str, **kwargs):
        def decorator(f):
            if command_id in registered_commands:
                raise CommandException(CommandException.COMMAND_ID_ALREADY_REGISTERED, command_id=command_id)
            function_name = f.__name__
            if function_name in used_commands_names:
                raise CommandException(CommandException.COMMAND_HANDLER_ALREADY_REGISTERED, function_name=function_name)
            used_commands_names.append(function_name)
            registered_commands[command_id] = {
                "f": f,
                "metadata": kwargs
            }
            return f

        return decorator

    def create_command_data_from_request(self) -> CommandData:
        post_data = flask.request.get_json()['data']

        if 'id' not in post_data:
            raise Exception("Missing id in command data")
        if post_data['id'] not in registered_commands:
            raise Exception("Unknown command id: " + post_data['id'])

        command_data = CommandData()
        command_data.id = post_data['id']
        if 'payload' in post_data:
            command_data.payload = post_data['payload']
        return command_data

    def process_request(self) -> (Response, int):
        last_command_id = None
        session = None
        try:
            session = self.__session_class()
            session.cache_conn_in_flask()
            session.initialize_from_request()
            
            command_data = self.create_command_data_from_request()
            command_data.metadata = registered_commands[command_data.id]["metadata"]

            session.validate_command(command_data=command_data)

            last_command_id = command_data.id

            print(f"Command: {command_data.id}")

            f = registered_commands[command_data.id]["f"]

            args_len = len(inspect.getfullargspec(f).args)

            if args_len == 0:
                command_response = f()
            elif args_len == 1:
                command_response = f(session)
            elif args_len == 2:
                command_response = f(session, command_data)
            else:
                raise Exception(f"Unknown arguments for command {command_data.id}")

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
