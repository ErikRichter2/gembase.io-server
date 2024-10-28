import inspect
import json

import flask
from flask import Flask

from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import registered_commands
from gembase_server_core.commands.command_exception import CommandException
from gembase_server_core.commands.files_command_data import FilesCommandData


class CommandsModel:

    on_before_command_callback = None
    on_after_command_callback = None

    __last_command_data: CommandData | None = None

    @staticmethod
    def get_last_command_id() -> str | None:
        if CommandsModel.__last_command_data is not None:
            return CommandsModel.__last_command_data.id
        return None

    @staticmethod
    def __from_post_request() -> CommandData:
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

    @staticmethod
    def __from_files_request() -> FilesCommandData:

        def get_files_from_request() -> []:
            return [flask.request.files[k] for k in flask.request.files]

        form_data = json.loads(flask.request.form['data'])

        if 'id' not in form_data:
            raise Exception("Missing id in command data")
        if form_data['id'] not in registered_commands:
            raise Exception("Unknown command id: " + form_data['id'])

        command_data = FilesCommandData()
        command_data.files = get_files_from_request()
        command_data.id = form_data['id']
        if 'payload' in form_data:
            command_data.payload = form_data['payload']
        return command_data

    @staticmethod
    def set_endpoint(
            app: Flask,
            route: str,
            route_files: str,
            create_response_from_exception_callback
    ):

        def process():
            try:
                res = CommandsModel.__process_command(CommandsModel.__from_post_request())
                return flask.jsonify(res)
            except Exception as err:
                return flask.jsonify(create_response_from_exception_callback(err)), 500

        def process_files():
            try:
                res = CommandsModel.__process_command(CommandsModel.__from_files_request())
                return flask.jsonify(res)
            except Exception as err:
                return flask.jsonify(create_response_from_exception_callback(err)), 500

        app.add_url_rule(
            rule=route,
            view_func=process,
            methods=["POST"]
        )

        app.add_url_rule(
            rule=route_files,
            view_func=process_files,
            methods=["POST"]
        )

    @staticmethod
    def __process_command(command_data: CommandData):
        print(f"Command: {command_data.id}")
        CommandsModel.__last_command_data = command_data

        if CommandsModel.on_before_command_callback is not None:
            CommandsModel.on_before_command_callback(command_data)

        c = registered_commands[command_data.id]

        if c["permissions"] is not None:
            for p in c["permissions"]:
                if not p():
                    raise CommandException(CommandException.CMD001)

        f = c["f"]

        args_len = len(inspect.getfullargspec(f).args)

        if args_len == 0:
            command_response = f()
        elif args_len == 1:
            command_response = f(command_data)
        else:
            raise Exception(f"Unknown arguments for command {command_data.id}")

        if CommandsModel.on_after_command_callback is not None:
            command_response = CommandsModel.on_after_command_callback(command_data, command_response)
        return command_response
