import traceback
from typing import Optional

import flask
from werkzeug.exceptions import HTTPException

from server_core.exception.server_exception import ServerException
from server_core.logs.logs_db_model import LogsDbModel
from src.app.app_init import APP_IS_DEBUG


def create_response_from_exception(
        e: Exception,
        user_id: Optional[int] = None,
        last_command_id: Optional[str] = None
):
    if isinstance(e, ServerException):
        error = e
    elif isinstance(e, HTTPException):
        error = ServerException(error=f"HTTP: {e.name} - {e.description}", error_code=e.code)
    else:
        error = ServerException(error=f"EXCEPTION: {str(e)}")

    if error.error_code != 404:
        print(error.message)
        print(traceback.format_exc())

    LogsDbModel.server_error_log(
        user_id=user_id,
        title=error.message,
        stacktrace=traceback.format_exc(),
        exception=True
    )

    response = {
        'message': error.message
    }
    
    if error.error_id is not None:
        response["error_id"] = error.error_id

    if APP_IS_DEBUG:
        response['last_command_id'] = last_command_id
        response['message'] = error.message
        response['call_stack'] = traceback.format_exc()
        response["server_error"] = True

    return flask.jsonify(response), error.error_code
