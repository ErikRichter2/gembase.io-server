import traceback
from typing import cast

from gembase_server_core.commands.commands_model import CommandsModel
from gembase_server_core.environment.runtime_constants import rr
from gembase_server_core.exception.base_app_exception import BaseAppException
from src.server.models.logs.logs_model import LogsModel


class AppUtils:

    @staticmethod
    def create_response_from_exception(e):
        from src.session.session import gb_session

        error: BaseAppException | None = None

        if not isinstance(e, BaseAppException):
            error = BaseAppException("app", "ERR999", {"ERR999": str(e)})
        else:
            error = cast(BaseAppException, e)

        if error.error_code != 404:
            print(error.message)
            print(traceback.format_exc())

        res = {
            "id": error.id,
            "server_error": True
        }

        user_id = -1
        if gb_session().is_logged():
            user_id = gb_session().user().get_id()

        LogsModel.server_error_log(
            user_id=user_id,
            title=error.message,
            stacktrace=traceback.format_exc(),
            exception=True
        )

        if rr.is_debug() or gb_session().is_admin():
            res['last_command_id'] = CommandsModel.get_last_command_id()
            res['message'] = error.message
            res['module'] = error.module
            res['call_stack'] = traceback.format_exc()
        elif error.is_public:
            res['message'] = error.message
            res['is_public'] = True
        else:
            res['is_default'] = True

        return res
