import flask
from flask import Flask

from gembase_server_core.environment.runtime_constants import rr

from src.app.app_init import AppInit
from src.server.models.logs.logs_model import LogsModel

LogsModel.init()

app = Flask(__name__)
AppInit(app)


@app.teardown_appcontext
def app_teardown(exception):
    from src.app.app_teardown import teardown
    teardown()


@app.errorhandler(Exception)
def handle_exception(e):
    from src.app.app_utils import AppUtils
    return flask.jsonify(AppUtils.create_response_from_exception(e)), 500


@app.errorhandler(404)
def not_found(e):
    if rr.is_debug():
        handle_exception(e)
    else:
        return "Page not found", 404
