import mimetypes

import os
from sys import path
from flask import Flask
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix

from server_core.utils.flask_utils import FlaskUtils
from server_core.utils.utils import Utils
from src.env_data import ENV_DATA

path.append(str(Utils.get_project_root_path().absolute()))

mimetypes.add_type('application/javascript', '.js')
mimetypes.add_type('text/css', '.css')

app = Flask(__name__)

CORS(app, expose_headers=["Content-Disposition"])

APP_IS_DEBUG = app.debug

app.config['SECRET_KEY'] = ENV_DATA.flask_secret_key

app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)


@app.teardown_appcontext
def app_teardown(exception):
    FlaskUtils.close_session_conn()


@app.errorhandler(Exception)
def unhandled_exception(e):
    from src.app.app_exception_response import create_response_from_exception
    return create_response_from_exception(e=e)


@app.errorhandler(404)
def not_found(e):
    return unhandled_exception(e) if APP_IS_DEBUG else ("Page not found", 404)
