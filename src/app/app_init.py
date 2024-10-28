from sys import path

from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix

from gembase_server_core.environment.runtime_constants import rr
from gembase_server_core.private_data.private_data_model import PrivateDataModel
from gembase_server_core.utils.gb_utils import GbUtils
from src.app.app_init_commands import AppInitCommands


class AppInit:

    @staticmethod
    def __init_routes():
        # init routes
        # noinspection PyUnresolvedReferences
        import src.app.routes  # noqa: E402

    def __init__(self, app):
        CORS(app)
        path.append(str(GbUtils.get_project_root_path().absolute()))
        secret_key = PrivateDataModel.get_private_data()['flask']['config']['secret_key']
        rr.FLASK_SECRET_KEY = secret_key
        app.config['SECRET_KEY'] = secret_key
        app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
        rr.IS_DEBUG = app.debug
        AppInit.__init_routes()
        AppInitCommands(app)
