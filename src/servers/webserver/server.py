# =========== #
# init app    #
# =========== #
from src.app.app_init import app
server = app

# =========== #
# init routes #
# =========== #
# noinspection PyUnresolvedReferences
import src.routes.web_server  # noqa: E402
