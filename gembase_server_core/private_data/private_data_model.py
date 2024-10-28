import json
import os

from gembase_server_core.environment.runtime_constants import rr
from gembase_server_core.utils.gb_utils import GbUtils


class PrivateDataModel:

    __private_data = {}
    __private_data_path: str | None = None

    @staticmethod
    def get_private_data(env: str = None):

        if PrivateDataModel.__private_data_path is None:
            root_path = GbUtils.get_project_root_path()
            PrivateDataModel.__private_data_path = os.path.join(root_path, os.getenv("PRIVATE_DATA_PATH"))

        if env is None:
            env = rr.ENV

        if env not in PrivateDataModel.__private_data:
            path = os.path.join(PrivateDataModel.__private_data_path, 'private_data.json')
            file_shared = json.load(open(path, 'r'))

            path = os.path.join(PrivateDataModel.__private_data_path, env, 'private_data.json')
            if os.path.exists(path):
                file_env = json.load(open(path, 'r'))
                for gr1 in file_env:
                    for gr2 in file_env[gr1]:
                        if gr1 not in file_shared:
                            file_shared[gr1] = {}
                        file_shared[gr1][gr2] = file_env[gr1][gr2]

            PrivateDataModel.__private_data[env] = file_shared

        return PrivateDataModel.__private_data[env]
