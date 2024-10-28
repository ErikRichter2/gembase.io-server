import os
from dotenv import load_dotenv


class RuntimeConstants:

    ENV_DEV = 'dev'
    ENV_TEST = 'test'
    ENV_PROD = 'prod'

    ALL_ENVS = [ENV_DEV, ENV_TEST, ENV_PROD]

    FLASK_SECRET_KEY: str = ''
    ENV: str = ENV_DEV
    IS_REMOTE = False

    IS_DEBUG: bool = False

    @staticmethod
    def init_dotenv():
        load_dotenv()
        RuntimeConstants.ENV = os.getenv('APP_ENV')

    @staticmethod
    def set_env(env: str, remote: bool = False):
        RuntimeConstants.ENV = env
        RuntimeConstants.IS_REMOTE = remote

    @staticmethod
    def is_prod() -> bool:
        return RuntimeConstants.ENV == RuntimeConstants.ENV_PROD

    @staticmethod
    def is_test() -> bool:
        return RuntimeConstants.ENV == RuntimeConstants.ENV_TEST

    @staticmethod
    def is_dev() -> bool:
        return RuntimeConstants.ENV == RuntimeConstants.ENV_DEV

    @staticmethod
    def is_debug() -> bool:
        return RuntimeConstants.IS_DEBUG

    @staticmethod
    def redirect_emails() -> bool:
        return False


rr = RuntimeConstants
rr.init_dotenv()
