from dataclasses import dataclass
import os
from typing import Final, Literal, Optional, TypeGuard, get_args

from dataclasses_json import dataclass_json
from dotenv import load_dotenv, dotenv_values, find_dotenv


APP_ENV_DEV: Final = 'dev'
APP_ENV_TEST: Final = 'test'
APP_ENV_PROD: Final = 'prod'
APP_ENV_ADMIN: Final = 'admin'

AppEnv = Literal['dev', 'test', 'prod']


def is_app_env(value: str | None) -> TypeGuard[AppEnv]:
    return value in get_args(AppEnv)


@dataclass_json
@dataclass
class EnvData:
    app_env: AppEnv = APP_ENV_DEV
    flask_secret_key: Optional[str] = None
    server_url: str = ''
    client_url: str = ''
    email_sender_address: Optional[str] = None
    email_sender_password: Optional[str] = None
    db_user: Optional[str] = None
    db_pass: Optional[str] = None
    db_host: Optional[str] = None
    db_port: Optional[int] = None
    google_search_credentials: Optional[str] = None

    
    @staticmethod
    def __create():
        db_port: str | None = os.getenv('DB_PORT')
        
        server_url = os.getenv('SERVER_URL')
        assert server_url is not None
        
        client_url = os.getenv('CLIENT_URL')
        assert client_url is not None
        
        app_env = os.getenv('APP_ENV')
        if not is_app_env(app_env):
            raise ValueError(f"Invalid APP_ENV: {app_env!r}, expected one of {get_args(AppEnv)}")
               
        res = EnvData(
            app_env=app_env,
            flask_secret_key=os.getenv('FLASK_SECRET_KEY'),
            server_url=server_url,
            client_url=client_url,
            email_sender_address=os.getenv('EMAIL_SENDER_ADDRESS'),
            email_sender_password=os.getenv('EMAIL_SENDER_PASSWORD'),
            db_user = os.getenv('DB_USER'),
            db_pass = os.getenv('DB_PASS'),
            db_host = os.getenv('DB_HOST'),
            db_port = int(db_port) if db_port is not None else None,
            google_search_credentials=os.getenv('GOOGLE_SEARCH_CREDENTIALS'),
        )
        
        return res
    
    def override(self, env: Literal['dev', 'test', 'prod', 'admin']) -> 'EnvData':
        env_file = find_dotenv(f'.env_{env}')
        load_dotenv(env_file, override=True)
        return EnvData.__create()

    @staticmethod
    def load() -> 'EnvData':
        load_dotenv(override=True)
        return EnvData.__create()
        
            
ENV_DATA: EnvData = EnvData.load()
