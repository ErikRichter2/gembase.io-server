import jwt

from src.env_data import ENV_DATA

class TokenData:

    @staticmethod
    def encode(token_guid: str, password: str) -> str:
        return jwt.encode({
            'token_guid': token_guid,
            'password': password
        }, ENV_DATA.flask_secret_key, "HS256")

    @staticmethod
    def decode(token: str) -> dict:
        try:
            parsed_token = jwt.decode(token, ENV_DATA.flask_secret_key, algorithms=["HS256"])
        except Exception:
            raise Exception("Invalid token")

        if 'token_guid' not in parsed_token:
            raise Exception("Invalid token")
        if 'password' not in parsed_token:
            raise Exception("Invalid token")

        return parsed_token
