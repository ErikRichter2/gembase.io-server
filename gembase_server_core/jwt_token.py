import jwt

from gembase_server_core.environment.runtime_constants import rr


class TokenData:

    @staticmethod
    def encode(token_guid: str, password: str) -> str:
        return jwt.encode({
            'token_guid': token_guid,
            'password': password
        }, rr.FLASK_SECRET_KEY, "HS256")

    @staticmethod
    def decode(token: str) -> dict:
        try:
            parsed_token = jwt.decode(token, rr.FLASK_SECRET_KEY, algorithms=["HS256"])
        except Exception:
            raise Exception("Invalid token")

        if 'token_guid' not in parsed_token:
            raise Exception("Invalid token")
        if 'password' not in parsed_token:
            raise Exception("Invalid token")

        return parsed_token
