from typing import Optional

from server_core.utils.flask_utils import FlaskUtils
from server_core.utils.jwt_token import TokenData
from server_core.utils.utils import Utils
from src.db.public_db_model import PublicDbModel
from src.db.user_db_model import UserDbModel
from src.exceptions.auth_exception import AuthException
from src.db.base.base_db_model import BaseDbModel


class UsersDbModel(BaseDbModel):

    def create_user_id(
            self,
            email_id: int,
            created_by: Optional[int] = None
    ) -> int:
        user_id = self.conn().insert("""
        INSERT INTO gb_users.users (email_id, created_by) VALUES (%s, %s)
        """, [email_id, created_by])

        return user_id

    def get_user_id_from_email(
            self,
            email: str
    ) -> int:
        email_id = PublicDbModel(conn=self.conn()).get_email_id(email=email)
        row = self.conn().select_one_or_none("""
        SELECT u.id 
          FROM gb_users.users u
         WHERE u.email_id = %s
        """, [email_id])
        if row is not None:
            return row["id"]
        return 0

    def get_user_id_by_credentials(
            self,
            email: str,
            password_raw: str
    ) -> int:

        user_id = self.get_user_id_from_email(email=email)

        if user_id == 0:
            raise AuthException(AuthException.INVALID_USERNAME_OR_PASSWORD)

        row = self.conn().select_one_or_none("""
        SELECT password, 
               secret 
          FROM gb_users.users_credentials 
         WHERE id = %s 
        """, [user_id])

        if row is None or not Utils.compare_hash(
                value=password_raw,
                secret=row["secret"],
                compare_to_hash=row["password"]
        ):
            raise AuthException(AuthException.INVALID_USERNAME_OR_PASSWORD)

        return user_id

    def get_user_id_from_auth_token(
            self,
            auth_token: Optional[str]
    ) -> Optional[int]:
        if auth_token is None:
            return None

        try:
            parsed_token = TokenData.decode(auth_token)
        except Exception:
            return None

        row = self.conn().select_one_or_none("""
            SELECT t.user_id, 
                   u.password, 
                   u.secret
              FROM gb_users.users_login_tokens t,
                   gb_users.users_credentials u
             WHERE t.token = %s
               AND t.user_id = u.id
               AND t.created_t >= DATE_SUB(NOW(), INTERVAL t.expire_days DAY)
        """, [parsed_token['token_guid']])

        if row is None:
            return None

        if not Utils.compare_hash(parsed_token['password'], row["secret"], row["password"]):
            return None

        return row["user_id"]

    def get_user_id_from_api_key(self, api_key: str):
        row = self.conn().select_one_or_none("""
        SELECT user_id
          FROM gb_users.users_api_keys
         WHERE api_key = %s
        """, [api_key])
                
        if row is None:
            return None

        return row["user_id"]
    
    def logout(self, user_id: int):
        self.conn().query("""
        DELETE FROM gb_users.users_login_tokens
         WHERE user_id = %s
        """, [user_id])

    def login(
            self,
            email: str,
            password_raw: str
    ):
        user_id = self.get_user_id_by_credentials(
            email=email,
            password_raw=password_raw,
        )

        expire_days = 14
        token_guid = Utils.get_guid()

        self.conn().query("""
                INSERT INTO gb_users.users_login_tokens (token, user_id, ip_address, expire_days) 
                VALUES (%s, %s, %s, %s)
                """, [token_guid, user_id, FlaskUtils.get_ip_address(), expire_days])

        auth_token = TokenData.encode(token_guid, password_raw)
        
        user_role = self.conn().select_one("""
                                      SELECT role
                                        FROM gb_users.users_roles
                                       WHERE id = %s
                                      """, [user_id])['role']

        return {
            "auth_token": auth_token,
            "user_id": user_id,
            "role": user_role
        }

    def change_password(self, user_id: int, password_raw: str):
        password_hash, secret = Utils.sha256(password_raw)

        self.conn().query("""
        DELETE FROM gb_users.users_credentials
         WHERE id = %s
        """, [user_id])

        self.conn().query("""
        INSERT INTO gb_users.users_credentials(id, password, secret)
        VALUES (%s, %s, %s)
        """, [user_id, password_hash, secret])

        self.conn().query("""
        DELETE FROM gb_users.users_login_tokens
         WHERE user_id = %s
        """, [user_id])

    def delete(self, user_id: int, deleted_by_user_id: Optional[int] = None):
        self.conn().query("""
        UPDATE gb_users.users u
           SET u.deleted_by = %s,
               u.deleted_t = NOW()
         WHERE u.id = %s
        """, [deleted_by_user_id, user_id])
        
    def create_user_by_credentials(self, email: str, password_raw: str):
        if self.conn().select_one_or_none("""
                                          SELECT *
                                            FROM gb_users.map_email_to_user_id m
                                           INNER JOIN gb_users.users u
                                              ON u.email_id = m.id
                                           WHERE m.email = %s
                                          """, [email]) is not None:
            return
        
        email_id = PublicDbModel(conn=self.conn()).get_or_create_email_id(email=email)
        user_id = self.create_user_id(email_id=email_id)
        
        self.change_password(user_id=user_id, password_raw=password_raw)
        
        return user_id
    
       