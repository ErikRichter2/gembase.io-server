from typing import Optional

from server_core.utils.flask_utils import FlaskUtils
from server_core.utils.jwt_token import TokenData
from server_core.utils.utils import Utils
from src.db.public_db_model import PublicDbModel
from src.exceptions.auth_exception import AuthException
from src.db.base.base_db_model import BaseDbModel


class UsersDbModel(BaseDbModel):

    def create_user_id(
            self,
            email_id: int,
            created_by: Optional[int] = None
    ) -> int:
        user_id = self.conn.insert("""
        INSERT INTO gb_users.users (email_id, created_by) VALUES (%s, %s)
        """, [email_id, created_by])

        return user_id

    def change_password(self, user_id: int, password_raw: str):
        password_hash, secret = Utils.sha256(password_raw)

        self.conn.query("""
        DELETE FROM gb_users.users_credentials
         WHERE id = %s
        """, [user_id])

        self.conn.query("""
        INSERT INTO gb_users.users_credentials(id, password, secret)
        VALUES (%s, %s, %s)
        """, [user_id, password_hash, secret])

        self.conn.query("""
        DELETE FROM gb_users.users_login_tokens
         WHERE user_id = %s
        """, [user_id])

    def delete(self, user_id: int, deleted_by_user_id: Optional[int] = None):
        self.conn.query("""
        UPDATE gb_users.users u
           SET u.deleted_by = %s,
               u.deleted_t = NOW()
         WHERE u.id = %s
        """, [deleted_by_user_id, user_id])
        
    def create_user_by_credentials(self, email: str, password_raw: str):
        if self.conn.select_one_or_none("""
                                          SELECT *
                                            FROM gb_users.map_email_to_user_id m
                                           INNER JOIN gb_users.users u
                                              ON u.email_id = m.id
                                           WHERE m.email = %s
                                          """, [email]) is not None:
            return
        
        email_id = PublicDbModel(conn=self.conn).get_or_create_email_id(email=email)
        user_id = self.create_user_id(email_id=email_id)
        
        self.change_password(user_id=user_id, password_raw=password_raw)
        
        return user_id
    
       