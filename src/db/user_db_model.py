import json
from typing import Optional, TypeVar

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.emails_db_model import EmailsDbModel
from src.db.public_db_model import PublicDbModel
from src.db.user_billing_db_model import UserBillingDbModel
from src.db.user_games_db_model import UserGamesDbModel
from src.db.user_organization_db_model import UserOrganizationDbModel
from src.db.user_platform_db_model import UserPlatformDbModel
from src.db.user_portal_db_model import UserPortalDbModel
from src.db.user_tutorial_db_model import UserTutorialDbModel
from src.models.emails.email_sender import EmailSender
from src.models.emails.user_emails import UserEmails
from src.models.user_obfuscator import UserObfuscator
from src.models.web_search.demo_search import DemoSearch

T = TypeVar('T', bound=BaseUserDbModel)

class UserDbModel(BaseDbModel):
    
    def __init__(self, conn: DbConnection, user_id: int):
        super(UserDbModel, self).__init__(conn=conn)
        self.__user_id = user_id
        self.__db_models = {}
        
    def __get_or_create_db_model(self, model: type[T], **kwargs) -> T:
        if model not in self.__db_models:
            self.__db_models[model] = model(user_db=self, **kwargs)
        return self.__db_models[model]        

    @property
    def user_id(self):
        return self.__user_id
    
    @property
    def user_portal_db(self):
        return self.__get_or_create_db_model(UserPortalDbModel)
        
    @property
    def user_billing_db(self):
        return self.__get_or_create_db_model(UserBillingDbModel)
    
    @property
    def user_platform_db(self):
        return self.__get_or_create_db_model(UserPlatformDbModel)
    
    @property
    def user_tutorial_db(self):
        return self.__get_or_create_db_model(UserTutorialDbModel)
    
    @property
    def user_organization_db(self):
        return self.__get_or_create_db_model(UserOrganizationDbModel)

    @property
    def user_games_db(self):
        return self.__get_or_create_db_model(UserGamesDbModel)
    
    @property
    def obfuscator(self):
        prime_number = self.conn.select_one("""
            SELECT prime_number
            FROM gb_users.users
            WHERE user_id = %s
            """, [self.user_id])['prime_number']
        
        return UserObfuscator(multiplier=prime_number)
        
    def user_exists(self):
        row = self.conn.select_one_or_none("""
        SELECT 1
          FROM gb_users.users
         WHERE id = %s
        """, [self.user_id])

        return row is not None
        
    def get_role(self):
        row = self.conn.select_one_or_none("""
        SELECT role
          FROM gb_users.users_roles ur
         WHERE ur.id = %s
        """, [self.user_id])

        return row['role'] if row is not None else None

    def is_admin(self):
        row = self.conn.select_one_or_none("""
        SELECT 1
          FROM gb_users.users_roles ur
         WHERE ur.id = %s
           AND ur.role = 'admin'
        """, [self.user_id])

        return row is not None

    def is_system(self):
        row = self.conn.select_one_or_none("""
        SELECT 1
          FROM gb_users.users_roles ur
         WHERE ur.id = %s
           AND ur.role = 'system'
        """, [self.user_id])

        return row is not None
      
    def send_email(
      self,
      subject: str,
      body: str
    ):
        
      email = self.get_email()
      assert email is not None
            
      email_data = EmailSender().send(
        recipient=email,
        subject=subject,
        body=body
      )
      
      if email_data is not None:
        self.conn.query("""
                        INSERT INTO gb_users.sent_emails (user_id, email_data)
                        VALUES (%s, %s)
                        """, [self.user_id, json.dumps(email_data)])

    def get_email(self) -> Optional[str]:
        row = self.conn.select_one_or_none("""
        SELECT e.email
          FROM gb_users.users u
         INNER JOIN gb_public.emails e ON e.id = u.email_id
         WHERE u.id = %s
        """, [self.user_id])
        
        return row["email"] if row is not None else None

    def get_email_id(self) -> int:
        row = self.conn.select_one_or_none("""
        SELECT email_id
          FROM gb_users.users
         WHERE u.id = %s
        """, [self.user_id])
        assert row is not None
        return row["email_id"]

    def get_first_name(self):
        row = self.conn.select_one_or_none("""
        SELECT first_name
          FROM gb_users.users
         WHERE id = %s
        """, [self.user_id])
        return row['first_name'] if row is not None else None

    def request_password_change(self):
        
        assert self.user_exists()
        
        email_id = self.get_email_id()
        
        email = self.get_email()
        assert email is not None
        
        first_name = self.get_first_name()
        assert first_name is not None
        
        self.conn.query("""
        DELETE 
          FROM gb_users.users_password_reset_pending u
         WHERE u.id = %s
        """, [self.user_id])

        guid = Utils.get_guid()
        self.__conn.query("""
        INSERT INTO gb_users.users_password_reset_pending (id, guid, email_id)
        VALUES (%s, %s, %s)
        """, [self.user_id, guid, email])

        subscribe_guid = PublicDbModel(conn=self.conn).get_or_create_unsubscribe_guid(email_id=email_id)

        email_data = UserEmails.password_change(
            subscribe_guid=subscribe_guid,
            to_address=email,
            guid=guid,
            user_name=first_name
        )
        EmailsDbModel(conn=self.conn).archive_email(email_data) 
        

    def get_credits(self) -> int:
        if self.user_organization_db.organization_exists():
            return self.user_organization_db.get_credits()
        
        row = self.conn.select_one_or_none("""
        SELECT u.credits
          FROM app.users u
         WHERE u.id = %s
        """, [self.user_id])

        if row is None:
            return 0
        return row["credits"]

    def audit_credits_change(self, credits_value: float, credits_before: int, context: object):
        audit_guid = Utils.get_guid()

        self.__conn.query("""
        INSERT INTO audit.users_credits (user_id, audit_guid, price, credits_before, context)
        VALUES (%s, %s, %s, %s, %s) 
        """, [self.user_id, audit_guid, credits_value, credits_before, json.dumps(context)])

        return audit_guid    
    
    def add_credits(self, credits_value: float, context: object) -> str | None:

        if credits_value == 0:
            return None
        
        if self.user_organization_db.organization_exists():
            return self.user_organization_db.add_credits(credits_value=credits_value, context=context)
        
        credits_before = self.__conn.select_one("""
        SELECT credits FROM app.users WHERE id = %s
        """, [self.__user_id])["credits"]
        self.__conn.query("""
        UPDATE app.users
            SET credits = credits + %s
            WHERE id = %s
        """, [credits_value, self.__user_id])
        
        return self.audit_credits_change(credits_value=credits_value, credits_before=credits_before, context=context)

    def update(self, first_name: str, last_name: str, position_area: int, position_role: int):
        self.conn.query("""
                          UPDATE gb_users.users
                             SET first_name = %s, last_name = %s, position_area = %s, position_role = %s
                           WHERE id = %s
                          """, [self.user_id])
        
    def get_search_model(self):
        return DemoSearch()

    def track_action(self, action: str, data: str):
        self.conn.query("""
        INSERT INTO archive.users_tracking (user_id, action, data) 
        VALUES (%s, %s, %s)
        """, [self.__user_id, action, data])
        
   