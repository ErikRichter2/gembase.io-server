import json
from typing import Optional

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.emails_db_model import EmailsDbModel
from src.db.public_db_model import PublicDbModel
from src.models.emails.email_sender import EmailSender
from src.models.emails.user_emails import UserEmails
from src.models.user_obfuscator import UserObfuscator
from src.models.web_search.demo_search import DemoSearch



class UserDbModel(BaseUserDbModel):
    
    USER_ROLE_ADMIN = 1
    USER_ROLE_DEMO = 2
    USER_ROLE_SUPER_ADMIN = 3
    USER_ROLE_SYSTEM_BATCH = 5
    USER_ROLE_DEMO_BATCH = 6

    ADMIN_USER_GUID = "00000000-0000-0000-0000-000000000001"
    DEMO_USER_GUID = "00000000-0000-0000-0000-000000000002"
    SUPER_ADMIN_USER_GUID = "00000000-0000-0000-0000-000000000003"
    SYSTEM_CHAT_USER_GUID = "00000000-0000-0000-0000-000000000004"
    SYSTEM_BATCH_USER_GUID = "00000000-0000-0000-0000-000000000005"
    DEMO_BATCH_USER_GUID = "00000000-0000-0000-0000-000000000006"    
    
    def __init__(self, conn: DbConnection, user_id: int):
        super(UserDbModel, self).__init__(conn=conn, user_id=user_id)
        self.__user_id = user_id

    @property
    def obfuscator(self):
        prime_number = self.conn.select_one("""
            SELECT prime_number
            FROM gb_app.users
            WHERE id = %s
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
    
    def is_demo(self):
        return True

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

    def get_email(self) -> str:
        row = self.conn.select_one("""
        SELECT e.email
          FROM gb_users.users u
         INNER JOIN gb_public.emails e ON e.id = u.email_id
         WHERE u.id = %s
        """, [self.user_id])
        
        return row["email"]

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
        if self.organization_db.organization_exists():
            return self.organization_db.get_credits()
        
        row = self.conn.select_one_or_none("""
        SELECT u.credits
          FROM gb_app.users u
         WHERE u.id = %s
        """, [self.user_id])

        if row is None:
            return 0
        return row["credits"]

    def audit_credits_change(self, credits_value: float, credits_before: int, context: object):
        audit_guid = Utils.get_guid()

        self.__conn.query("""
        INSERT INTO gb_audit.users_credits (user_id, audit_guid, price, credits_before, context)
        VALUES (%s, %s, %s, %s, %s) 
        """, [self.user_id, audit_guid, credits_value, credits_before, json.dumps(context)])

        return audit_guid    
    
    def add_credits(self, credits_value: float, context: object) -> str | None:

        if credits_value == 0:
            return None
        
        if self.organization_db.organization_exists():
            return self.organization_db.add_credits(credits_value=credits_value, context=context)
        
        credits_before = self.__conn.select_one("""
        SELECT credits FROM gb_app.users WHERE id = %s
        """, [self.__user_id])["credits"]
        self.__conn.query("""
        UPDATE gb_app.users
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
        INSERT INTO gb_archive.users_tracking (user_id, action, data) 
        VALUES (%s, %s, %s)
        """, [self.__user_id, action, data])
        
    def demo_batch_user_id(self):
        return self.conn.select_one("""
        SELECT id FROM gb_app.users WHERE guid = %s
        """, [UserDbModel.DEMO_BATCH_USER_GUID])["id"]
