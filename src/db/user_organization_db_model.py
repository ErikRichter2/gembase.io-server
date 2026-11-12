import json
from typing import Optional

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.organizations_db_model import OrganizationsDbModel


class UserOrganizationDbModel(BaseUserDbModel):
    
    def __init__(self, conn: DbConnection, user_id: int):
        super(UserOrganizationDbModel, self).__init__(conn=conn, user_id=user_id)
    
    def organization_id(self) -> int:
        row = self.conn.select_one("""
        SELECT o.id
          FROM gb_app.organization_users ou,
               gb_app.organization o
         WHERE ou.organization_id = o.id
           AND ou.active = 1
           AND ou.user_id = %s
        """, [self.user_id])

        return row['id']
        
    def organization_exists(self):
        row = self.conn.select_one_or_none("""
        SELECT 1
          FROM gb_app.organization_users ou,
               gb_app.organization o
         WHERE ou.organization_id = o.id
           AND ou.active = 1
           AND ou.user_id = %s
        """, [self.user_id])

        return row is not None
    
    def get_credits(self) -> int:
        row = self.conn.select_one_or_none("""
        SELECT o.credits
          FROM gb_app.organization_users ou
         INNER JOIN gb_app.organization o ON o.id = ou.organization_id
         WHERE ou.user_id = %s
        """, [self.user_id])

        if row is None:
            return 0
        return row["credits"]

    def add_credits(self, credits_value: float, context: object) -> str | None:

        if credits_value == 0:
            return None

        credits_before = self.get_credits()
        
        self.conn.query("""
        UPDATE gb_users.organization
           SET credits = credits + %s
         WHERE id = %s
        """, [credits_value, self.organization_id()])

        audit_guid = Utils.get_guid()

        self.__conn.query("""
        INSERT INTO gb_audit.users_credits (user_id, audit_guid, price, credits_before, context)
        VALUES (%s, %s, %s, %s, %s) 
        """, [self.user_id, audit_guid, credits_value, credits_before, json.dumps(context)])

        return audit_guid

    def get_data(self) -> Optional[dict]:
        row = self.conn.select_one_or_none("""
        SELECT o.id, ou.role, o.dev_id_int, o.credits, o.prime_number
          FROM gb_app.organization_users ou,
               gb_app.organization o
         WHERE ou.organization_id = o.id
           AND ou.active = 1
           AND ou.user_id = %s
        """, [self.user_id])

        return row
    
    def create(self, credits_value: int):
        assert not self.organization_exists()

        OrganizationsDbModel(conn=self.conn).create(
            dev_id_int=self.user_db.games_db.get_dev_id_int(),
            prime_number=self.user_db.obfuscator.prime_number,
            users=[{
                "user_id": self.user_id,
                "role": "admin"
            }],
            email_domain=Utils.get_email_domain(self.user_db.get_email()),
            credits_value=credits_value
        )
        
    def is_organization_admin(self) -> bool:
        data = self.get_data()
        if data is not None:
            return data is not None and data["role"] == "admin"
        return False
    
    def get_id(self) -> Optional[int]:
        data = self.get_data()
        if data is not None:
            return data is not None and data["id"]
        return None
