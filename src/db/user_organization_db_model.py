import json

from server_core.utils.utils import Utils
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.user_db_model import UserDbModel


class UserOrganizationDbModel(BaseUserDbModel):
    
    def __init__(self, user_db: UserDbModel):
        super(UserOrganizationDbModel, self).__init__(user_db=user_db)
    
    def organization_id(self) -> int:
        row = self.conn().select_one("""
        SELECT o.id
          FROM app.organization_users ou,
               app.organization o
         WHERE ou.organization_id = o.id
           AND ou.active = 1
           AND ou.user_id = %s
        """, [self.user_id()])

        return row['id']
        
    def organization_exists(self):
        row = self.conn().select_one_or_none("""
        SELECT 1
          FROM app.organization_users ou,
               app.organization o
         WHERE ou.organization_id = o.id
           AND ou.active = 1
           AND ou.user_id = %s
        """, [self.user_id()])

        return row is not None
    
    def get_credits(self) -> int:
        row = self.conn().select_one_or_none("""
        SELECT o.credits
          FROM app.organization_users ou
         INNER JOIN app.organization o ON o.id = ou.organization_id
         WHERE ou.user_id = %s
        """, [self.user_id()])

        if row is None:
            return 0
        return row["credits"]

    def add_credits(self, credits_value: float, context: object) -> str | None:

        if credits_value == 0:
            return None

        credits_before = self.get_credits()
        
        self.conn().query("""
        UPDATE gb_users.organization
           SET credits = credits + %s
         WHERE id = %s
        """, [credits_value, self.organization_id()])

        audit_guid = Utils.get_guid()

        self.__conn.query("""
        INSERT INTO audit.users_credits (user_id, audit_guid, price, credits_before, context)
        VALUES (%s, %s, %s, %s, %s) 
        """, [self.user_id(), audit_guid, credits_value, credits_before, json.dumps(context)])

        return audit_guid
