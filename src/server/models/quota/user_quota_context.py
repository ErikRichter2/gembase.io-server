import json

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.quota.base_quota_context import BaseQuotaContext
from src.server.models.user.user_data import UserData
from src.server.models.user.user_model import UserModel


class UserQuotaContext(BaseQuotaContext):
    QUOTA_GOOGLE = "google_search"
    QUOTA_OPENAI = "openai_gpt4_8k"

    # cost $5 per 1000 queries
    PRICE_PER_OPERATION_GOOGLE_SEARCH = 5
    # $0.03/1k prompt tokens
    PRICE_PER_OPERATION_QUOTA_OPENAI_GPT4_8k = 0.03

    def __init__(self, user_id: int, quota_type: str, context):
        super(UserQuotaContext, self).__init__({
            "source": "portal",
            "user_id": user_id,
            "context": context
        })
        self.__quota_type = quota_type
        self.__user_id = user_id

    def get_user_id(self) -> int:
        return self.__user_id

    @staticmethod
    def __get_quota_types() -> []:
        return [UserQuotaContext.QUOTA_GOOGLE, UserQuotaContext.QUOTA_OPENAI]

    def get_pricing(self):
        conn = DbConnection()
        row = conn.select_one_or_none("""
            SELECT p.credits
              FROM app.users_pricing p
             WHERE p.user_id = %s
               AND p.operation_type = %s
            """, [self.__user_id, self.__quota_type])
        conn.close()

        operation_credits = None
        if row is not None:
            operation_credits = row["credits"]

        if self.__quota_type == UserQuotaContext.QUOTA_GOOGLE:
            if operation_credits is None:
                operation_credits = UserQuotaContext.PRICE_PER_OPERATION_GOOGLE_SEARCH
            return {
                "count": 1000,
                "credits": operation_credits
            }
        elif self.__quota_type == UserQuotaContext.QUOTA_OPENAI:
            if operation_credits is None:
                operation_credits = UserQuotaContext.PRICE_PER_OPERATION_QUOTA_OPENAI_GPT4_8k
            return {
                "count": 1000,
                "credits": operation_credits
            }
        else:
            raise Exception(f"Unknown quota type {self.__quota_type}")

    def has(self) -> bool:
        pricing = self.get_pricing()

        if pricing["credits"] <= 0:
            return True

        conn = DbConnection()
        c = UserData.get_credits(
            conn=conn,
            user_id=self.__user_id
        )
        conn.close()

        return c > 0

    @staticmethod
    def add_credits(conn: DbConnection, to_user_id: int, from_user_id: int, credits: int, context):
        if credits <= 0:
            return

        audit_guid = UserModel(
            conn=conn,
            user_id=to_user_id
        ).add_credits(
            credits_value=credits,
            context=context
        )

        audit_context = {
            "credits": credits,
            "from_user_id": from_user_id,
            "to_user_id": from_user_id
        }

        conn.query("""
            INSERT INTO audit.portal_admin (audit_guid, user_id, op, context)
            VALUES (%s, %s, %s, %s) 
            """, [audit_guid, from_user_id, "add_credits", json.dumps(audit_context)])

        #setattr(flask.g, "add_credits_to_response", True)

    def add(self, count: int, audit_guid: str):

        pricing = self.get_pricing()
        final_price = pricing["credits"] / pricing["count"] * count

        conn = DbConnection()
        UserModel(
            conn=conn,
            user_id=self.__user_id
        ).add_credits(
            credits_value=-final_price,
            context=self.get_audit_context()
        )
        conn.commit()
        conn.close()
