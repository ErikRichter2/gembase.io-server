from src.db.quota.base_quota_context import BaseQuotaContext
from src.db.user_db_model import UserDbModel


class UserQuotaContext(BaseQuotaContext):
    QUOTA_GOOGLE = "google_search"
    QUOTA_OPENAI = "openai_gpt4_8k"

    # cost $5 per 1000 queries
    PRICE_PER_OPERATION_GOOGLE_SEARCH = 5
    # $0.03/1k prompt tokens
    PRICE_PER_OPERATION_QUOTA_OPENAI_GPT4_8k = 0.03

    def __init__(self, user_db: UserDbModel, quota_type: str, context):
        super(UserQuotaContext, self).__init__({
            "source": "portal",
            "user_id": user_db.user_id(),
            "context": context
        })
        self.__user_db = user_db
        self.__quota_type = quota_type
        
    @staticmethod
    def __get_quota_types() -> list[str]:
        return [UserQuotaContext.QUOTA_GOOGLE, UserQuotaContext.QUOTA_OPENAI]

    def get_pricing(self):
        row = self.__user_db.conn().select_one_or_none("""
            SELECT p.credits
              FROM app.users_pricing p
             WHERE p.user_id = %s
               AND p.operation_type = %s
            """, [self.__user_db.user_id(), self.__quota_type])

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

    def has_enough_credits(self) -> bool:
        pricing = self.get_pricing()

        if pricing["credits"] <= 0:
            return True

        c = self.__user_db.get_credits()
        return c - pricing['credits'] > 0

    def pay_with_quota(self, count: int, audit_guid: str):

        pricing = self.get_pricing()
        final_price = pricing["credits"] / pricing["count"] * count
        
        self.__user_db.add_credits(
            credits_value=-final_price,
            context=self.get_audit_context()
        )
