from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.user_db_model import UserDbModel


class UserBillingDbModel(BaseUserDbModel):

    DISCOUNT_TESTIMONIAL = 1
    DISCOUNT_MULTI = 2
    DISCOUNT_MULTI_SEATS = 3

    BILLING_MODULE_AUDITOR = 1
    BILLING_MODULE_INSIGHT = 2
    BILLING_MODULE_IDEAS = 3

    ALL_BILLING_MODULES = [
        BILLING_MODULE_AUDITOR, BILLING_MODULE_INSIGHT, BILLING_MODULE_IDEAS
    ]
            
    def __init__(self, user_db: UserDbModel):
        super(UserBillingDbModel, self).__init__(user_db=user_db)
    
    def get_unlocked_modules(self):

        row = self.conn.select_one_or_none("""
                SELECT u.email, UNIX_TIMESTAMP(u.free_trial_end_t) as free_trial_end_t FROM app.users u WHERE u.id = %s
                """, [self.user_id])

        if row is not None:
            if row["free_trial_end_t"] is not None and row["free_trial_end_t"] > Utils.timestamp_int():
                return UserBillingDbModel.ALL_BILLING_MODULES.copy()
            if row["email"] == "alexis@stillfront.com":
                return UserBillingDbModel.ALL_BILLING_MODULES.copy()

        rows = self.conn.select_all("""
        SELECT oum.module_id
          FROM app.organization_users_modules oum,
               app.organization_users ou,
               billing.billings b
         WHERE oum.user_id = %s
           AND oum.user_id = ou.user_id
           AND oum.organization_id = ou.organization_id
           AND oum.billing_id = b.id
           AND ou.active = TRUE
           AND b.expire_t > NOW()
         GROUP BY oum.module_id
        """, [self.user_id])

        res = []
        for row in rows:
            res.append(row["module_id"])

        return res      

    def is_module_locked(self, module_id: int):
        return module_id not in self.get_unlocked_modules()
    
    