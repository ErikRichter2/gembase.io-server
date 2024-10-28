import stripe

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.environment.runtime_constants import rr
from gembase_server_core.private_data.private_data_model import PrivateDataModel
from src.utils.gembase_utils import GembaseUtils


class BillingUtils:

    UNLOCKED_DEFAULT_APP_ID_INT = 146  # clash of clans

    DISCOUNT_TESTIMONIAL = 1
    DISCOUNT_MULTI = 2
    DISCOUNT_MULTI_SEATS = 3

    BILLING_MODULE_AUDITOR = 1
    BILLING_MODULE_INSIGHT = 2
    BILLING_MODULE_IDEAS = 3

    ALL_BILLING_MODULES = [
        BILLING_MODULE_AUDITOR, BILLING_MODULE_INSIGHT, BILLING_MODULE_IDEAS
    ]

    @staticmethod
    def init_stripe_api(
            test_payment_val=0
    ) -> str:
        stripe_api_key = PrivateDataModel.get_private_data()["stripe"]["api_key"]
        tax_rate = PrivateDataModel.get_private_data()["stripe"]["tax_rate"]

        if test_payment_val == 2:
            stripe_api_key = PrivateDataModel.get_private_data(rr.ENV_PROD)["stripe"]["api_key"]
            tax_rate = PrivateDataModel.get_private_data(rr.ENV_PROD)["stripe"]["tax_rate"]
        elif test_payment_val == 1:
            stripe_api_key = PrivateDataModel.get_private_data()["stripe"]["api_key_test"]
            tax_rate = PrivateDataModel.get_private_data()["stripe"]["tax_rate_test"]

        stripe.api_key = stripe_api_key

        return tax_rate

    @staticmethod
    def get_allowed_tags_for_locked_module(conn: DbConnection, module_id: int):
        rows = conn.select_all("""
        SELECT d.tag_id_int
          FROM app.def_allowed_tags_per_locked_module d
         WHERE d.module_id = %s
        """, [module_id])
        return [row["tag_id_int"] for row in rows]

    @staticmethod
    def get_changeable_tags_for_locked_module(conn: DbConnection, module_id: int):
        rows = conn.select_all("""
            SELECT d.tag_id_int
              FROM app.def_allowed_tags_per_locked_module d
             WHERE d.module_id = %s
               AND d.is_changeable = 1
            """, [module_id])
        return [row["tag_id_int"] for row in rows]

    @staticmethod
    def is_auditor_locked(conn: DbConnection, user_id: int):
        return BillingUtils.is_module_locked(conn=conn, user_id=user_id, module_id=BillingUtils.BILLING_MODULE_AUDITOR)

    @staticmethod
    def is_module_locked(conn: DbConnection, user_id: int, module_id: int):
        return module_id not in BillingUtils.get_unlocked_modules(
            conn=conn,
            user_id=user_id
        )

    @staticmethod
    def get_unlocked_modules(conn: DbConnection, user_id: int, ignore_free_trial=False):

        if not ignore_free_trial:
            row = conn.select_one_or_none("""
                    SELECT u.email, UNIX_TIMESTAMP(u.free_trial_end_t) as free_trial_end_t FROM app.users u WHERE u.id = %s
                    """, [user_id])

            if row is not None:
                if row["free_trial_end_t"] is not None and row["free_trial_end_t"] > GembaseUtils.timestamp_int():
                    return BillingUtils.ALL_BILLING_MODULES.copy()

        rows = conn.select_all("""
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
        """, [user_id])

        res = []
        for row in rows:
            res.append(row["module_id"])

        return res
