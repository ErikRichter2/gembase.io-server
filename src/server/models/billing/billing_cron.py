import stripe

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.user.user_model import UserModel
from src.session.session_factory import GbSessionFactory


class BillingCron:

    @staticmethod
    def process(
            conn: DbConnection
    ):
        rows = conn.select_all("""
                SELECT guid, stripe_id, user_id
                  FROM billing.requests
                 WHERE stripe_state = 'request'
                   AND stripe_id IS NOT NULL
                """)

        for row in rows:
            session = GbSessionFactory.create_session_local(
                conn=conn,
                user=UserModel(
                    conn=conn,
                    user_id=row["user_id"]
                )
            )
            session.models().billing().payment_confirm(
                request_guid=row["guid"]
            )
            conn.commit()

        rows = conn.select_all("""
        SELECT b.id, r.test_payment, r.user_id, r.stripe_subscription_id
          FROM billing.billings b,
               billing.requests r
         WHERE r.id = b.payment_id
           AND b.expire_t <= NOW()
           AND r.stripe_subscription_id IS NOT NULL
           AND DATE_ADD(b.expire_t, INTERVAL 40 DAY) >= NOW()
        """)

        for row in rows:
            BillingUtils.init_stripe_api(
                test_payment_val=row["test_payment"]
            )
            subscription = stripe.Subscription.retrieve(row["stripe_subscription_id"])
            if subscription.status == "active":
                conn.query("""
                        UPDATE billing.billings
                        SET expire_t = FROM_UNIXTIME(%s)
                        WHERE id = %s
                        """, [subscription.current_period_end, row["id"]])
                conn.commit()
