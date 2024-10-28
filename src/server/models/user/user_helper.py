from gembase_server_core.db.db_connection import DbConnection
from src.server.models.billing.billing_utils import BillingUtils


class UserHelper:

    @staticmethod
    def is_app_locked(conn: DbConnection, user_id: int, app_id_int: int) -> bool:
        unlocked = UserHelper.get_unlocked_apps(
            conn=conn,
            user_id=user_id
        )
        return unlocked is not None and app_id_int not in unlocked

    @staticmethod
    def get_unlocked_apps(conn: DbConnection, user_id: int) -> list | None:

        return None

        if BillingUtils.BILLING_MODULE_AUDITOR in BillingUtils.get_unlocked_modules(
                conn=conn,
                user_id=user_id
        ):
            return None

        rows_installs = conn.select_all("""
            SELECT ua.app_id_int
              FROM app.users_devs ud,
                   app.users_apps ua,
                   scraped_data.devs_apps da,
                   scraped_data.apps_valid a
             WHERE ua.user_id = %s
               AND ud.user_id = ua.user_id
               AND ua.app_id_int = a.app_id_int
               AND da.primary_dev = 1
               AND da.dev_id_int = ud.dev_id_int
               AND da.app_id_int = a.app_id_int
             ORDER BY a.installs DESC
             LIMIT 1
            """, [user_id])

        rows_unlocked = conn.select_all("""
            SELECT ua.app_id_int
              FROM app.users_apps ua
             WHERE ua.user_id = %s
               AND ua.unlocked_in_demo = 1
            """, [user_id])

        res = [BillingUtils.UNLOCKED_DEFAULT_APP_ID_INT]

        for row in rows_installs:
            if row["app_id_int"] not in res:
                res.append(row["app_id_int"])
        for row in rows_unlocked:
            if row["app_id_int"] not in res:
                res.append(row["app_id_int"])

        return res
