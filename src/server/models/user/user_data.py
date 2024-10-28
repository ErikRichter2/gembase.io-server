from flask import request

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.user.user_constants import uc
from src.utils.gembase_utils import GembaseUtils
from src.utils.hash_utils import sha256


class UserData:

    @staticmethod
    def create_user(
            conn: DbConnection, password_raw: str, name: str,
            email: str, role: int, position_role=0, position_area=0,
            initial_credits=0, sent_request_t=None, free_trial_end_t=None,
            guid: str | None = None
    ) -> int:
        password_hash, secret = sha256(password_raw)

        if guid is None:
            guid = GembaseUtils.get_guid()

        prime_number = conn.select_one("""
            SELECT p.id FROM app.def_prime_numbers p ORDER BY RAND() LIMIT 1
            """)["id"]

        email_id = UserData.get_or_create_email_id(conn=conn, email=email)

        user_id = conn.insert("""
            INSERT INTO app.users(guid, password, secret, name, email, role, prime_number, 
            position_role, position_area, credits, sent_request_t, free_trial_end_t)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s))
            """, [guid, password_hash, secret, name, email, role, prime_number,
                  position_role, position_area, initial_credits, sent_request_t, free_trial_end_t])

        return user_id

    @staticmethod
    def get_name(conn: DbConnection, user_id: int) -> str:
        return conn.select_one("""
        SELECT name FROM app.users WHERE id = %s
        """, [user_id])["name"]

    @staticmethod
    def get_or_create_email_id(conn: DbConnection, email: str):
        row = conn.select_one_or_none("""
                            SELECT id FROM app.map_user_email WHERE email = %s FOR UPDATE
                            """, [email])
        if row is None:
            email_id = conn.insert("""
                                INSERT INTO app.map_user_email (email) VALUES (%s)
                                """, [email])
        else:
            email_id = row["id"]

        return email_id

    @staticmethod
    def get_user_id_from_email(conn: DbConnection, email: str) -> int:
        row = conn.select_one_or_none("""
                        SELECT id 
                          FROM app.users 
                         WHERE email = %s
                        """, [email])
        if row is not None:
            return row["id"]
        return 0

    @staticmethod
    def delete_user(conn: DbConnection, user_id: int, only_if_temp=True):
        is_temp = conn.select_one_or_none("""
            SELECT temp FROM app.users WHERE id = %s
            """, [user_id])["temp"]

        if only_if_temp and not is_temp:
            return

        conn.query("DELETE FROM app.users u WHERE u.id = %s", [user_id])
        conn.query("DELETE FROM app.organization_users ou WHERE ou.user_id = %s", [user_id])

    @staticmethod
    def get_email_from_id(conn: DbConnection, email_id: int) -> str | None:
        row = conn.select_one_or_none("""
                    SELECT email FROM app.map_user_email WHERE id = %s
                    """, [email_id])
        return row["email"] if row is not None else None

    @staticmethod
    def demo_batch_user_id(conn: DbConnection):
        return conn.select_one("""
        SELECT id FROM app.users WHERE guid = %s
        """, [uc.DEMO_BATCH_USER_GUID])["id"]

    @staticmethod
    def get_credits(conn: DbConnection, user_id: int):
        row = conn.select_one_or_none("""
        SELECT IF(o.id IS NULL, u.credits, o.credits) as credits
          FROM app.users u
          LEFT JOIN app.organization_users ou ON ou.user_id = u.id
          LEFT JOIN app.organization o ON o.id = ou.organization_id
         WHERE u.id = %s
        """, [user_id])

        if row is None:
            return 0
        return row["credits"]

    @staticmethod
    def get_email_id_from_email(conn: DbConnection, email: str) -> int | None:
        row = conn.select_one_or_none("""
                                SELECT id FROM app.map_user_email WHERE email = %s
                                """, [email])
        return row["id"] if row is not None else None

    @staticmethod
    def get_or_create_registration_whitelist_pending(conn: DbConnection, email: str):
        row = conn.select_one_or_none("""
            SELECT r.request_guid
              FROM app.registration_whitelist_pending r
             WHERE r.email = %s
            """, [email])

        if row is not None:
            return row["request_guid"]

        request_guid = GembaseUtils.get_guid()
        conn.query("""
            INSERT INTO app.registration_whitelist_pending (email, ip_address, request_guid)
            VALUES (%s, %s, %s)
            """, [email, request.remote_addr, request_guid])

        return request_guid

    @staticmethod
    def get_or_create_unsubscribe_guid(conn: DbConnection, email_id: int | None = None, email: str | None = None):
        if email_id is None:
            email_id = UserData.get_email_id_from_email(conn=conn, email=email)

        if email_id is not None:
            row = conn.select_one_or_none("""
                        SELECT request_guid FROM app.users_email_subscription WHERE email_id = %s
                        """, [email_id])
            if row is not None:
                return row["request_guid"]
            guid = GembaseUtils.get_guid()
            conn.query("""
                        INSERT INTO app.users_email_subscription (email_id, request_guid, subscribed)
                        VALUES (%s, %s, 1)
                        """, [email_id, guid])
            return guid
        else:
            return UserData.get_or_create_registration_whitelist_pending(
                conn=conn,
                email=email
            )
