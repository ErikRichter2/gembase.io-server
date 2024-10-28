from flask import request

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.emails.emails_helper import EmailsHelper
from src.server.models.user.user_emails import UserEmails
from src.server.models.user.user_data import UserData
from src.utils.gembase_utils import GembaseUtils
from src.server.models.user.user_model import UserModel


class UserRegistrationHelper:

    @staticmethod
    def __send_not_whitelisted_email(
            conn: DbConnection,
            email: str,
            request_guid: str,
            from_user: UserModel | None = None
    ):
        subscribe_guid = UserData.get_or_create_unsubscribe_guid(
            conn=conn,
            email=email
        )

        if from_user is not None:
            res = UserEmails.not_whitelisted_email_from_user(
                subscribe_guid=subscribe_guid,
                request_guid=request_guid,
                from_user_email=from_user.get_email(),
                from_user_name=from_user.get_name(),
                to_user_email=email
            )
        else:
            res = UserEmails.not_whitelisted_email(
                subscribe_guid=subscribe_guid,
                request_guid=request_guid,
                to_user_email=email
            )

        EmailsHelper.archive_email(conn, res)

    @staticmethod
    def __send_already_registered_email(
            conn: DbConnection,
            email: str
    ):
        subscribe_guid = UserData.get_or_create_unsubscribe_guid(conn=conn, email=email)
        res = UserEmails.already_registered_email(
            subscribe_guid=subscribe_guid,
            to_email=email
        )

        EmailsHelper.archive_email(conn, res)

    @staticmethod
    def __send_invite_mail_for_user_without_organization(
        conn: DbConnection,
        locked: bool,
        request_guid: str,
        user_email: str,
        user_name: str | None = None
    ):
        subscribe_guid = UserData.get_or_create_unsubscribe_guid(conn=conn, email=user_email)
        res = UserEmails.invite_mail_for_user_without_organization(
            subscribe_guid=subscribe_guid,
            request_guid=request_guid,
            user_name=user_name,
            to_email=user_email,
            locked=False
        )

        EmailsHelper.archive_email(conn, res)

        conn.query("""
        UPDATE app.users_registration_requests r
           SET r.sent_request_t = NOW()
         WHERE r.guid = %s
        """, [request_guid])

    @staticmethod
    def get_whitelist_request_guid(conn: DbConnection, email: str) -> str | None:
        assert GembaseUtils.is_email(email)
        email_domain = GembaseUtils.get_email_domain(email)

        row = conn.select_one_or_none("""
                SELECT o.request_guid
                  FROM app.organization_requests o,
                       app.organization_domains od
                 WHERE o.email = %s
                   AND o.organization_id = od.organization_id
                   AND od.domain = %s
                """, [email, email_domain])

        if row is not None:
            return row["request_guid"]

        row = conn.select_one_or_none("""
                SELECT r.guid
                  FROM app.users_registration_requests r
                 WHERE r.email = %s
                 AND r.blocked IS NULL
                """, [email])

        if row is not None:
            return row["guid"]

        return None

    @staticmethod
    def get_invite_request_guid(conn: DbConnection, email: str) -> str:
        request_guid = UserRegistrationHelper.get_whitelist_request_guid(
            conn=conn,
            email=email
        )

        if request_guid is not None:
            return request_guid

        return UserData.get_or_create_registration_whitelist_pending(
            conn=conn,
            email=email
        )

    @staticmethod
    def send_registration_email(
            conn: DbConnection,
            email: str,
            from_user: UserModel | None = None,
            prevent_locked=False
    ):
        def __send_not_whitelisted_mail():
            row = conn.select_one_or_none("""
            SELECT p.request_guid, UNIX_TIMESTAMP(p.blocked) as blocked
              FROM app.registration_whitelist_pending p
             WHERE p.email = %s
            """, [email])

            if row is None:
                blocked = None
                request_guid = GembaseUtils.get_guid()
                email_id = UserData.get_or_create_email_id(conn=conn, email=email)
                conn.query("""
                INSERT INTO app.registration_whitelist_pending (email, ip_address, request_guid)
                VALUES (%s, %s, %s)
                """, [email, request.remote_addr, request_guid])
            else:
                blocked = row["blocked"]
                request_guid = row["request_guid"]

            if blocked is None:
                UserRegistrationHelper.__send_not_whitelisted_email(
                    conn=conn,
                    email=email,
                    request_guid=request_guid,
                    from_user=from_user
                )

        row = conn.select_one_or_none("""
        SELECT r.guid as request_guid, r.name, r.locked, UNIX_TIMESTAMP(r.blocked) as blocked
          FROM app.users_registration_requests r
         WHERE r.email = %s
        """, [email])

        if row is not None:

            if row["blocked"] is not None:
                return

            user_id = UserData.get_user_id_from_email(conn=conn, email=email)
            if user_id == 0:
                UserRegistrationHelper.__send_invite_mail_for_user_without_organization(
                    conn=conn,
                    request_guid=row["request_guid"],
                    user_email=email,
                    user_name=row["name"],
                    locked=False
                )
            else:
                UserRegistrationHelper.__send_already_registered_email(conn=conn, email=email)
        else:
            user_id = UserData.get_user_id_from_email(conn=conn, email=email)
            if user_id == 0:
                __send_not_whitelisted_mail()
            else:
                UserRegistrationHelper.__send_already_registered_email(conn=conn, email=email)
