from __future__ import annotations
from typing import TYPE_CHECKING

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.apps.app_data_model import AppDataModel
from src.server.models.emails.emails_helper import EmailsHelper
from src.server.models.user.user_data import UserData
from src.server.models.user.user_emails import UserEmails
from src.utils.gembase_utils import GembaseUtils

if TYPE_CHECKING:
    from src.server.models.user.user_model import UserModel


class OrganizationHelper:

    @staticmethod
    def get_organization_id_by_domain(conn: DbConnection, email_domain: str) -> int | None:
        row_org_id = conn.select_one_or_none("""
            SELECT o.id
              FROM app.organization o,
                   app.organization_domains od
             WHERE o.id = od.organization_id
               AND od.domain = %s
            """, [email_domain])
        if row_org_id is not None:
            return row_org_id["id"]
        return None

    @staticmethod
    def delete(
        conn: DbConnection,
        organization_id: int,
    ):
        conn.query("DELETE FROM app.organization WHERE id = %s", [organization_id])
        conn.query("DELETE FROM app.organization_users WHERE organization_id = %s", [organization_id])
        conn.query("DELETE FROM app.organization_domains WHERE organization_id = %s", [organization_id])

    @staticmethod
    def create(
            conn: DbConnection,
            dev_id_int: int,
            prime_number: int,
            users: [],
            email_domain: str | None,
            credits_value: int
    ) -> int:
        organization_id = conn.insert("""
            INSERT INTO app.organization (dev_id_int, prime_number, credits)
            VALUES (%s, %s, %s)
            """, [dev_id_int, prime_number, credits_value])

        for user in users:
            conn.query("""
                INSERT INTO app.organization_users (organization_id, user_id, role, active)
                VALUES (%s, %s, %s, true)
                """, [organization_id, user["user_id"], user["role"]])

        if email_domain is not None:
            conn.query("""
                INSERT INTO app.organization_domains (organization_id, domain)
                VALUES (%s, %s)
                """, [organization_id, email_domain])

        return organization_id

    @staticmethod
    def add_user(conn: DbConnection, organization_id: int, user_id: int, role: str):
        conn.query("""
        INSERT INTO app.organization_users (organization_id, user_id, role, active)
        VALUES (%s, %s, %s, 1)
        """, [organization_id, user_id, role])

    @staticmethod
    def is_email_allowed(conn: DbConnection, organization_id: int, email: str) -> bool:
        rows_domains = conn.select_all("""
        SELECT d.domain
          FROM app.organization_domains d
         WHERE d.organization_id = %s
        """, [organization_id])

        found = False
        email_domain = GembaseUtils.get_email_domain(email=email)
        for row in rows_domains:
            if row["domain"] == email_domain:
                found = True
                break

        return found

    @staticmethod
    def add_request(conn: DbConnection, organization_id: int, email: str, from_user: UserModel) -> str:

        assert GembaseUtils.is_email(email)
        assert from_user.get_organization().is_organization_admin()

        cnt = conn.select_one("""
        SELECT count(1) as cnt
          FROM app.organization_requests o
         WHERE o.organization_id = %s
        """, [organization_id])["cnt"]

        assert cnt <= 50

        assert OrganizationHelper.is_email_allowed(conn=conn, organization_id=organization_id, email=email)

        conn.query("""
        DELETE FROM app.organization_requests r
         WHERE r.organization_id = %s
           AND r.email = %s
        """, [organization_id, email])

        request_guid = GembaseUtils.get_guid()

        UserData.get_or_create_email_id(conn=conn, email=email)

        conn.query("""
        INSERT INTO app.organization_requests (organization_id, email, request_guid, from_user_id)
        VALUES (%s, %s, %s, %s)
        """, [organization_id, email, request_guid, from_user.get_id()])

        return request_guid

    @staticmethod
    def send_organization_invite(
            conn: DbConnection,
            request_guid: str,
            from_user_email: str
    ):

        row = conn.select_one("""
        SELECT o.email, 
               o.locked,
               o.organization_id
          FROM app.organization_requests o
         WHERE o.request_guid = %s
        """, [request_guid])

        locked = row["locked"]
        email = row["email"]
        organization_id = row["organization_id"]

        subscribe_guid = UserData.get_or_create_unsubscribe_guid(
            conn=conn,
            email=email
        )

        user_id = UserData.get_user_id_from_email(
            conn=conn,
            email=email
        )

        if user_id == 0:
            dev_detail = OrganizationHelper.get_dev_detail(
                conn=conn,
                organization_id=organization_id
            )
            res = UserEmails.organization_invite_mail_for_new_user(
                subscribe_guid=subscribe_guid,
                locked=locked,
                organization_name=dev_detail["title"],
                request_guid=request_guid,
                organization_admin_email=from_user_email,
                to_address=email
            )
        else:
            user_data = conn.select_one("""
            SELECT name, 
                   email 
              FROM app.users 
             WHERE id = %s
            """, [user_id])

            res = UserEmails.organization_invite_mail_for_existing_user(
                subscribe_guid=subscribe_guid,
                organization_admin_email=from_user_email,
                request_guid=request_guid,
                user_name=user_data["name"],
                user_email=user_data["email"],
                locked=locked,
            )

        EmailsHelper.archive_email(conn, res)

        conn.query("""
        UPDATE app.organization_requests o
           SET o.sent_request_t = NOW()
         WHERE o.organization_id = %s
           AND o.request_guid = %s
        """, [organization_id, request_guid])

    @staticmethod
    def get_dev_detail(
            conn: DbConnection,
            organization_id: int
    ):
        dev_id_int = conn.select_one("""
          SELECT dev_id_int FROM app.organization WHERE id = %s
          """, [organization_id])["dev_id_int"]

        return AppDataModel.get_devs_details(
            conn=conn,
            devs_ids_int=[dev_id_int]
        )[dev_id_int]
