from __future__ import annotations
from typing import TYPE_CHECKING

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.user.organization_helper import OrganizationHelper
from src.server.models.user.user_data import UserData
from src.utils.gembase_utils import GembaseUtils

if TYPE_CHECKING:
    from src.server.models.user.user_model import UserModel


class UserOrganizationModel:

    def __init__(self, user: UserModel):
        self.__user_model: UserModel = user
        self.__id: int | None = None
        self.__is_organization_admin = False
        self.reload_data()

    def reload_data(self):
        data = self.get_data()

        if data is not None:
            self.__id = data["id"]
            self.__is_organization_admin = data is not None and data["role"] == "admin"

    def conn(self) -> DbConnection:
        return self.__user_model.conn()

    def is_organization(self) -> bool:
        return self.__id is not None

    def get_credits(self) -> int | None:
        data = self.get_data()
        if data is None:
            return None
        return data["credits"]

    def get_data(self) -> dict:
        row = self.conn().select_one_or_none("""
        SELECT o.id, ou.role, o.dev_id_int, o.credits, o.prime_number
          FROM app.organization_users ou,
               app.organization o
         WHERE ou.organization_id = o.id
           AND ou.active = 1
           AND ou.user_id = %s
        """, [self.__user_model.get_id()])

        return row

    def get_id(self) -> int | None:
        return self.__id

    def is_organization_admin(self) -> bool:
        return self.__is_organization_admin

    def remove_organization_request(self, request_guid: str):
        assert GembaseUtils.is_guid(request_guid)
        assert self.__is_organization_admin

        organization_id = self.get_id()

        self.conn().query("""
                DELETE FROM app.organization_requests_modules rm
                 WHERE rm.request_id IN (
                     SELECT r.request_id
                     FROM app.organization_requests r
                     WHERE r.organization_id = %s
                       AND r.request_guid = %s
                 )
                """, [organization_id, request_guid])

        self.conn().query("""
                DELETE FROM app.organization_requests r
                 WHERE r.organization_id = %s
                   AND r.request_guid = %s
                """, [organization_id, request_guid])

    def send_confirmation_mail(self, request_guid: str):

        assert self.__is_organization_admin

        OrganizationHelper.send_organization_invite(
            conn=self.conn(),
            request_guid=request_guid,
            from_user_email=self.__user_model.get_email()
        )

    def add_request(self, email: str):
        return OrganizationHelper.add_request(
            conn=self.conn(),
            organization_id=self.get_id(),
            email=email,
            from_user=self.__user_model
        )

    def set_licences(self, added_accounts: [], licences: []):

        assert self.__is_organization_admin

        add_requests = []
        guid_map = {}
        organization_id = self.get_id()

        for added_account in added_accounts:
            add_requests.append(added_account["request_guid"])
            email = added_account["email"]

            assert GembaseUtils.is_email(email)

            cnt = self.conn().select_one("""
            SELECT count(1) as cnt
              FROM app.organization_requests o
             WHERE o.organization_id = %s
            """, [organization_id])["cnt"]

            assert cnt <= 50

            rows_domains = self.conn().select_all("""
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

            assert found

            self.conn().query("""
            DELETE FROM app.organization_requests r
             WHERE r.organization_id = %s
               AND r.email = %s
            """, [organization_id, email])

            request_guid = GembaseUtils.get_guid()
            guid_map[added_account["request_guid"]] = request_guid

            email_id = UserData.get_or_create_email_id(conn=self.conn(), email=email)

            self.conn().query("""
            INSERT INTO app.organization_requests (organization_id, email, request_guid, from_user_id)
            VALUES (%s, %s, %s, %s)
            """, [organization_id, email, request_guid, self.__user_model.get_id()])

        self.conn().query("""
        DELETE FROM app.organization_requests_modules rm
        WHERE rm.request_id IN (
        SELECT r.request_id
        FROM app.organization_requests r
        WHERE r.organization_id = %s
        )
       """, [organization_id])

        self.conn().query("""
        DELETE FROM app.organization_users_modules oum
         WHERE oum.organization_id = %s
        """, [organization_id])

        for licence in licences:

            guid = licence["user_or_request_guid"]
            if guid in guid_map:
                guid = guid_map[guid]

            self.conn().query("""
            INSERT INTO app.organization_users_modules (organization_id, user_id, billing_id, module_id)
            SELECT %s as organization_id, 
                   u.id as user_id, 
                   b.id as billing_id, 
                   %s as module_id
              FROM app.users u,
                   billing.billings b
             WHERE u.guid = %s
               AND b.guid = %s
            """, [organization_id, licence["module_id"],
                  licence["user_or_request_guid"], licence["billing_guid"]])

            self.conn().query("""
            INSERT INTO app.organization_requests_modules 
            (request_id, billing_id, module_id)
            SELECT r.request_id, b.id as billing_id, %s as module_id
              FROM app.organization_requests r,
                   billing.billings b
             WHERE r.request_guid = %s
               AND b.guid = %s
            """, [licence["module_id"], guid, licence["billing_guid"]])

    def create(self, credits_value: int):
        assert not self.is_organization()

        OrganizationHelper.create(
            conn=self.conn(),
            dev_id_int=self.__user_model.get_dev_id_int(),
            prime_number=self.__user_model.get_user_prime_number(),
            users=[{
                "user_id": self.__user_model.get_id(),
                "role": "admin"
            }],
            email_domain=GembaseUtils.get_email_domain(self.__user_model.get_email()),
            credits_value=credits_value
        )

        self.reload_data()

    def set_organization(self, organization_id: int):
        self.conn().query("""
        DELETE FROM app.organization_users ou
         WHERE ou.user_id = %s
        """, [self.__user_model.get_id()])

        self.conn().query("""
        INSERT INTO app.organization_users (organization_id, user_id, role, active)
        VALUES (%s, %s, 'member', TRUE)
        """, [organization_id, self.__user_model.get_id()])

        self.reload_data()

    def remove_request(self, request_guid: str):
        assert GembaseUtils.is_guid(request_guid)
        assert self.is_organization_admin()

        self.conn().query("""
                    DELETE FROM app.organization_requests_modules rm
                     WHERE rm.request_id IN (
                         SELECT r.request_id
                         FROM app.organization_requests r
                         WHERE r.organization_id = %s
                           AND r.request_guid = %s
                     )
                    """, [self.get_id(), request_guid])

        self.conn().query("""
                    DELETE FROM app.organization_requests r
                     WHERE r.organization_id = %s
                       AND r.request_guid = %s
                    """, [self.get_id(), request_guid])
