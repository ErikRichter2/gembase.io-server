from typing import Optional

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.auth_db_model import AuthDbModel
from src.db.base.base_db_model import BaseDbModel
from src.db.calc_db_model import CalcDbModel
from src.db.emails_db_model import EmailsDbModel
from src.db.games_db_model import GamesDbModel
from src.db.public_db_model import PublicDbModel
from src.db.user_db_model import UserDbModel
from src.models.emails.user_emails import UserEmails
from src.models.scraper.store_scrapers.google_play_scraper_model import GooglePlayScraperModel
from src.models.scraper.store_scrapers.steam_scraper_model import SteamScraperModel


class OrganizationsDbModel(BaseDbModel):
    
    def __init__(self, conn: DbConnection):
        super(OrganizationsDbModel, self).__init__(conn=conn)
    
    def get_organization_id_by_domain(self, email_domain: str) -> int | None:
        row_org_id = self.conn.select_one_or_none("""
            SELECT o.id
              FROM app.organization o,
                   app.organization_domains od
             WHERE o.id = od.organization_id
               AND od.domain = %s
            """, [email_domain])
        if row_org_id is not None:
            return row_org_id["id"]
        return None

    def delete(
        self,
        organization_id: int,
    ):
        self.conn.query("DELETE FROM app.organization WHERE id = %s", [organization_id])
        self.conn.query("DELETE FROM app.organization_users WHERE organization_id = %s", [organization_id])
        self.conn.query("DELETE FROM app.organization_domains WHERE organization_id = %s", [organization_id])

    def create(
            self,
            dev_id_int: int,
            prime_number: int,
            users: list[dict],
            email_domain: str | None,
            credits_value: int
    ) -> int:
        organization_id = self.conn.insert("""
            INSERT INTO app.organization (dev_id_int, prime_number, credits)
            VALUES (%s, %s, %s)
            """, [dev_id_int, prime_number, credits_value])

        for user in users:
            self.conn.query("""
                INSERT INTO app.organization_users (organization_id, user_id, role, active)
                VALUES (%s, %s, %s, true)
                """, [organization_id, user["user_id"], user["role"]])

        if email_domain is not None:
            self.conn.query("""
                INSERT INTO app.organization_domains (organization_id, domain)
                VALUES (%s, %s)
                """, [organization_id, email_domain])

        return organization_id

    def add_user(self, organization_id: int, user_id: int, role: str):
        self.conn.query("""
        INSERT INTO app.organization_users (organization_id, user_id, role, active)
        VALUES (%s, %s, %s, 1)
        """, [organization_id, user_id, role])

    def is_email_allowed(self, organization_id: int, email: str) -> bool:
        rows_domains = self.conn.select_all("""
        SELECT d.domain
          FROM app.organization_domains d
         WHERE d.organization_id = %s
        """, [organization_id])

        found = False
        email_domain = Utils.get_email_domain(email=email)
        for row in rows_domains:
            if row["domain"] == email_domain:
                found = True
                break

        return found

    def add_request(self, organization_id: int, email: str, from_user: UserDbModel) -> str:

        assert Utils.is_email(email)
        assert from_user.organization_db.is_organization_admin()

        cnt = self.conn.select_one("""
        SELECT count(1) as cnt
          FROM app.organization_requests o
         WHERE o.organization_id = %s
        """, [organization_id])["cnt"]

        assert cnt <= 50

        assert self.is_email_allowed(organization_id=organization_id, email=email)

        self.conn.query("""
        DELETE FROM app.organization_requests r
         WHERE r.organization_id = %s
           AND r.email = %s
        """, [organization_id, email])

        request_guid = Utils.get_guid()

        PublicDbModel(conn=self.conn).get_or_create_email_id(email=email)

        self.conn.query("""
        INSERT INTO app.organization_requests (organization_id, email, request_guid, from_user_id)
        VALUES (%s, %s, %s, %s)
        """, [organization_id, email, request_guid, from_user.user_id])

        return request_guid

    def send_organization_invite(
            self,
            request_guid: str,
            from_user_email: str
    ):

        row = self.conn.select_one("""
        SELECT o.email, 
               o.locked,
               o.organization_id
          FROM app.organization_requests o
         WHERE o.request_guid = %s
        """, [request_guid])

        locked = row["locked"]
        email = row["email"]
        organization_id = row["organization_id"]
        
        public_db = PublicDbModel(conn=self.conn)
        
        email_id = public_db.get_email_id(email=email)
        assert email_id is not None
        
        subscribe_guid = public_db.get_or_create_unsubscribe_guid(email_id=email_id)

        auth_db = AuthDbModel(conn=self.conn)

        user_id = auth_db.get_user_id_from_email(email=email)

        if user_id == 0:
            dev_detail = self.get_dev_detail(organization_id=organization_id)
            assert dev_detail is not None
            res = UserEmails.organization_invite_mail_for_new_user(
                subscribe_guid=subscribe_guid,
                locked=locked,
                organization_name=dev_detail["title"],
                request_guid=request_guid,
                organization_admin_email=from_user_email,
                to_address=email
            )
        else:
            user_data = self.conn.select_one("""
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

        EmailsDbModel(conn=self.conn).archive_email(res)

        self.conn.query("""
        UPDATE app.organization_requests o
           SET o.sent_request_t = NOW()
         WHERE o.organization_id = %s
           AND o.request_guid = %s
        """, [organization_id, request_guid])

    def get_dev_detail(
            self,
            organization_id: int
    ):
        dev_id_int = self.conn.select_one("""
          SELECT dev_id_int FROM gb_app.organization WHERE id = %s
          """, [organization_id])["dev_id_int"]

        devs = GamesDbModel(conn=self.conn).get_devs_details(devs_ids_int=[dev_id_int])
        if devs is not None:
            return devs[dev_id_int]
        return None
        