import json

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.apps.app_data_model import AppDataModel
from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.emails.emails_helper import EmailsHelper
from src.server.models.user.user_obfuscator import UserObfuscator
from src.server.models.user.user_data import UserData
from src.server.models.user.user_emails import UserEmails
from src.server.models.user.user_constants import uc
from src.utils.gembase_utils import GembaseUtils
from src.utils.hash_utils import sha256
from src.server.models.user.user_organization_model import UserOrganizationModel


class UserModel:

    def __init__(self, conn: DbConnection, user_id: int):
        self.__conn = conn
        self.__user_id = user_id
        self.__role = 0
        self.__email: str | None = None
        self.__obfuscator: UserObfuscator | None = None
        self.__fake_logged_by_user_id: int | None = None
        self.__fake_logged_user_id: int | None = None
        self.__organization_model: UserOrganizationModel | None = None
        self.__reload_data()

    def __reload_data(self):
        data = self.__conn.select_one("""
        SELECT u.role,
               u.prime_number,
               u.fake_login,
               u.email
          FROM app.users u
         WHERE u.id = %s
        """, [self.__user_id])

        self.__role = data["role"]
        self.__fake_logged_user_id = data["fake_login"]
        self.__email = data["email"]

        prime_number = data["prime_number"]

        if self.__organization_model is None:
            self.__organization_model = UserOrganizationModel(self)
        else:
            self.__organization_model.reload_data()
        organization_data = self.__organization_model.get_data()
        if organization_data is not None:
            prime_number = organization_data["prime_number"]

        self.__obfuscator = UserObfuscator(multiplier=prime_number)

    def conn(self) -> DbConnection:
        return self.__conn

    def get_id(self) -> int:
        return self.__user_id

    def get_email(self) -> str:
        return self.__email

    def get_organization(self):
        return self.__organization_model

    def is_organization(self):
        return self.__organization_model.is_organization()

    def get_user_prime_number(self):
        return self.__conn.select_one("""
        SELECT prime_number
          FROM app.users
         WHERE id = %s
        """, [self.__user_id])["prime_number"]

    def get_credits(self):
        return UserData.get_credits(
            conn=self.conn(),
            user_id=self.__user_id
        )

    def add_credits(self, credits_value: float, context: any) -> str | None:

        if credits_value == 0:
            return None

        organization_data = self.__organization_model.get_data()
        if organization_data is not None:
            credits_before = organization_data["credits"]
            self.__conn.query("""
            UPDATE app.organization
               SET credits = credits + %s
             WHERE id = %s
            """, [credits_value, organization_data["id"]])
        else:
            credits_before = self.__conn.select_one("""
            SELECT credits FROM app.users WHERE id = %s
            """, [self.__user_id])["credits"]
            self.__conn.query("""
            UPDATE app.users
               SET credits = credits + %s
             WHERE id = %s
            """, [credits_value, self.__user_id])

        audit_guid = GembaseUtils.get_guid()

        self.__conn.query("""
        INSERT INTO audit.users_credits (user_id, audit_guid, price, credits_before, context)
        VALUES (%s, %s, %s, %s, %s) 
        """, [self.__user_id, audit_guid, credits_value, credits_before, json.dumps(context)])

        return audit_guid

    def is_admin(self) -> bool:
        return self.__role == uc.USER_ROLE_ADMIN or self.__role == uc.USER_ROLE_SUPER_ADMIN

    def obfuscator(self) -> UserObfuscator:
        return self.__obfuscator

    def confirm_tos(self):
        self.__conn.query("""
        UPDATE app.users u
           SET tos_agree_t = NOW()
         WHERE u.id = %s
        """, [self.__user_id])

    def get_dev_id_int(self) -> int:
        organization_data = self.__organization_model.get_data()
        if organization_data is not None:
            return organization_data["dev_id_int"]

        row = self.__conn.select_one_or_none("""
        SELECT d.dev_id_int
          FROM app.users_devs d
         WHERE d.user_id = %s
         LIMIT 1
        """, [self.__user_id])
        if row is not None:
            return row["dev_id_int"]

        return -1

    def get_dev_detail(self) -> dict:
        dev_id_int = self.get_dev_id_int()
        return AppDataModel.get_devs_details(
            conn=self.conn(),
            devs_ids_int=[dev_id_int]
        )[dev_id_int]

    def get_client_data(self):
        data = self.__conn.select_one("""
        SELECT u.guid,
               u.role,
               u.email,
               u.name,
               u.position_area,
               u.position_role,
               UNIX_TIMESTAMP(u.tos_agree_t) as tos_agree_t,
               u.concepts_counter,
               u.added_to_my_apps,
               u.tutorial_finished,
               IF(u.free_trial_end_t > NOW(), 1, 0) as free_trial
          FROM app.users u
         WHERE id = %s
        """, [self.__user_id])

        data["tutorial_finished"] = True if data["tutorial_finished"] != 0 else False
        data["organization"] = None

        organization_data = self.__organization_model.get_data()
        if organization_data is not None:
            data["organization"] = {
                "role": organization_data["role"]
            }

        data["dev_detail"] = self.get_dev_detail()

        if self.is_fake_logged():
            data["fake_login"] = True

        return data

    def is_fake_logged(self):
        return self.__fake_logged_by_user_id is not None

    def fake_logout(self):
        if self.is_fake_logged():
            self.__conn.query("""
            UPDATE app.users u
               SET u.fake_login = 0
             WHERE u.id = %s
            """, [self.__fake_logged_by_user_id])

    def get_user_who_is_fake_logged_by_this_user(self):
        if self.is_admin() and self.__fake_logged_user_id != 0:
            try:
                user = UserModel(conn=self.__conn, user_id=self.__fake_logged_user_id)
                user.__fake_logged_by_user_id = self.__user_id
                return user
            except Exception as err:
                return self
        return self

    def get_blocked(self) -> bool:
        row = self.__conn.select_one("""
        SELECT unix_timestamp(blocked) as blocked FROM app.users WHERE id = %s
        """, [self.__user_id])

        return row["blocked"] is not None

    def get_concepts_counter(self) -> int:
        return self.__conn.select_one("""
        SELECT concepts_counter 
          FROM app.users 
         WHERE id = %s
        """, [self.__user_id])["concepts_counter"]

    def set_concepts_counter(self, value: int):
        self.__conn.query("""
        UPDATE app.users
           SET concepts_counter = %s
         WHERE id = %s
        """, [value, self.__user_id])

    def can_create_concept(self) -> bool:
        if BillingUtils.is_module_locked(
            conn=self.__conn,
            user_id=self.__user_id,
            module_id=BillingUtils.BILLING_MODULE_AUDITOR
        ):
            if self.get_concepts_counter() >= 2:
                return False
        return True

    def get_removed_initial_app(self) -> bool:
        removed_initial_app = self.__conn.select_one("""
        SELECT removed_initial_app FROM app.users WHERE id = %s
        """, [self.__user_id])["removed_initial_app"]
        return removed_initial_app == 1

    def set_removed_initial_app(self):
        self.__conn.query("""
        UPDATE app.users SET removed_initial_app = 1 WHERE id = %s
        """, [self.__user_id])

    def get_my_apps(self) -> (list[int], int | None):
        rows = self.__conn.select_all("""
        SELECT ua.app_id_int
          FROM app.users_apps ua
         WHERE ua.user_id = %s
        """, [self.__user_id])

        my_apps = []
        is_demo_app = None

        if len(rows) == 0 and not self.get_removed_initial_app():
            my_apps.append(BillingUtils.UNLOCKED_DEFAULT_APP_ID_INT)
            is_demo_app = BillingUtils.UNLOCKED_DEFAULT_APP_ID_INT

        for row in rows:
            my_apps.append(row["app_id_int"])

        return my_apps, is_demo_app

    def track_action(self, action: str, data: str):
        self.__conn.query("""
        INSERT INTO archive.users_tracking (user_id, action, data) 
        VALUES (%s, %s, %s)
        """, [self.__user_id, action, data])

    def update(self, **kwargs) -> dict:

        update_query = []
        update_params = []
        allowed = ["name", "position_role", "position_area"]

        for k in allowed:
            if k in kwargs:
                update_query.append(f"{k} = %s")
                update_params.append(kwargs[k])

        if len(update_query) > 0:
            update_params.append(self.__user_id)
            update_query_db = ",".join(update_query)
            self.__conn.query(f"""
            UPDATE app.users u
               SET {update_query_db}
             WHERE u.id = %s
            """, update_params)

        return self.get_client_data()

    def get__tutorial_finished(self) -> bool:
        return self.__conn.select_one("""
        SELECT tutorial_finished
          FROM app.users
         WHERE id = %s
        """, [self.__user_id])["tutorial_finished"] != 0

    def set__tutorial_finished(self):
        if self.is_fake_logged():
            return
        if self.get__tutorial_finished():
            return

        self.__conn.query("""
        UPDATE app.users 
           SET tutorial_finished = 1 
         WHERE id = %s
        """, [self.__user_id])

    def set_password(self, password: str):
        password_hash, secret = sha256(password)

        self.__conn.query("""
        UPDATE app.users 
           SET password = %s,
               secret = %s
         WHERE id = %s
        """, [password_hash, secret, self.__user_id])

        return self.get_client_data()

    def get_name(self):
        return UserData.get_name(conn=self.__conn, user_id=self.__user_id)

    def get_organization_title(self) -> str | None:
        if not self.__organization_model.is_organization():
            return None
        dev_detail = self.get_dev_detail()
        return dev_detail["title"]

    def request_password_change(self):

        if self.get_blocked():
            return

        email = self.get_email()
        name = self.get_name()

        self.__conn.query("""
        DELETE FROM app_temp_data.users_password_reset_pending u
         WHERE u.id = %s
        """, [self.__user_id])

        guid = GembaseUtils.get_guid()
        self.__conn.query("""
        INSERT INTO app_temp_data.users_password_reset_pending (id, guid, email)
        VALUES (%s, %s, %s)
        """, [self.__user_id, guid, email])

        subscribe_guid = UserData.get_or_create_unsubscribe_guid(
            conn=self.__conn,
            email=email
        )

        res = UserEmails.password_change(
            subscribe_guid=subscribe_guid,
            to_address=email,
            guid=guid,
            user_name=name
        )
        EmailsHelper.archive_email(self.__conn, res)

    def set_organization_from_request(self, request_guid: str):

        row = self.conn().select_one("""
        SELECT r.organization_id, 
               o.dev_id_int
          FROM app.organization_requests r,
               app.organization o
         WHERE r.request_guid = %s
           AND r.organization_id = o.id
        """, [request_guid])

        self.set_my_dev_and_apps(
            dev_id_int=row["dev_id_int"]
        )

        self.conn().query("""
        INSERT INTO app.organization_users (organization_id, user_id, role, active)
        VALUES (%s, %s, 'member', TRUE)
        """, [row["organization_id"], self.__user_id])

        self.conn().query("""
        INSERT INTO app.organization_users_modules (organization_id, user_id, billing_id, module_id) 
        SELECT r.organization_id, %s as user_id, rm.billing_id, rm.module_id
          FROM app.organization_requests r,
               app.organization_requests_modules rm
         WHERE r.request_guid = %s
           AND r.organization_id = %s
           AND r.request_id = rm.request_id
        """, [self.__user_id, request_guid, row["organization_id"]])

        self.conn().query("""
        DELETE FROM app.organization_requests_modules rm
         WHERE EXISTS (
             SELECT 1
               FROM app.organization_requests r
              WHERE r.request_id = rm.request_id
                AND r.request_guid = %s
                AND r.organization_id = %s
         )
        """, [request_guid, row["organization_id"]])

        self.conn().query("""
        DELETE FROM app.organization_requests r
         WHERE r.request_guid = %s
        """, [request_guid])

    def remove_my_dev_and_apps(self, remove_apps=False, keep_concepts=False):
        if remove_apps:
            if keep_concepts:
                self.conn().query("""
                DELETE FROM app.users_apps a
                WHERE a.user_id = %s AND NOT EXISTS (
                    SELECT 1 FROM scraped_data.apps_concepts c WHERE c.app_id_int = a.app_id_int
                )""", [self.__user_id])
            else:
                self.conn().query("""
                DELETE FROM app.users_apps 
                WHERE user_id = %s""", [self.get_id()])
        self.conn().query("DELETE FROM app.users_devs WHERE user_id = %s", [self.__user_id])

    def set_my_dev_and_apps(
            self,
            dev_id_int: int,
            apps_ids_int: list[int] | None = None,
            remove_existing_apps=False,
            keep_concepts=False
    ):
        self.remove_my_dev_and_apps(remove_apps=remove_existing_apps, keep_concepts=keep_concepts)

        self.conn().query("""
        INSERT INTO app.users_devs (user_id, dev_id_int) 
        VALUES (%s, %s) 
        """, [self.__user_id, dev_id_int])

        if apps_ids_int is not None and len(apps_ids_int) > 0:
            queries = []
            for app_id_int in apps_ids_int:
                queries.append(f"SELECT {app_id_int} as app_id_int")
            queries_str = " UNION ".join(queries)
            query = f"""
            INSERT INTO app.users_apps (user_id, app_id_int)
            SELECT {self.__user_id} as user_id, z1.app_id_int
              FROM ({queries_str}) z1
            WHERE z1.app_id_int NOT IN (
               SELECT a.app_id_int
                 FROM app.users_apps a
                WHERE a.user_id = {self.__user_id}
               )
            """
            self.conn().query(query)

        self.conn().query("""
        INSERT INTO app.users_apps (user_id, app_id_int)
        SELECT ud.user_id,
               a.app_id_int
          FROM app.users_devs ud,
               scraped_data.apps_valid a,
               scraped_data.devs_apps da,
               scraped_data.devs_devs dd
         WHERE ud.user_id = %s
           AND ud.dev_id_int = dd.parent_dev_id_int
           AND dd.child_dev_id_int = da.dev_id_int
           AND da.app_id_int = a.app_id_int
           AND da.primary_dev = 1
           AND a.app_id_int NOT IN (
               SELECT z.app_id_int
                 FROM app.users_apps z
                WHERE z.user_id = %s
           )
         ORDER BY a.loyalty_installs DESC
        """, [self.__user_id, self.__user_id])

        apps_cnt = self.conn().select_one("""
        SELECT count(1) as cnt FROM app.users_apps ua WHERE ua.user_id = %s
        """, [self.__user_id])["cnt"]

        if apps_cnt == 0:

            apps_ids_int = [
                157,
                174,
                188,
                177,
                148,
                17666,
                14,
                79
            ]
            # Homescapes 157,
            # Hill Climb Racing 2 - 174
            # Words of Wonders - 188
            # Coin Master - 177
            # Township - 148
            # Call of Duty: Mobile Season 7 - 17666
            # 8 Ball Pool - 14
            # aquapark.io - 79

            if BillingUtils.UNLOCKED_DEFAULT_APP_ID_INT not in apps_ids_int:
                apps_ids_int.append(BillingUtils.UNLOCKED_DEFAULT_APP_ID_INT)

            self.conn().query(f"""
            INSERT INTO app.users_apps (user_id, app_id_int)
            SELECT {self.__user_id} as user_id, a.app_id_int
              FROM scraped_data.apps a
             WHERE a.app_id_int IN ({self.conn().values_arr_to_db_in(apps_ids_int, int_values=True)})
            """)

