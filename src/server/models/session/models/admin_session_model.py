import json

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.private_data.private_data_model import PrivateDataModel
from gembase_server_core.utils.gb_utils import GbUtils
from src import external_api
from src.server.models.apps.app_model import AppModel
from src.server.models.dms.dms_constants import DmsConstants
from src.server.models.dms.dms_model import DmsCache, DmsModel
from src.server.models.platform_values.cache.platform_values_cache import PlatformValuesCache
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.tags.tags_constants import TagsConstants
from src.server.models.user.user_data import UserData
from src.server.models.user.user_model import UserModel
from src.server.models.user.user_obfuscator import UserObfuscator
from src.utils.gembase_utils import GembaseUtils


class AdminSessionModel(BaseSessionModel):

    def __init__(self, session):
        super(AdminSessionModel, self).__init__(session)

    def reject_user_tags_override_request(
            self,
            request_id: int
    ):
        self.conn().query("""
            UPDATE app.users_tags_override_requests SET state = 'rejected' WHERE request_id = %s
            """, [request_id])

    def get_user_tags_override_client_data(
            self
    ):
        rows_requests = self.conn().select_all("""
            SELECT r.request_id, r.user_id, r.app_id_int, r.state, r.rejected_reason, u.email,
                   UNIX_TIMESTAMP(r.t) as t
              FROM app.users_tags_override_requests r,
                   app.users u
             WHERE r.user_id = u.id
            """)

        rows_requests_tags = self.conn().select_all("""
            SELECT request_id, tags_before, tag_id_int, tag_rank 
              FROM app.users_tags_override_requests_tags
            """)

        apps_ids = []
        app_id_int_per_request = {}
        res = {}
        for row in rows_requests:
            app_id_int = row[UserObfuscator.APP_ID_INT]
            if app_id_int not in apps_ids:
                apps_ids.append(app_id_int)
            res[row["request_id"]] = {
                "request_id": row["request_id"],
                "user_id": row["user_id"],
                "email": row["email"],
                "t": row["t"],
                "state": row["state"],
                "rejected_reason": row["rejected_reason"]
            }
            if app_id_int not in app_id_int_per_request:
                app_id_int_per_request[app_id_int] = []
            app_id_int_per_request[app_id_int].append(row["request_id"])
            res[row["request_id"]]["app_detail"] = {}
            res[row["request_id"]]["request_tags"] = []
            res[row["request_id"]]["before_tags"] = []

        for row in rows_requests_tags:
            if row["tags_before"] == 1:
                res[row["request_id"]]["before_tags"].append({
                    UserObfuscator.TAG_ID_INT: row[UserObfuscator.TAG_ID_INT],
                    "tag_rank": row["tag_rank"]
                })
            else:
                res[row["request_id"]]["request_tags"].append({
                    UserObfuscator.TAG_ID_INT: row[UserObfuscator.TAG_ID_INT],
                    "tag_rank": row["tag_rank"]
                })

        apps_details = AppModel.get_app_detail_bulk(
            conn=self.conn(),
            app_ids_int=apps_ids,
            remove_desc=True
        )
        for app_id_int in apps_details:
            if app_id_int in app_id_int_per_request:
                for request_id in app_id_int_per_request[app_id_int]:
                    res[request_id]["app_detail"] = apps_details[app_id_int]

        return [res[k] for k in res]

    def reset_tutorial(self):
        self.conn().query("""
        DELETE FROM app.users_tutorial WHERE user_id = %s
        """, [self.user_id()])
        self.conn().query("""
        UPDATE app.users SET tutorial_finished = 0 WHERE id = %s
        """, [self.user_id()])

    def update_email_templates_from_sheet(self):
        sheet = PrivateDataModel.get_private_data()["google"]["google_docs"]["platform"]
        rows = external_api.read_sheet(sheet["sheet_id"], "Mails", to_arr_dict=True)

        rows_existing = self.conn().select_all("""
        SELECT id, 
               template_def 
          FROM app.def_email_templates
        """)

        map_email_id = {}
        for row in rows_existing:
            map_email_id[row["template_def"]] = row["id"]

        bulk_data_insert = []
        bulk_data_update = []
        for row in rows:
            if row["Type"] in map_email_id:
                bulk_data_update.append(
                    (row["Type"], row["Subject"], row["Title"], row["Body"], row["Footer"], map_email_id[row["Type"]]))
            else:
                bulk_data_insert.append((row["Type"], row["Subject"], row["Title"], row["Body"], row["Footer"]))

        self.conn().bulk("""
            INSERT INTO app.def_email_templates (template_def, subject, title, body, footer) 
            VALUES (%s, %s, %s, %s, %s)
            """, bulk_data_insert)

        self.conn().bulk("""
            UPDATE app.def_email_templates 
               SET template_def = %s, 
                   subject = %s, 
                   title = %s, 
                   body = %s, 
                   footer = %s 
            WHERE id = %s
            """, bulk_data_update)

    def users_set_developer(
            self,
            email: str,
            dev_id_in_store: str,
            store: int,
            customer_id=0,
            is_concept=False,
            concept_name="",
            remove_existing_apps=False,
            keep_concepts=True
    ):
        if self.session().user().is_fake_logged():
            raise Exception(f"Cannot set developer for fake login")

        scraped_app_ids_int = None

        if is_concept:
            dev_id_int = AppModel.create_dev_concept(
                conn=self.conn(),
                title=concept_name
            )["dev_id_int"]
        else:
            dev_id_int = AppModel.get_dev_id_int(
                conn=self.conn(),
                dev_id=ScraperModel.get_dev_id_from_dev_id_in_store(
                    dev_id_in_store=dev_id_in_store,
                    store=store
                )
            )

            if dev_id_int is None or not ScraperModel.is_dev_scraped(
                    conn=self.conn(),
                    dev_id_int=dev_id_int
            ):
                scraped = ScraperModel.scrap_dev(
                    conn=self.conn(),
                    dev_id_in_store=dev_id_in_store,
                    store=store
                )

                if scraped["state"] == 1:
                    dev_id_int = scraped["dev_id_int"]
                    scraped_app_ids_int = scraped["app_ids_int"]
            else:
                apps_ids = AppModel.get_devs_apps_ids_int(
                    conn=self.conn(),
                    user_id=0,
                    devs_ids_int=[dev_id_int],
                )
                if dev_id_int in apps_ids:
                    scraped_app_ids_int = apps_ids[dev_id_int]

        if dev_id_int is None or dev_id_int == 0:
            return {
                "error": "Developer not found"
            }

        if customer_id is not None and customer_id != 0:
            self.conn().query("""
            UPDATE app.customers
            SET data = JSON_SET(data, '$.dev_id_int', %s)
            WHERE id = %s
            """, [dev_id_int, customer_id])

        if GbUtils.is_email(email):
            user_id = UserData.get_user_id_from_email(
                conn=self.conn(),
                email=email
            )
            if user_id != 0:
                user = UserModel(
                    conn=self.conn(),
                    user_id=user_id
                )

                user.set_my_dev_and_apps(
                    dev_id_int=dev_id_int,
                    apps_ids_int=scraped_app_ids_int,
                    remove_existing_apps=remove_existing_apps,
                    keep_concepts=keep_concepts
                )

                if user.get_organization().is_organization_admin():
                    self.conn().query("""
                    UPDATE app.organization
                       SET dev_id_int = %s
                     WHERE id = %s
                    """, [dev_id_int, user.get_organization().get_id()])
            else:
                self.conn().query("""
                UPDATE app.users_registration_requests r
                   SET r.dev_id_int = %s
                 WHERE r.email = %s
                """, [dev_id_int, email])

        return self.get_users()

    def block_user(self, email: str, remove_block=False):

        if remove_block:
            self.conn().query("""
                        UPDATE app.users u SET u.blocked = NULL WHERE u.email = %s
                        """, [email])
            self.conn().query("""
                        UPDATE app.users_registration_requests u SET u.blocked = NULL WHERE u.email = %s
                        """, [email])
            self.conn().query("""
                        UPDATE app.registration_whitelist_pending u SET u.blocked = NULL WHERE u.email = %s
                        """, [email])
        else:
            self.conn().query("""
            UPDATE app.users u SET u.blocked = NOW() WHERE u.email = %s
            """, [email])
            self.conn().query("""
            UPDATE app.users_registration_requests u SET u.blocked = NOW() WHERE u.email = %s
            """, [email])
            self.conn().query("""
            UPDATE app.registration_whitelist_pending u SET u.blocked = NOW() WHERE u.email = %s
            """, [email])

        return self.get_users()

    def get_users(self):
        rows_registered = self.conn().select_all("""
            SELECT u.guid, u.email, u.name, 
            IF (d.title is NULL, IF(dd.title IS NULL, '', dd.title), d.title) as developer_title,
            UNIX_TIMESTAMP(u.created_t) as created_t, 
            UNIX_TIMESTAMP(u.session_t) as session_t,
            UNIX_TIMESTAMP(u.sent_request_t) as sent_request_t,
            UNIX_TIMESTAMP(u.free_trial_end_t) as free_trial_end_t,
            UNIX_TIMESTAMP(u.blocked) as blocked
              FROM app.users u
             LEFT JOIN app.users_devs ud ON ud.user_id = u.id
             LEFT JOIN scraped_data.devs d ON d.dev_id_int = ud.dev_id_int
             LEFT JOIN scraped_data.devs_concepts dd ON dd.dev_id_int = ud.dev_id_int
             WHERE u.system_user = 0
               AND u.role = 0
            """)

        rows_whitelist = self.conn().select_all("""
            SELECT r.guid as request_guid, r.email, r.name, IF(d.title is NULL, dd.title, d.title) as title, 0 as is_organization, r.locked,
                   UNIX_TIMESTAMP(r.sent_request_t) as sent_request_t, NULL as request_t, 1 as whitelisted,
                   UNIX_TIMESTAMP(r.free_trial_end_t) as free_trial_end_t, 
                   UNIX_TIMESTAMP(r.responded_t) as responded_t,
                   UNIX_TIMESTAMP(r.blocked) as blocked
              FROM app.users_registration_requests r
              LEFT JOIN scraped_data.devs d ON d.dev_id_int = r.dev_id_int
              LEFT JOIN scraped_data.devs_concepts dd ON dd.dev_id_int = r.dev_id_int
             UNION ALL
            SELECT r.request_guid, r.email, r.name, IF(d.title is NULL, dd.title, d.title) as title, 1 as is_organization, r.locked,
                   UNIX_TIMESTAMP(r.sent_request_t), NULL, 1 as whitelisted, NULL, 
                   NULL as responded_t, NULL as blocked
              FROM app.organization_requests r
             inner join app.organization o ON r.organization_id = o.id
              LEFT JOIN scraped_data.devs d ON d.dev_id_int = o.dev_id_int
              LEFT JOIN scraped_data.devs_concepts dd ON dd.dev_id_int = o.dev_id_int
            UNION ALL
            SELECT r.request_guid, r.email, '' as name, '' as title, 0 as is_organization, 0 as locked, 
            NULL, UNIX_TIMESTAMP(r.t), 0 as whitelisted, NULL, 
            UNIX_TIMESTAMP(r.responded_t), UNIX_TIMESTAMP(r.blocked) as blocked
              FROM app.registration_whitelist_pending r
            """)

        rows_customers = self.conn().select_all("""
            SELECT c.id, c.email, c.data, 
            IF (d.title is NULL, IF(dd.title IS NULL, data->>'$.company', dd.title), d.title) as developer_title
            FROM app.customers c
            LEFT JOIN scraped_data.devs d ON d.dev_id_int = data->>'$.dev_id_int'
            LEFT JOIN scraped_data.devs_concepts dd ON dd.dev_id_int = data->>'$.dev_id_int'
            WHERE c.deleted IS NULL
            """)

        rows_emails_sent_cnt = self.conn().select_all("""
        SELECT email, count(1) as cnt
        FROM archive.users_sent_emails group by email
        """)

        for row in rows_customers:
            row["data"] = json.loads(row["data"])
            row["data"]["id"] = row["id"]

        return {
            "registered": rows_registered,
            "whitelist": rows_whitelist,
            "customers": rows_customers,
            "emails_sent_cnt": rows_emails_sent_cnt
        }

    def users_set_text(
            self,
            email: str,
            customer_id: int,
            parameter: str,
            value: str,
            is_timestamp=False
    ):
        if is_timestamp:
            value = int(value)

        if parameter == "freeTrialEnd":
            timestamp = value

            self.conn().query("""
                UPDATE app.users SET free_trial_end_t = FROM_UNIXTIME(%s) WHERE email = %s
                """, [timestamp, email])

            self.conn().query("""
                UPDATE app.users_registration_requests SET free_trial_end_t = FROM_UNIXTIME(%s) WHERE email = %s
                """, [timestamp, email])

            return self.get_users()

        if parameter == "email":
            if customer_id is not None and customer_id != 0:
                prev_row = self.conn().select_one_or_none("""
                SELECT id, data FROM app.customers WHERE email = %s
                AND deleted IS NULL
                """, [value])
                next_row = self.conn().select_one_or_none("""
                SELECT id, data FROM app.customers WHERE id = %s
                AND deleted IS NULL
                """, [customer_id])
                if prev_row is not None and next_row is not None:
                    prev_data = json.loads(prev_row["data"])
                    next_data = json.loads(next_row["data"])
                    merged_data = {}
                    for k in prev_data:
                        merged_data[k] = prev_data[k]
                    for k in next_data:
                        merged_data[k] = next_data[k]
                    self.conn().query("""
                    UPDATE app.customers SET email = %s, data = %s WHERE id = %s
                    """, [value, json.dumps(merged_data), prev_row["id"]])
                    if customer_id != prev_row["id"]:
                        self.conn().query("""
                        UPDATE app.customers SET deleted = NOW() WHERE id = %s
                        """, [customer_id])
                else:
                    self.conn().query(f"""
                    UPDATE app.customers
                    SET email = %s
                    WHERE id = %s
                    AND deleted IS NULL
                    """, [value, customer_id])

                return self.get_users()

        if customer_id is not None and customer_id != 0:
            self.conn().query(f"""
            UPDATE app.customers
            SET data = JSON_SET(data, '$.{parameter}', %s)
            WHERE id = %s
            AND deleted IS NULL
            """, [value, customer_id])

        if GbUtils.is_email(email):
            self.__create_customer_row_if_not_exists(email)
            self.conn().query(f"""
            UPDATE app.customers
            SET data = JSON_SET(data, '$.{parameter}', %s)
            WHERE email = %s
            AND deleted IS NULL
            """, [value, email])
            self.conn().query_safe(f"""
            UPDATE app.users SET {parameter} = %s WHERE email = %s
            """, [value, email])
            self.conn().query_safe(f"""
            UPDATE app.users_registration_requests SET {parameter} = %s WHERE email = %s
            """, [value, email])

        return self.get_users()

    def __create_customer_row_if_not_exists(self, email: str):
        row = self.conn().select_one_or_none("""
        SELECT 1 FROM app.customers WHERE email = %s AND deleted IS NULL
        """, [email])
        if row is None:
            self.conn().query("""
            INSERT INTO app.customers (email, data) VALUES (%s, %s)
            """, [email, json.dumps({})])

    def users_delete(
            self,
            email: str,
            customer_id: int
    ):
        self.conn().query("""
        UPDATE app.customers SET deleted = NOW() WHERE id = %s
        """, [customer_id])

        return self.get_users()

    def users_add(
            self,
    ):
        self.conn().query("""
        INSERT INTO app.customers (data) VALUES (%s)
        """, [json.dumps({})])

        return self.get_users()

    @staticmethod
    def update_def_sheets(
            conn: DbConnection,
            sheet_name: str
    ):
        def update_platform_billing(d: {}):

            bulk_data = []
            for row in d["platform_modules_pricing"]:
                bulk_data.append((row["id"], row["name"], int(row["price"]) / 100, int(row["hidden"]),
                                  row["invoice_name"], row["invoice_desc"]))
            conn.query("""
                    DELETE FROM billing.def_modules_pricing
                    """)
            conn.bulk("""
                    INSERT INTO billing.def_modules_pricing (id, name, price, hidden, invoice_name, invoice_desc) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """, bulk_data)

            bulk_data = []
            for row in d["platform_modules_discount"]:
                bulk_data.append(
                    (row["id"], row["name"], int(row["discount_perc"]), row["invoice_name"], row["invoice_desc"]))
            conn.query("""
                            DELETE FROM billing.def_modules_discount
                            """)
            conn.bulk("""
                            INSERT INTO billing.def_modules_discount (id, name, discount_perc, invoice_name, invoice_desc) 
                            VALUES (%s, %s, %s, %s, %s)
                            """, bulk_data)

        def update_user_position_role(rows: []):
            bulk_data = []
            for row in rows:
                bulk_data.append((int(row["id"]), row["value"]))
            conn.query("""
            DELETE FROM app.def_user_position_role
            """)
            conn.bulk("""
            INSERT INTO app.def_user_position_role (id, value) VALUES (%s, %s)
            """, bulk_data)

        def update_user_position_area(rows: []):
            bulk_data = []
            for row in rows:
                bulk_data.append((int(row["id"]), row["value"]))
            conn.query("""
            DELETE FROM app.def_user_position_area
            """)
            conn.bulk("""
            INSERT INTO app.def_user_position_area (id, value) VALUES (%s, %s)
            """, bulk_data)

        def update_subcategories_goals(rows: []):
            bulk_data = []
            for row in rows:
                bulk_data.append((float(row["goal"]), row["category"]))
            conn.query("DELETE FROM app.def_sheet_subcategory_goals")
            conn.bulk("""
            INSERT INTO app.def_sheet_subcategory_goals (subcategory_int, goal_value)
            SELECT m.id, %s as goal_value
              FROM app.map_tag_subcategory m
             WHERE m.subcategory = %s
            """, bulk_data)

        def update_countries_map(countries: []):
            arr = []
            for country in countries:
                arr.append(f"SELECT '{country.lower()}' as country")
            countries_db = " UNION ".join(arr)

            rows = conn.select_all(f"""
            SELECT z.country
              FROM ({countries_db}) z
             WHERE NOT EXISTS (
                SELECT 1
                  FROM app.map_country_id m
                 WHERE LOWER(m.country) = z.country
             )
            """)

            bulk_data = []
            for row in rows:
                bulk_data.append((row["country"],))
            if len(bulk_data) > 0:
                conn.bulk("""
                INSERT INTO app.map_country_id (country) VALUES (%s)
                """, bulk_data)

            rows = conn.select_all("""
            SELECT m.id, m.country
              FROM app.map_country_id m
            """)
            res = {}
            for row in rows:
                res[row["country"]] = row["id"]
            return res

        def update_studies_traits_map(traits: []):
            arr = []
            for trait in traits:
                arr.append(f"SELECT '{trait.lower()}' as trait_id")
            traits_db = " UNION ".join(arr)

            rows = conn.select_all(f"""
            SELECT z.trait_id
              FROM ({traits_db}) z
             WHERE NOT EXISTS (
                SELECT 1
                  FROM app.map_studies_traits m
                 WHERE LOWER(m.trait_id) = z.trait_id
             )
            """)

            bulk_data = []
            for row in rows:
                bulk_data.append((row["trait_id"],))
            if len(bulk_data) > 0:
                conn.bulk("""
                INSERT INTO app.map_studies_traits (trait_id) VALUES (%s)
                """, bulk_data)

        def update_countries_from_product_sheet(d):
            countries = []
            for row in d["Countries"]:
                countries.append(row["Code2"])
            countries_map = update_countries_map(countries)
            bulk_data = []
            for row in d["Countries"]:
                bulk_data.append((row["Country"], row["Code2"], row["Code3"], row["VAT Abbreviation"],
                                  GembaseUtils.int_safe(row["VAT Characters Min"]),
                                  GembaseUtils.int_safe(row["VAT Characters Max"]),
                                  countries_map[row["Code2"].lower()]))
            conn.query("""
            UPDATE app.def_countries
               SET vat_abbreviation = NULL, vat_name = NULL, vat_characters_min = 0, vat_characters_max = 0
            """)
            if len(bulk_data) > 0:
                conn.bulk("""
                UPDATE app.def_countries d
                   SET d.vat_name = %s,
                       d.code_2 = %s,
                       d.code_3 = %s,
                       d.vat_abbreviation = %s,
                       d.vat_characters_min = %s,
                       d.vat_characters_max = %s
                 WHERE d.country_id = %s
                """, bulk_data)
                conn.bulk("""
                INSERT INTO app.def_countries (vat_name, code_2, code_3, vat_abbreviation, vat_characters_min,
                            vat_characters_max, country_id, survey_cpi)
                SELECT z1.vat_name, z1.code_2, z1.code_3, z1.vat_abbreviation, z1.vat_characters_min,
                       z1.vat_characters_max, z1.country_id, 0 as survey_cpi
                  FROM (SELECT %s as vat_name, %s as code_2, %s as code_3, %s as vat_abbreviation,
                  %s as vat_characters_min, %s as vat_characters_max, %s as country_id) z1
                 WHERE NOT EXISTS (
                     SELECT 1
                       FROM app.def_countries d
                      WHERE d.country_id = z1.country_id
                 )
                """, bulk_data)

        def update_countries(sheet_rows: []):
            countries = []
            for row in sheet_rows:
                countries.append(row["country"])
            countries_map = update_countries_map(countries)
            bulk_data = []
            for row in sheet_rows:
                bulk_data.append((int(row["survey_cpi"]) / 100, row["name"], countries_map[row["country"].lower()]))
            conn.query("""
            UPDATE app.def_countries
               SET survey_cpi = 0
            """)
            if len(bulk_data) > 0:
                conn.bulk("""
                UPDATE app.def_countries d
                   SET d.survey_cpi = %s,
                       d.name = %s
                 WHERE d.country_id = %s
                """, bulk_data)
                conn.bulk("""
                INSERT INTO app.def_countries (survey_cpi, name, country_id)
                SELECT z1.survey_cpi, z1.name, z1.country_id
                  FROM (SELECT %s as survey_cpi, %s as name, %s as country_id) z1
                 WHERE NOT EXISTS (
                     SELECT 1
                       FROM app.def_countries d
                      WHERE d.country_id = z1.country_id
                 )
                """, bulk_data)

        def update_studies_dcm_concepts(sheet_rows: []):
            headers = []
            features = []
            for row in sheet_rows:
                if row["title"] is not None and row["title"] != "" and row["description"] is not None and row[
                    "description"] != "":
                    headers.append({
                        "title": row["title"],
                        "description": row["description"]
                    })
                for k in ["features_1", "features_2"]:
                    if row[k] is not None and row[k] != "":
                        features.append({
                            "pool_id": k,
                            "text": row[k]
                        })
            conn.query("""
            DELETE FROM app.def_studies_dcm_concepts
            """)
            conn.query("""
            INSERT INTO app.def_studies_dcm_concepts (headers, features) VALUES (%s, %s)
            """, [json.dumps(headers), json.dumps(features)])

        def update_studies_traits(sheet_rows: []):
            bulk_data = []
            traits = []
            for row in sheet_rows:
                bulk_data.append((row["name"], row["default"], row["id"].lower()))
                traits.append(row["id"])
            update_studies_traits_map(traits)
            conn.query("""
            DELETE FROM app.def_studies_traits
            """)
            if len(bulk_data) > 0:
                conn.bulk("""
                INSERT INTO app.def_studies_traits (id, name, study_default)
                SELECT m.id as id, %s as name, %s as study_default
                  FROM app.map_studies_traits m
                 WHERE m.trait_id = %s
                """, bulk_data)

        update_platform_values_bit_tags = False
        update_sheet = True
        sheet = PrivateDataModel.get_private_data()["google"]["google_docs"][sheet_name]

        if update_sheet:
            r = None
            if "range" in sheet:
                r = sheet["range"]
            d = external_api.sheet_to_dict(sheet["sheet_id"], r)
            DmsModel.save_json_to_dms(conn, d, guid=sheet['dms_guid'])
        else:
            d = DmsModel.get_dms_data_to_json(conn, sheet['dms_guid'])

        def update_is_survey_tag_flag(survey_tags_str_ids: []):
            survey_tags_str_ids_db = conn.values_arr_to_db_in(survey_tags_str_ids)
            query = f"""
            UPDATE app.def_sheet_platform_product p
            INNER JOIN (
               SELECT m.id
                 FROM app.map_tag_id m
                WHERE m.tag_id IN ({survey_tags_str_ids_db})
            ) z1 ON z1.id = p.tag_id_int
            SET p.is_survey = 1
            """
            conn.query("""
            UPDATE app.def_sheet_platform_product
               SET is_survey = 0
            """)
            conn.query(query)

        def update_product_weights():
            platform_values_sheet = DmsCache.get_json(conn, DmsConstants.platform_values_guid)
            if platform_values_sheet is not None:
                conn.query("""
                UPDATE app.def_sheet_platform_product 
                   SET competitors_pool_w = 0,
                       threatscore_similarity_w = 0
                """)
                for row in platform_values_sheet["competitors_pool_weights"]:
                    conn.query("""
                    UPDATE app.def_sheet_platform_product
                       SET competitors_pool_w = %s
                     WHERE subcategory = %s
                    """, [int(row["weight"]), row["subcategory"]])
                for row in platform_values_sheet["threatscore_similarity_weights"]:
                    conn.query("""
                    UPDATE app.def_sheet_platform_product
                       SET threatscore_similarity_w = %s
                     WHERE subcategory = %s
                    """, [int(row["weight"]), row["subcategory"]])

        def process_store_tags():
            data = []
            for row in d["Product"]:
                if row["ID"] is not None and row["ID"] != "":
                    o = {
                        "tag_id": row["ID"],
                        "store": {
                            AppModel.STORE__GOOGLE_PLAY: {
                                "a": [],
                                "o": []
                            },
                            AppModel.STORE__STEAM: {
                                "a": [],
                                "o": []
                            }
                        }
                    }
                    val = row["GoogleO"]
                    if val != "":
                        o["store"][AppModel.STORE__GOOGLE_PLAY]["o"] = val.split(",")
                    val = row["GoogleA"]
                    if val != "":
                        o["store"][AppModel.STORE__GOOGLE_PLAY]["a"] = val.split(",")
                    val = row["SteamA"]
                    if val != "":
                        o["store"][AppModel.STORE__STEAM]["a"] = val.split(",")
                    val = row["SteamO"]
                    if val != "":
                        o["store"][AppModel.STORE__STEAM]["o"] = val.split(",")

                    data.append(o)

            store_tags_ids = []
            bulk_data_def = []
            bulk_data_map = []
            for o in data:
                for store in o["store"]:
                    for k in o["store"][store]:
                        for i in range(len(o["store"][store][k])):
                            store_tag = o["store"][store][k][i].strip().lower()
                            if store == AppModel.STORE__STEAM:
                                store_tag = f"steam__{store_tag}"
                            if store_tag not in store_tags_ids:
                                store_tags_ids.append(store_tag)
                                bulk_data_def.append((store, store_tag, o["store"][store][k][i]))
                            t = "append" if k == "a" else "override"
                            bulk_data_map.append((t, o["tag_id"], store_tag))

            rows_existing = conn.select_all(f"""
            SELECT d.store_tag FROM scraped_data.def_store_tags d
            WHERE d.store_tag IN ({conn.values_arr_to_db_in(store_tags_ids)})
            """)
            tags_existing = [row["store_tag"] for row in rows_existing]
            bulk_data_def = [it for it in bulk_data_def if it[1] not in tags_existing]

            conn.bulk("""
            INSERT INTO scraped_data.def_store_tags (store, store_tag, store_tag_raw)
            VALUES (%s, %s, %s)
            """, bulk_data_def)

            conn.query("DELETE FROM scraped_data.def_map_store_tags")
            conn.bulk("""
            INSERT INTO scraped_data.def_map_store_tags (tag_id_int, store_tag_id, type)
            SELECT m.id, st.id, %s as type
              FROM app.map_tag_id m,
                   scraped_data.def_store_tags st
             WHERE m.tag_id = %s
               AND st.store_tag = %s
            """, bulk_data_map)

        if "post_process" in sheet:
            if sheet["post_process"] == "platform":
                conn.query("""
                DELETE FROM app.def_sheet_platform_product
                """)
                for row in d["Product"]:
                    if row["ID"] is not None and row["ID"] != "":
                        # todo hack - set Platforms category as Mechanics
                        if row["Subcategory"] == "Platforms":
                            row["Category"] = "Mechanics"
                        if row["Subcategory"] == "Monetization":
                            row["Category"] = "Mechanics"
                        subgenre = 0
                        if "Subgenre" in row and row["Subgenre"] == "1":
                            subgenre = 1
                        # todo hack - hidden platforms
                        hidden = 0
                        unlocked = "Unlocked" in row and row["Unlocked"] == "1"
                        loved = 0
                        if "Loved" in row:
                            loved_str = row["Loved"]
                            if loved_str is not None and loved_str != "":
                                loved = float(loved_str)
                        if row["Subcategory"] == "Platforms":
                            unlocked = 1
                            if row["Node"] != "PC" and row["Node"] != "Mobile":
                                hidden = 1
                        is_prompt = row["Category"] in ['Mechanics', 'Content']
                        not_rejected = row["NotRejected"] == "1"
                        conn.query("""
                        INSERT INTO app.def_sheet_platform_product (tag_id, adj, description, category, subcategory, node, 
                        is_prompt, unlocked, hidden, subgenre, loved_ratio_ext, not_rejected) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, [row["ID"], row["Adj"], row["Description"], row["Category"], row["Subcategory"],
                              row["Node"], is_prompt, unlocked, hidden, subgenre, loved, not_rejected])
                for row in d["Audience"]:
                    if row["ID"] is not None and row["ID"] != "" and row["Category"] != "Behaviors":
                        unlocked = "Unlocked" in row and row["Unlocked"] == "1"
                        conn.query("""
                        INSERT INTO app.def_sheet_platform_product (tag_id, description, category, subcategory, 
                        node, is_prompt, unlocked) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, [row["ID"], row["Description"], "Audience", row["Category"], row["Node"], 0, unlocked])
                conn.query("""
                INSERT INTO app.map_tag_id (tag_id) 
                SELECT p.tag_id
                  FROM app.def_sheet_platform_product p
                 WHERE p.tag_id NOT IN (
                    SELECT m.tag_id
                      FROM app.map_tag_id m)
                """)
                conn.query("""
                UPDATE app.def_sheet_platform_product p
                INNER JOIN app.map_tag_id m ON m.tag_id = p.tag_id
                SET p.tag_id_int = m.id
                """)

                query = """
                INSERT INTO app.map_tag_subcategory (subcategory)
                SELECT DISTINCT subcategory
                FROM app.def_sheet_platform_product
                WHERE subcategory NOT IN (SELECT x.subcategory FROM app.map_tag_subcategory x)
                ORDER BY subcategory;
                """
                conn.query(query)

                query = """
                UPDATE app.def_sheet_platform_product p
                INNER JOIN app.map_tag_subcategory map ON map.subcategory = p.subcategory
                SET p.subcategory_int = map.id
                """
                conn.query(query)

                subc_name = conn.select_one("""
                SELECT m.subcategory
                  FROM app.map_tag_subcategory m
                 WHERE m.id = %s
                """, [TagsConstants.SUBCATEGORY_CORE_ID])["subcategory"]

                parent_nodes = []
                core_parent = {}
                for row in d["Product"]:
                    if row["ID"] is not None and row["ID"] != "" and row["Parent"] is not None and row["Parent"] != "":
                        if row["Subcategory"].lower() == subc_name.lower():
                            if row["Parent"] not in parent_nodes:
                                parent_nodes.append(row["Parent"])
                            core_parent[row["ID"]] = row["Parent"]
                if len(parent_nodes) > 0:
                    rows_parent_nodes = conn.select_all(f"""
                    SELECT p.tag_id_int, p.node
                      FROM app.def_sheet_platform_product p
                     WHERE p.node IN ({conn.values_arr_to_db_in(parent_nodes)})
                    """)
                    bulk_data = []
                    for k in core_parent:
                        for row in rows_parent_nodes:
                            if core_parent[k] == row["node"]:
                                bulk_data.append((k, row["tag_id_int"]))
                                break
                    query = """
                    UPDATE app.def_sheet_platform_product p
                     INNER JOIN app.map_tag_id m1
                        ON m1.tag_id = %s
                       AND m1.id = p.tag_id_int 
                       SET p.parent_genre_for_core_tag_id_int = %s
                    """
                    conn.bulk(query, bulk_data)

                update_countries_from_product_sheet(d)

                update_product_weights()
                update_platform_values_bit_tags = True

                process_store_tags()

            elif sheet["post_process"] == "platform_values":
                conn.query("""
                UPDATE app.def_sheet_platform_product
                   SET threatscore_similarity_w = 0
                """)
                rows_subcategory_ids = conn.select_all("""
                SELECT s.id, s.subcategory
                  FROM app.map_tag_subcategory s
                """)
                subcategory_ids = {}
                for row in rows_subcategory_ids:
                    subcategory_ids[row["subcategory"]] = row["id"]
                for row in d["threatscore_similarity_weights"]:
                    if row["subcategory"] in subcategory_ids:
                        conn.query("""
                        UPDATE app.def_sheet_platform_product p
                           SET p.threatscore_similarity_w = %s
                         WHERE p.subcategory_int = %s
                        """, [row["weight"], subcategory_ids[row["subcategory"]]])
                conn.query("""
                DELETE FROM app.def_sheet_platform_values_install_tiers
                """)
                for row in d["app_installs_tiers"]:
                    conn.query("""
                    INSERT INTO app.def_sheet_platform_values_install_tiers (tier, store_id, value_from, value_to) VALUES (%s, %s, %s, %s)
                    """, [row["tier"], AppModel.STORE__GOOGLE_PLAY, row["google_from"], row["google_to"]])
                    conn.query("""
                    INSERT INTO app.def_sheet_platform_values_install_tiers (tier, store_id, value_from, value_to) VALUES (%s, %s, %s, %s)
                    """, [row["tier"], AppModel.STORE__STEAM, row["steam_from"], row["steam_to"]])

                conn.query("""
                DELETE FROM app.def_sheet_platform_threats_params
                """)
                for row in d["threatscore_params"]:
                    if row["v1"] == "NULL":
                        row["v1"] = None
                    if row["v2"] == "NULL":
                        row["v2"] = None
                    conn.query("""
                    INSERT INTO app.def_sheet_platform_threats_params (param, v1, v2, v3, name, color) VALUES (%s, %s, %s, %s, %s, %s)
                    """, [row["param"], row["v1"], row["v2"], row["v3"], row["name"], row["color"]])

                conn.query("""
                DELETE FROM app.def_sheet_platform_age_groups
                """)
                for row in d["age_groups"]:
                    conn.query("""
                    INSERT INTO app.def_sheet_platform_age_groups (age_from, age_to, group_name) VALUES (%s, %s, %s)
                    """, [row["age_from"], row["age_to"], row["name"]])

                conn.query("""
                DELETE FROM app.def_sheet_platform_spending_groups
                """)
                for row in d["spending_groups"]:
                    conn.query("""
                    INSERT INTO app.def_sheet_platform_spending_groups (id, group_name, spending_from, spending_to) 
                    VALUES (%s, %s, %s, %s)
                    """, [row["id"], row["name"], row["spending_from"], row["spending_to"]])

                # -----------
                # survey tags
                # -----------
                survey_tags_str_ids = []
                for row in d["survey_tags"]:
                    survey_tags_str_ids.append(row["tag_id"])

                update_product_weights()
                update_is_survey_tag_flag(survey_tags_str_ids)
                update_platform_values_bit_tags = True

                conn.query("""
                DELETE FROM app.def_sheet_platform_values_constants
                """)
                for row in d["constants"]:
                    conn.query("""
                    INSERT INTO app.def_sheet_platform_values_constants (param, v1) VALUES (%s, %s)
                    """, [row["param"], row["v1"]])

                # -----------
                # countries
                # -----------
                update_countries(d["countries"])

                # -----------
                # studies dcm concepts
                # -----------
                update_studies_dcm_concepts(d["studies_dcm_concepts"])

                # -----------
                # studies traits
                # -----------
                update_studies_traits(d["studies_traits"])

                # -----------
                # user position roles
                # -----------
                update_user_position_role(d["user_position_role"])

                # -----------
                # user position areas
                # -----------
                update_user_position_area(d["user_position_area"])

                # -----------
                # subcategories goals
                # -----------
                update_subcategories_goals(d["categories_goals"])

                update_platform_billing(d)

        return "OK"
