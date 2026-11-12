import json

from werkzeug.datastructures import FileStorage

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.games_db_model import GamesDbModel
from src.db.services_db_model import ServicesDbModel
from src.db.surveys_db_model import SurveysDbModel
from src.db.user_billing_db_model import UserBillingDbModel
from src.models.user_obfuscator import UserObfuscator
from src.processes.process_args import ProcessArgs


class UserGamesDbModel(BaseUserDbModel):


    def __init__(self, conn: DbConnection, user_id: int):
        super(UserGamesDbModel, self).__init__(conn=conn, user_id=user_id)
    
    def save_concept_app(
            self,
            app_id_int: int,
            app_detail_changes: dict,
            files: list[FileStorage]
    ):
        if self.user_db.billing_db.is_module_locked(UserBillingDbModel.BILLING_MODULE_AUDITOR):
            raise Exception(f"Cannot save concept in demo")

        row = self.conn.select_one_or_none("""
        SELECT 1
          FROM gb_scraped_data.apps_concepts a
         WHERE a.user_id = %s
           AND a.app_id_int = %s
        """, [self.user_id, app_id_int])

        if row is None:
            raise Exception(f"App {app_id_int} does not exist")
        
        if app_detail_changes is not None:
            self.update_concept_app(
                app_id_int=app_id_int,
                app_detail_changes=app_detail_changes,
            )

            if "removed_images" in app_detail_changes:
                bulk_data = []
                for image_id in app_detail_changes["removed_images"]:
                    bulk_data.append((app_id_int, image_id))
                self.conn.bulk("""
                DELETE FROM gb_scraped_data.apps_gallery ag
                WHERE ag.app_id_int = %s
                  AND ag.id = %s
                """, bulk_data)

        if len(files) > 0:
            max_order = 1
            row_max_order = self.conn.select_one_or_none("""
            SELECT MAX(img_order) as max_order
              FROM gb_scraped_data.apps_gallery
             WHERE app_id_int = %s
            """, [app_id_int])

            if row_max_order is not None and row_max_order["max_order"] is not None:
                max_order = row_max_order["max_order"] + 1

            for file in files:
                mime = file.content_type
                dms_row = self.user_db.dms_db.save_file_to_dms(
                    mime=mime,
                    file_type="image",
                    is_binary=True,
                    file=file
                )

                self.conn.query("""
                INSERT INTO gb_scraped_data.apps_gallery
                (app_id_int, img_order, dms_id) 
                VALUES (%s, %s, %s) 
                """, [app_id_int, max_order, dms_row["id"]])
                max_order += 1
                
        return self.get_app_detail(
            app_id_int=app_id_int,
            include_gallery=True
        )
        
    def save_concept_app_icon(self, app_id_int: int, icon_bytes: bytes | None):

        self.conn.query("""
        DELETE FROM gb_scraped_data.apps_icons
         WHERE app_id_int = %s
        """, [app_id_int])

        if icon_bytes is not None:
            self.conn.query("""
            INSERT INTO gb_scraped_data.apps_icons (app_id_int, icon) 
            VALUES (%s, %s)
            """, [app_id_int, icon_bytes])
            
    def create_concept_app_from_temp(
            self,
            app_detail_changes: dict
    ):
        if not self.can_create_concept():
            raise Exception(f"Cannot create more than {self.get_concepts_counter()} concepts in DEMO")

        concepts_counter = self.get_concepts_counter() + 1

        app_id_int = GamesDbModel.create_next_id_atomic()

        self.create_concept_app(
            app_id_int=app_id_int,
            app_detail_changes=app_detail_changes,
            concept_counter=concepts_counter
        )

        self.set_concepts_counter(
            value=concepts_counter
        )

        self.user_db.portal_db.add_app_to_my_apps(
            app_id_int=app_id_int,
            unlocked_in_demo=True
        )

        app_detail = self.get_app_detail(
            app_id_int=app_id_int
        )

        return app_detail            
    
    def get_app_detail(
            self,
            app_id_int: int,
            include_gallery=False,
            include_tags=True,
    ) -> dict:
        return GamesDbModel(conn=self.conn).get_app_detail(
            app_id_int=app_id_int,
            user_id=self.user_id,
            include_gallery=include_gallery,
            include_tags=include_tags,            
        )[0]

    def update_concept_app(self, app_id_int: int, app_detail_changes: dict):

        current_app_detail = self.get_app_detail(app_id_int=app_id_int)

        for p in ["title", "description", "store", "platform"]:
            if p in app_detail_changes:
                current_app_detail[p] = app_detail_changes[p]

        self.conn.query("""
        UPDATE gb_scraped_data.apps_concepts a
           SET a.title = %s,
               a.description = %s,
               a.store = %s,
               a.platform = %s,
               a.t = NOW()
         WHERE a.app_id_int = %s
        """, [current_app_detail["title"],
              current_app_detail["description"],
              current_app_detail["store"],
              current_app_detail["platform"],
              app_id_int])

        if "tags" in app_detail_changes and app_detail_changes["tags"] is not None:
            self.set_manual_tags(
                app_id_int=app_id_int,
                tags_details=app_detail_changes["tags"]
            )
            
    def set_manual_tags(self, app_id_int: int, tags_details: list[dict]):

        if not self.is_concept(app_id_int):
            raise Exception(f"Setting tags allowed only for concept app")

        self.set_tags(
            app_id_int=app_id_int,
            tags_details=tags_details,
            context="concept_manual_tags",
            manual=True
        )

        return self.get_tags(app_id_int)
    
    def get_tagging_state(
            self,
            app_id_int: int,
    ):
        games_db = GamesDbModel(conn=self.conn)
        return games_db.get_tagging_state_bulk(
            user_id=self.user_id,
            apps_ids_int=[app_id_int]
        )[app_id_int]
            
    def get_tags(
            self,
            app_id_int: int
    ):

        tags = []
        tagged_t = None

        tagging_state = self.get_tagging_state(app_id_int=app_id_int)

        if tagging_state["state"] == "done":

            tagged_t = tagging_state["tagged_t"]

            tags = GamesDbModel(conn=self.conn).get_tags_details_bulk(
                apps_ids_int=[app_id_int]
            )[app_id_int]

        return {
            "app_id_int": app_id_int,
            "tagging": tagging_state,
            "tags": tags,
            "tagged_t": tagged_t
        }
            
    def is_concept(self, app_id_int: int, check_owner: int | None = None) -> bool:
        row = self.conn.select_one_or_none("""
        SELECT a.app_id_int, 
               a.user_id
          FROM gb_scraped_data.apps_concepts a
         WHERE a.app_id_int = %s
        """, [app_id_int])

        if row is None:
            return False

        if check_owner is not None and check_owner != row["user_id"]:
            return False

        return True    
    
    def set_tags(
            self,
            app_id_int: int,
            tags_details: list[dict],
            context: str,
            manual=False,
            remove_tags: list | None = None
    ):
        overriden_by_user = 1 if self.conn.select_one_or_none("""
        SELECT 1 FROM gb_tagged_data.tags_override_from_users_apps WHERE app_id_int = %s
        """, [app_id_int]) is not None else 0

        hist_id = self.conn.insert("""
        INSERT INTO gb_tagged_data.platform_tagged_history (app_id_int, context)
        VALUES (%s, %s)
        """, [app_id_int, context])
        self.conn.query("""
        INSERT INTO gb_tagged_data.tags_history (hist_id, prompt_row_id, app_id_int, tag_id_int, tag_rank,
        added_from_store, removed_from_store, is_tag_rank_override, tag_rank_override)
        SELECT %s as hist_id, t.prompt_row_id, t.app_id_int, t.tag_id_int, t.tag_rank,
        t.added_from_store, t.removed_from_store, t.is_tag_rank_override, t.tag_rank_override
          FROM gb_tagged_data.tags t
         WHERE t.app_id_int = %s
        """, [app_id_int, hist_id])

        if remove_tags is not None:
            remove_tags_db = self.conn.values_arr_to_db_in(remove_tags, int_values=True)
            self.conn.query(f"""
            DELETE FROM gb_tagged_data.tags t
             WHERE t.app_id_int = %s
               AND t.tag_id_int IN ({remove_tags_db})
            """, [app_id_int])
        else:
            self.conn.query("""
            DELETE FROM gb_tagged_data.tags t
             WHERE t.app_id_int = %s
            """, [app_id_int])

        self.conn.query("""
        DELETE FROM gb_tagged_data.platform_tagged t
         WHERE t.app_id_int = %s
        """, [app_id_int])

        q_data = []
        for it in tags_details:
            added_from_store = it["added_from_store"] if "added_from_store" in it else 0
            removed_from_store = it["removed_from_store"] if "removed_from_store" in it else 0
            is_tag_rank_override = it["is_tag_rank_override"] if "is_tag_rank_override" in it else 0
            tag_rank_override = it["tag_rank_override"] if "tag_rank_override" in it else 0
            q_data.append((app_id_int, it["tag_id_int"], 0, it["tag_rank"], added_from_store,
                           removed_from_store, is_tag_rank_override, tag_rank_override, overriden_by_user))
        if len(q_data) > 0:
            self.conn.bulk("""
            INSERT INTO gb_tagged_data.tags (app_id_int, tag_id_int, prompt_row_id, tag_rank, 
            added_from_store, removed_from_store, is_tag_rank_override, tag_rank_override, overriden_by_user) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, q_data)

        self.conn.query("""
        INSERT INTO gb_tagged_data.platform_tagged (app_id_int, manual, prompts_b)
        SELECT %s as app_id_int, %s as manual, b.b
          FROM gb_tagged_data.active_prompts_b b
        """, [app_id_int, manual])

    def create_concept_app(
            self,
            app_id_int: int,
            app_detail_changes: dict,
            concept_counter: int
    ):
        self.conn.query("""
        INSERT INTO gb_scraped_data.apps_concepts (app_id_int, app_id_in_store, user_id, title, description, icon, copied_from_app_id_int, platform, concept_counter) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [app_id_int, Utils.get_guid(), self.user_id, "Concept", "", "", app_id_int, GamesDbModel.PLATFORM__MOBILE, concept_counter])

        if app_detail_changes is not None:
            self.update_concept_app(
                app_id_int=app_id_int,
                app_detail_changes=app_detail_changes,
            )

        return app_id_int   
    
    def can_create_concept(self) -> bool:
        if self.user_db.billing_db.is_module_locked(
            module_id=UserBillingDbModel.BILLING_MODULE_AUDITOR
        ):
            if self.get_concepts_counter() >= 2:
                return False
        return True        
    
    def get_concepts_counter(self) -> int:
        return self.conn.select_one("""
        SELECT concepts_counter 
          FROM gb_app.users 
         WHERE id = %s
        """, [self.user_id])["concepts_counter"]
        
    def set_concepts_counter(self, value: int):
        self.conn.query("""
        UPDATE gb_app.users
           SET concepts_counter = %s
         WHERE id = %s
        """, [value, self.user_id])                  
        
    def delete_concept_app(self, app_id_int: int):

        row = self.conn.select_one_or_none("""
        SELECT *
          FROM gb_scraped_data.apps_concepts
         WHERE app_id_int = %s
        """, [app_id_int])

        if row is not None:
            self.conn.query("""
            INSERT INTO gb_archive.archive_data (user_id, type, data, expire_days)  VALUES (%s, %s, %s, %s)
            """, [row["user_id"], "scraped_data.apps_concepts", json.dumps(row, default=str), 30])

            self.conn.query("""
            INSERT INTO gb_archive.apps_concepts
            SELECT * FROM gb_scraped_data.apps_concepts WHERE app_id_int = %s
            """, [app_id_int])

            self.conn.query("""
            DELETE FROM gb_scraped_data.apps_concepts
             WHERE app_id_int = %s
            """, [app_id_int])

            self.conn.query("""
            DELETE FROM gb_scraped_data.devs_apps
             WHERE app_id_int = %s
            """, [app_id_int])

            self.conn.query("""
            DELETE FROM gb_tagged_data.tags t
             WHERE t.app_id_int = %s
            """, [app_id_int])

            self.conn.query("""
            DELETE FROM gb_tagged_data.platform_tagged t
             WHERE t.app_id_int = %s
            """, [app_id_int])

            self.conn.query("""
            DELETE FROM gb_tagged_data.platform_tagging_request t
             WHERE t.app_id_int = %s
            """, [app_id_int])

            self.conn.query("""
            DELETE FROM gb_app.users_apps WHERE app_id_int = %s
            """, [app_id_int])
        
    def tag_store_app_if_not_tagged(
            self,
            app_id_int: int,
            tagging_context: str,
            admin_force=False
    ):
        if self.is_concept(app_id_int=app_id_int):
            raise Exception(f"Cannot tag concept app")

        tagging_state = self.get_tagging_state(app_id_int=app_id_int)

        if admin_force or tagging_state["state"] == "not_tagged":
            tagging_state = self.tag_app(
                app_id_int=app_id_int,
                tagging_context=tagging_context
            )

        return tagging_state         
    
    def tag_app(
            self,
            app_id_int: int,
            tagging_context: str
    ):
        
        local_conn = DbConnection()
        
        local_conn.query("""
        LOCK TABLE gb_tagged_data.platform_tagging_request WRITE,
                   gb_tagged_data.platform_tagging_users WRITE
        """)

        row_request = local_conn.select_one_or_none("""
        SELECT id
          FROM gb_tagged_data.platform_tagging_request
         WHERE app_id_int = %s
        """, [app_id_int])

        if row_request is None:
            request_id = local_conn.insert("""
            INSERT INTO gb_tagged_data.platform_tagging_request (app_id_int, state)
            VALUES (%s, 'queue')
            """, [app_id_int])
        else:
            request_id = row_request["id"]

        local_conn.query("""
        INSERT INTO gb_tagged_data.platform_tagging_users (user_id, app_id_int, context, request_id)
        VALUES (%s, %s, %s, %s)
        """, [self.user_id, app_id_int, tagging_context, request_id])
        local_conn.unlock_tables()

        ServicesDbModel(conn=local_conn).run(ProcessArgs(service_script_id=ServicesDbModel.SERVICE_GPT_TAGGER))
        
        local_conn.close()

        return self.get_tagging_state(
            app_id_int=app_id_int
        )           
        
    def tag_concept_app(
            self,
            app_id_int: int,
            tagging_context: str
    ):
        if not self.is_concept(
            app_id_int=app_id_int
        ):
            raise Exception(f"Cannot tag store app")

        if self.user_db.billing_db.is_module_locked(UserBillingDbModel.BILLING_MODULE_AUDITOR):
            row = self.conn.select_one_or_none("""
            SELECT tagged_by_user
              FROM gb_scraped_data.apps_concepts a,
                   gb_app.users_apps ua
             WHERE a.app_id_int = %s
               AND a.user_id = %s
               AND ua.user_id = a.user_id
               AND ua.app_id_int = a.app_id_int
               AND ua.unlocked_in_demo = 1
            """, [app_id_int, self.user_id])
            if row is None or row["tagged_by_user"] > 0:
                raise Exception(f"Cannot tag concept more than once in DEMO")

            self.conn.query("""
            UPDATE gb_scraped_data.apps_concepts
               SET tagged_by_user = tagged_by_user + 1
             WHERE app_id_int = %s
            """, [app_id_int])

        return self.tag_app(
            app_id_int=app_id_int,
            tagging_context=tagging_context
        )
                
    def get_dev_id_int(self) -> int:
        organization_data = self.user_db.organization_db.get_data()
        if organization_data is not None:
            return organization_data["dev_id_int"]

        row = self.conn.select_one_or_none("""
        SELECT d.dev_id_int
          FROM gb_app.users_devs d
         WHERE d.user_id = %s
         LIMIT 1
        """, [self.user_id])
        if row is not None:
            return row["dev_id_int"]

        return -1

    def create_user_override_request(
            self,
            app_id_int: int,
            tags_details: list[dict]
    ):
        self.conn.query("""
        DELETE FROM gb_app.users_tags_override_requests
         WHERE user_id = %s 
           AND app_id_int = %s
           AND state = 'pending'
        """, [self.user_id, app_id_int])

        request_id = self.conn.insert("""
        INSERT INTO gb_app.users_tags_override_requests (user_id, app_id_int, state)
        VALUES (%s, %s, 'pending')
        """, [self.user_id, app_id_int])

        bulk_data = []
        for o in tags_details:
            bulk_data.append((request_id, o["tag_id_int"], o["tag_rank"]))
        self.conn.bulk("""
        INSERT INTO gb_app.users_tags_override_requests_tags (request_id, tag_id_int, tag_rank)
        VALUES (%s, %s, %s)
        """, bulk_data)

    def get_apps_history_kpis(
            self,
            app_ids_int: list[int],
            kpi: str,
            interval: str
    ) -> dict:

        if len(app_ids_int) == 0:
            return {}
        
        rows = self.conn.select_all(f"""
        SELECT UNIX_TIMESTAMP(h.t) as t, 
               h.installs,
               h.score,
               h.app_id_int
          FROM gb_scraped_data.apps_hist h
         WHERE h.app_id_int IN ({self.conn.values_arr_to_db_in(app_ids_int, int_values=True)})
           AND (
                ('{interval}' = '6m' AND DATE_ADD(h.t, INTERVAL 6 MONTH ) > gb_app.now()) OR
                ('{interval}' = '12m' AND DATE_ADD(h.t, INTERVAL 12 MONTH ) > gb_app.now()) OR
                ('{interval}' = 'all')
               )
         ORDER BY h.app_id_int, h.t
        """)

        rows_per_app_id_int = {}
        for row in rows:
            app_id_int = row["app_id_int"]
            if app_id_int not in rows_per_app_id_int:
                rows_per_app_id_int[app_id_int] = []
            rows_per_app_id_int[app_id_int].append(row)

        # fix installs
        for app_id_int in rows_per_app_id_int:
            cnt = len(rows_per_app_id_int[app_id_int])
            for i in range(0, cnt - 1):
                v1 = rows_per_app_id_int[app_id_int][cnt - 1 - i]["installs"]
                v2 = rows_per_app_id_int[app_id_int][cnt - 1 - i - 1]["installs"]
                if v2 > v1:
                    v2 = v1
                rows_per_app_id_int[app_id_int][cnt - 1 - i - 1]["installs"] = v2

        data_per_app_id_int = {}
        for app_id_int in rows_per_app_id_int:
            if kpi == "quality":
                data_per_app_id_int[app_id_int] = [{
                    "app_id_int": app_id_int,
                    "t": row["t"],
                    "v": round(row["score"] / 20, 1)
                } for row in rows_per_app_id_int[app_id_int]]
            elif kpi == "growth":
                res = []
                prev_t = None
                prev_i = None
                for i in range(0, len(rows_per_app_id_int[app_id_int])):
                    if i == 0:
                        prev_t = rows_per_app_id_int[app_id_int][i]["t"]
                        prev_i = rows_per_app_id_int[app_id_int][i]["installs"]
                        continue
                    t = rows_per_app_id_int[app_id_int][i]["t"]
                    if t - prev_t >= 7 * 24 * 60 * 60:
                        v = rows_per_app_id_int[app_id_int][i]["installs"] - prev_i
                        res.append({
                            "app_id_int": app_id_int,
                            "t": t,
                            "v": v
                        })
                        prev_t = t
                        prev_i = rows_per_app_id_int[app_id_int][i]["installs"]
                data_per_app_id_int[app_id_int] = res
            else:
                data_per_app_id_int[app_id_int] = [{
                    "app_id_int": app_id_int,
                    "t": row["t"],
                    "v": row["installs"]
                } for row in rows_per_app_id_int[app_id_int]]

        return data_per_app_id_int
    
    def get_most_hated_tags(
            self,
            loved_tag_ids_int: list[int],
            hated_tag_ids_int: list[int]
    ):
        survey_id = SurveysDbModel(conn=self.conn).get_survey_meta_id(SurveysDbModel.SURVEY_CONTROL_GUID)
        loved_tags_ids_int_db = self.conn.values_arr_to_db_in(loved_tag_ids_int, int_values=True)

        if len(hated_tag_ids_int) == 0:
            hated_tag_ids_int = [-1]

        hated_tags_ids_int_db = self.conn.values_arr_to_db_in(hated_tag_ids_int, int_values=True)

        loved_cnt = len(loved_tag_ids_int)

        query = f"""
        SELECT st.tag_id_int
          FROM (
                SELECT stb.survey_instance_int
                  FROM (
                        SELECT BIT_OR(d.b) as b
                        FROM gb_platform.def_tags_bin d
                        WHERE d.tag_id_int IN ({loved_tags_ids_int_db})
                       ) l,
                       (
                        SELECT BIT_OR(d.b) as b
                        FROM gb_platform.def_tags_bin d
                        WHERE d.tag_id_int IN ({hated_tags_ids_int_db})
                       ) h,
                       gb_platform.platform_values_survey_tags_bin stb,
                       gb_platform.def_tags_bin d
                 WHERE stb.survey_id = {survey_id}
                   AND BIT_COUNT(stb.b_hated & h.b) > 0
                   AND BIT_COUNT(stb.b_loved & l.b) = {loved_cnt}
                 GROUP BY stb.survey_instance_int
               ) z1,
               gb_platform.platform_values_survey_tags st
         WHERE st.survey_meta_id = {survey_id}
           AND st.survey_instance_int = z1.survey_instance_int
           AND st.tag_id_int IN ({hated_tags_ids_int_db})
           AND st.hated = 1
         GROUP BY st.tag_id_int
         ORDER BY COUNT(1) DESC
         LIMIT 5
        """

        rows = self.conn.select_all(query)

        return {
            "tag_ids_int": [row["tag_id_int"] for row in rows]
        }    
    
    def scrap_apps_for_devs(self, filters: list[dict]) -> bool:
        dev_ids_int: list[int] = []
        for filter in filters:
            if UserObfuscator.DEV_IDS_INT in filter and len(filter[UserObfuscator.DEV_IDS_INT]) > 0:
                for dev_id_int in filter[UserObfuscator.DEV_IDS_INT]:
                    if dev_id_int not in dev_ids_int:
                        dev_ids_int.append(dev_id_int)

        if len(dev_ids_int) == 0:
            return True

        apps_ids_per_dev_id = self.get_devs_apps_ids_int(devs_ids_int=dev_ids_int)

        all_apps_ids = []
        for dev_id_int in apps_ids_per_dev_id:
            all_apps_ids = all_apps_ids + apps_ids_per_dev_id[dev_id_int]

        if len(dev_ids_int) == 0:
            return True

        return GamesDbModel(conn=self.conn).scrap_apps_if_not_scraped(
            apps_ids_int=all_apps_ids
        )
        
    def get_devs_apps_ids_int(
            self,
            devs_ids_int: list[int],
            include_concepts=False
    ):
        devs_ids_int_db = self.conn.values_arr_to_db_in(devs_ids_int, int_values=True)

        res = {}

        rows = self.conn.select_all(f"""
        SELECT da.dev_id_int, da.app_id_int
          FROM scraped_data.devs_apps da,
               scraped_data.apps_v a
         WHERE da.dev_id_int IN ({devs_ids_int_db})
           AND a.app_id_int = da.app_id_int
        """)
        for row in rows:
            dev_id_int = row["dev_id_int"]
            if dev_id_int not in res:
                res[dev_id_int] = []
            res[dev_id_int].append(row["app_id_int"])

        if include_concepts:
            user_dev_id_int = self.get_dev_id_int()
            if user_dev_id_int in devs_ids_int:
                rows_concepts = self.conn.select_all("""
                SELECT %s as dev_id_int, a.app_id_int
                  FROM scraped_data.apps_concepts a
                 WHERE a.user_id = %s
                """, [user_dev_id_int, self.user_id])
                for row in rows_concepts:
                    dev_id_int = row["dev_id_int"]
                    if dev_id_int not in res:
                        res[dev_id_int] = []
                    res[dev_id_int].append(row["app_id_int"])

        return res        
    
    def get_games_explorer_filter_apps(
            self,
            filters: list
    ):
        res = []

        for filter in filters:

            tags = []
            tags_query = ""

            if UserObfuscator.TAG_IDS_INT in filter:

                tags = filter[UserObfuscator.TAG_IDS_INT]

                if len(tags) > 0 and self.billing_db.is_module_locked(
                        UserBillingDbModel.BILLING_MODULE_INSIGHT
                ):
                    allowed_tags_def = self.billing_db.get_allowed_tags(
                        UserBillingDbModel.BILLING_MODULE_INSIGHT
                    )

                    if allowed_tags_def is not None:
                        tags_new = []
                        for it in allowed_tags_def:
                            if it["is_changeable"] == 1 and it[UserObfuscator.TAG_ID_INT] in tags:
                                tags_new.append(it[UserObfuscator.TAG_ID_INT])
                        tags = tags_new

                if len(tags) > 0:
                    tags_db = self.conn.values_arr_to_db_in(tags, int_values=True)
                    tags_query = f"""
                        INNER JOIN (
                         SELECT tb.app_id_int, tb.b as app_b, t.b as my_b
                           FROM platform.platform_values_tags_bin tb,
                                (
                                    SELECT BIT_OR(dtb.b) as b, COUNT(1) as cnt
                                      FROM platform.def_tags_bin dtb
                                     WHERE dtb.tag_id_int IN ({tags_db})
                                ) t
                          WHERE BIT_COUNT(tb.b & t.b) = t.cnt
                        ) t ON t.app_id_int = a.app_id_int
                        """

            prices_query = ""
            if "prices" in filter:
                def get_price_by_tier(tier: int):
                    for it in GamesDbModel.GAMES_EXPLORER_FILTER_PRICES:
                        if it["tier"] == tier:
                            return it
                    raise Exception(f"Price tier {tier} not found")

                tier_from = filter["prices"]["from"]
                tier_to = filter["prices"]["to"]
                tier_min = filter["prices"]["min"]
                tier_max = filter["prices"]["max"]
                tiers_q = []
                if tier_from != tier_min:
                    tiers_q.append(f" a.price >= {get_price_by_tier(tier_from)['from']} ")
                if tier_to != tier_max:
                    tiers_q.append(f" a.price < {get_price_by_tier(tier_to)['to']} ")
                if len(tiers_q) > 0:
                    prices_query = f" AND {' AND '.join(tiers_q)}"

            store_query = ""
            if "stores" in filter and len(filter["stores"]) > 0:
                arr = []
                for store in filter["stores"]:
                    arr.append(f"(a.store = {store})")
                if len(arr) > 0:
                    store_query = f" AND ({' OR '.join(arr)})"

            tiers_query = ""
            if "tier" in filter:
                tier_from = filter["tier"]["from"]
                tier_to = filter["tier"]["to"]
                tier_min = filter["tier"]["min"]
                tier_max = filter["tier"]["max"]
                tiers_q = []
                if tier_from != tier_min:
                    tiers_q.append(
                        f" tiers.tier >= {tier_from} AND a.store = tiers.store_id AND a.installs >= tiers.value_from AND a.installs < tiers.value_to ")
                if tier_to != tier_max:
                    tiers_q.append(
                        f" tiers.tier <= {tier_to} AND a.store = tiers.store_id AND a.installs >= tiers.value_from AND a.installs < tiers.value_to ")
                if len(tiers_q) > 0:
                    tiers_query = f" INNER JOIN app.def_sheet_platform_values_install_tiers tiers ON {' AND '.join(tiers_q)}"

            user_dev_id_int = self.get_dev_id_int()

            app_ids: list[int] = [-1]
            if "my_apps" in filter and filter["my_apps"]:
                my_apps = self.portal_db.get_my_apps()[0][UserObfuscator.APP_ID_INT]
                if len(my_apps) > 0:
                    app_ids = my_apps
                else:
                    app_ids = [-2]
            elif UserObfuscator.DEV_IDS_INT in filter and len(filter[UserObfuscator.DEV_IDS_INT]) > 0:
                apps_ids_per_dev_id = self.get_devs_apps_ids_int(
                    devs_ids_int=filter[UserObfuscator.DEV_IDS_INT],
                    include_concepts=True
                )
                tmp_app_ids: list[int] = []
                for dev_id_int in apps_ids_per_dev_id:
                    for app_id_int in apps_ids_per_dev_id[dev_id_int]:
                        if app_id_int not in tmp_app_ids:
                            tmp_app_ids.append(app_id_int)

                if len(tmp_app_ids) > 0:
                    app_ids = tmp_app_ids
                else:
                    app_ids = [-2]

            app_ids_db = self.conn.values_arr_to_db_in(app_ids, int_values=True)

            top_competitors = 100

            sorting_enum = ["INSTALLS", "SIMILARITY", "TAM", "GROWTH", "NOVELTY", "QUALITY"]
            sorting = sorting_enum[0]
            if "advanced_filter_data" in filter and filter["advanced_filter_data"] is not None:
                advanced_filter_data = filter["advanced_filter_data"]
                if "sorting" in advanced_filter_data and advanced_filter_data["sorting"] is not None:
                    sorting = advanced_filter_data["sorting"]
                    if sorting not in sorting_enum:
                        sorting = sorting_enum[0]
                if "top_competitors" in advanced_filter_data and advanced_filter_data["top_competitors"] is not None:
                    assert Utils.is_int(advanced_filter_data["top_competitors"])
                    top_competitors = advanced_filter_data["top_competitors"]
                    if top_competitors <= 0:
                        top_competitors = 1
                    elif top_competitors > 100:
                        top_competitors = 100

            similarity_q = ""
            if sorting == "SIMILARITY":
                if len(tags) > 0:
                    calc_conn = DbConnection()
                    calc_utils = CalcUtils(conn=calc_conn)
                    
                    calc_utils.recreate_table(
                        table_name="tmp_games_explorer_competitors_pool_w",
                        query=f"""
                                CREATE TABLE platform_values.x__table_name__x (
                                subcategory_int int unsigned not null,
                                competitors_pool_w int unsigned not null,
                                constraint x__table_name__x_pk
                                    primary key (subcategory_int)
                                )
                                SELECT DISTINCT 
                                       d.subcategory_int,
                                       d.competitors_pool_w
                                  FROM platform.def_tags d
                                """
                    )

                    query = f"""
                                CREATE TABLE platform_values.x__table_name__x (
                                    app_id_int int UNSIGNED NOT NULL,
                                    score int NOT NULL,
                                    CONSTRAINT x__table_name__x_pk 
                                        PRIMARY KEY (app_id_int)
                                )
                                SELECT z2.app_id_int, round(z2.score) as score
                      FROM (
                    SELECT z1.app_id_int, z1.installs,
                    SUM(CAST(z1.score1 as SIGNED) - CAST((z1.score2 + z1.score3) as SIGNED)) AS score
                    FROM (

                    SELECT pool.app_id_int, pool.installs,   
                           3 * BIT_COUNT(pool.app_b & pool.my_b & d.b) * pool_w.competitors_pool_w AS score1,
                           BIT_COUNT(pool.app_b & d.b) * pool_w.competitors_pool_w AS score2,
                           BIT_COUNT(pool.my_b & d.b) * pool_w.competitors_pool_w AS score3
                      FROM (
                            SELECT a.app_id_int, a.installs, t.app_b, t.my_b
                              FROM platform.platform_values_apps a
                             {tags_query}
                             WHERE (a.app_id_int in ({app_ids_db}) OR -1 IN ({app_ids_db}))
                            ) pool,
                    platform.def_tags_bin d,
                    app.def_sheet_platform_product p,
                    platform_values.tmp_games_explorer_competitors_pool_w pool_w
                    WHERE p.tag_id_int = d.tag_id_int
                    AND pool_w.subcategory_int = p.subcategory_int
                    ) z1
                    GROUP BY z1.app_id_int
                    ) z2
                    ORDER BY z2.score DESC, z2.installs DESC
                    """

                    # todo add survey w
                    calc_utils.recreate_table(
                        table_name="tmp_games_explorer_similarity_pool",
                        query=query
                    )

                    calc_conn.close()

                    similarity_q = """ 
                    INNER JOIN platform_values.tmp_games_explorer_similarity_pool pool
                    ON pool.app_id_int = da.app_id_int
                    """

            sorting_q = ""
            if sorting == "INSTALLS":
                sorting_q = "p.installs DESC"
            elif sorting == "GROWTH":
                sorting_q = "p.growth DESC"
            elif sorting == "NOVELTY":
                sorting_q = "p.released_years ASC"
            elif sorting == "QUALITY":
                sorting_q = "p.rating DESC"
            elif sorting == "TAM":
                sorting_q = "tam.tam DESC"
            elif sorting == "SIMILARITY" and similarity_q != "":
                sorting_q = "pool.score DESC"

            sorting_q = f"{sorting_q}{'' if sorting_q == '' else ','} p.installs DESC"

            final_apps = []

            query_apps = f"""
                SELECT a.app_id_int,
                       a.title,
                       a.icon,
                       '{GamesDbModel.APP_TYPE__STORE}' as app_type,
                       a.store,
                       a.app_id_in_store,
                       IF(d.dev_id_int is null, dc.title, d.title) as dev_title,
                       p.growth,
                       p.rating,
                       p.installs,
                       p.released_years,
                       tam.tam
                  FROM scraped_data.devs_apps da
                  INNER JOIN scraped_data.apps_v a ON da.app_id_int = a.app_id_int
                  AND (a.app_id_int in ({app_ids_db}) OR -1 IN ({app_ids_db}))
                  INNER JOIN platform.platform_values_apps p ON da.app_id_int = p.app_id_int
                  {similarity_q}
                  LEFT JOIN platform.audience_angle_tam_per_app tam
                    ON tam.app_id_int = da.app_id_int
                    AND tam.angle_cnt = 1
                 LEFT JOIN scraped_data.devs d ON d.dev_id_int = da.dev_id_int
                 LEFT JOIN scraped_data.devs_concepts dc ON dc.dev_id_int = da.dev_id_int
                  {tags_query}
                  {tiers_query}
                  WHERE da.primary_dev = 1

                    {prices_query}
                    {store_query}
                  ORDER BY {sorting_q}
                  LIMIT {top_competitors}
                """

            rows_apps = self.conn.select_all(query_apps)

            for i in range(len(rows_apps)):
                rows_apps[i]["item"] = {
                    "type": 0,
                    "id": self.user_db.obfuscator.server_to_client(rows_apps[i][UserObfuscator.APP_ID_INT])
                }
                final_apps.append(rows_apps[i])

            query_apps_concepts = f"""
                            SELECT a.app_id_int,
                                   a.title,
                                   IF(i.app_id_int IS NULL, '', '[LOCAL_ICON_URL]') as icon,
                                   '{GamesDbModel.APP_TYPE__CONCEPT}' as app_type,
                                   a.store,
                                   a.app_id_in_store,
                                   IF(d.dev_id_int is null, dc.title, d.title) as dev_title
                              FROM scraped_data.apps_concepts a
                              LEFT JOIN scraped_data.apps_icons i ON i.app_id_int = a.app_id_int
                             LEFT JOIN scraped_data.devs d ON d.dev_id_int = {user_dev_id_int}
                             LEFT JOIN scraped_data.devs_concepts dc ON dc.dev_id_int = {user_dev_id_int}
                              {tags_query}
                              WHERE (a.app_id_int in ({app_ids_db}) OR -1 IN ({app_ids_db}))
                              AND a.user_id = {self.user_id}
                                {store_query}
                              ORDER BY a.app_id_int DESC
                              LIMIT 100
                            """

            rows_apps_concepts = self.conn.select_all(query_apps_concepts)

            for i in range(len(rows_apps_concepts)):
                rows_apps_concepts[i]["item"] = {
                    "type": 1,
                    "id": self.user_db.obfuscator.server_to_client(rows_apps_concepts[i][UserObfuscator.APP_ID_INT])
                }
                final_apps.append(rows_apps_concepts[i])

            res.append({
                "id": filter["id"],
                "apps": final_apps
            })

        return {
            "state": "ok",
            "data": res
        }
    
    
    def get_games_explorer_compare_apps_data(
            self,
            app_ids_int: list[int]
    ):
        if len(app_ids_int) <= 0:
            return []

        app_ids_int_db = self.conn.values_arr_to_db_in(app_ids_int, int_values=True)

        user_dev_id_int = self.get_dev_id_int()

        rows_apps = self.conn.select_all(f"""
        SELECT a.app_id_int,
               a.store,
               a.installs,
               scraped_data.get_app_growth(a.app_id_int) as growth,
               a.rating,
               a.title,
               a.icon,
               a.app_id_in_store,
               IF(d.dev_id_int IS NULL, dc.title, d.title) as dev_title,
               d.dev_id_int,
               YEAR(a.released) as released_year,
               UNIX_TIMESTAMP(a.scraped_t) as scraped_t,
               UNIX_TIMESTAMP(ts.t) as tagged_t,
               a.app_type
          FROM (
                     SELECT a.app_id_int, a.title, a.icon, a.store, a.app_id_in_store, a.released, 
                     a.price, a.installs, a.scraped_t, a.rating, da.dev_id_int, 
                     '{GamesDbModel.APP_TYPE__STORE}' as app_type
                       FROM scraped_data.devs_apps da,
                            scraped_data.apps_v a
                      WHERE da.app_id_int = a.app_id_int
                       AND da.primary_dev = 1
                       AND a.app_id_int IN ({app_ids_int_db})
                       AND a.released IS NOT NULL
                       UNION ALL
                    SELECT a.app_id_int, a.title, IF(i.app_id_int IS NULL, '', '[LOCAL_ICON_URL]') as icon, a.store, a.app_id_in_store, NOW() as released, 
                    0 as price, 0 as installs, NOW() as scrapred_t, 0 as rating, 
                    {user_dev_id_int} as dev_id_int, '{GamesDbModel.APP_TYPE__CONCEPT}' as app_type
                       FROM scraped_data.apps_concepts a
                       LEFT JOIN scraped_data.apps_icons i ON i.app_id_int = a.app_id_int
                      WHERE a.user_id = {self.user_id}
                        AND a.app_id_int IN ({app_ids_int_db})
                 ) a
            LEFT JOIN scraped_data.devs d ON d.dev_id_int = a.dev_id_int
            LEFT JOIN scraped_data.devs_concepts dc ON dc.dev_id_int = a.dev_id_int
          LEFT JOIN tagged_data.platform_tagged ts ON ts.app_id_int = a.app_id_int
        """)

        rows_tam = self.conn.select_all(f"""
        SELECT tam.app_id_int, 
               MAX(tam.tam) as tam,
               MAX(tam.total_audience) as total_audience
          FROM platform.audience_angle_tam_per_app tam
         WHERE tam.app_id_int IN ({app_ids_int_db})
         GROUP BY tam.app_id_int
        """)

        # tam for concepts
        rows_apps_concepts = self.conn.select_all(f"""
        SELECT a.app_id_int FROM scraped_data.apps_concepts a WHERE a.app_id_int IN ({app_ids_int_db})
        """)
        for row in rows_apps_concepts:
            app_id_int = row[UserObfuscator.APP_ID_INT]
            rows_tags = self.conn.select_all("""
            SELECT t.tag_id_int FROM tagged_data.tags_v t WHERE t.app_id_int = %s
            AND t.tag_rank != 0
            ORDER BY t.tag_id_int
            """, [app_id_int])

            audience_angle_ids = []
            for i in range(len(rows_tags)):
                if rows_tags[i][UserObfuscator.TAG_ID_INT] not in audience_angle_ids:
                    audience_angle_ids.append(rows_tags[i][UserObfuscator.TAG_ID_INT])
                for j in range(i + 1, len(rows_tags)):
                    aa_id = CalcUtils.create_audience_angle_2_comb_id(
                        rows_tags[i][UserObfuscator.TAG_ID_INT],
                        rows_tags[j][UserObfuscator.TAG_ID_INT]
                    )
                    if aa_id not in audience_angle_ids:
                        audience_angle_ids.append(aa_id)

            if len(audience_angle_ids) > 0:
                row_tam = self.conn.select_one_or_none(f"""
                SELECT a.tam as tam, a.total_audience as total_audience
                FROM platform.audience_angle_tam_per_angle a
                WHERE a.id IN ({self.conn.values_arr_to_db_in(audience_angle_ids, int_values=True)})
                ORDER BY a.tam DESC
                LIMIT 1
                """)

                if row_tam is not None:
                    rows_tam.append({
                        UserObfuscator.APP_ID_INT: app_id_int,
                        "tam": float(row_tam["tam"]),
                        "total_audience": float(row_tam["total_audience"])
                    })

        data_per_app = {}

        for row in rows_apps:
            data_per_app[row[UserObfuscator.APP_ID_INT]] = {
                "item": {
                    "type": 0,
                    "id": row[UserObfuscator.APP_ID_INT]
                },
                UserObfuscator.APP_ID_INT: row[UserObfuscator.APP_ID_INT],
                "store": row["store"],
                "installs": Utils.format_number(row["installs"]),
                "installs_raw": row["installs"],
                "growth": Utils.format_number(row["growth"]),
                "growth_raw": row["growth"],
                "rating": str(round(row["rating"] / 20, 2)),
                "rating_raw": round(row["rating"] / 20, 2),
                "tam": "",
                "tam_raw": 0,
                "total_audience": "",
                "total_audience_raw": 0,
                "tags": [],
                "title": row["title"],
                "icon": row["icon"],
                "app_id_in_store": row["app_id_in_store"],
                "dev_title": row["dev_title"],
                UserObfuscator.DEV_ID_INT: row[UserObfuscator.DEV_ID_INT],
                "released_year": str(row["released_year"]),
                "novelty_raw": row["released_year"],
                "scraped_t": row["scraped_t"],
                "tagged_t": row["tagged_t"],
                "app_type": row["app_type"]
            }

        for row in rows_tam:
            if row[UserObfuscator.APP_ID_INT] in data_per_app:
                d = data_per_app[row[UserObfuscator.APP_ID_INT]]
                d["tam"] = f"${Utils.format_number(float(row['tam']))}"
                d["tam_raw"] = float(row['tam'])
                d["total_audience"] = f"{Utils.format_number(float(row['total_audience']))}"
                d["total_audience_raw"] = float(row['total_audience'])

        query = f"""
        SELECT t.app_id_int, 
               p.tag_id_int,
               t.tag_rank
          FROM app.def_sheet_platform_product p
         INNER JOIN tagged_data.platform_tagged pt
            ON pt.app_id_int IN ({app_ids_int_db})
         INNER JOIN tagged_data.tags_v t
            ON t.app_id_int IN ({app_ids_int_db})
           AND t.tag_id_int = p.tag_id_int
           AND t.app_id_int = pt.app_id_int
         WHERE p.is_prompt = 1
           AND p.is_survey = 1
           AND (p.unlocked = 1 OR %s = 0)
        """

        rows_tags_apps = self.conn.select_all(query, [
            self.billing_db.is_module_locked(
                UserBillingDbModel.BILLING_MODULE_INSIGHT
            )
        ])

        for row in rows_tags_apps:
            if row[UserObfuscator.APP_ID_INT] in data_per_app:
                data_per_app[row[UserObfuscator.APP_ID_INT]]["tags"].append({
                    UserObfuscator.TAG_ID_INT: row[UserObfuscator.TAG_ID_INT],
                    "tag_rank": row["tag_rank"]
                })

        res = []
        for k in data_per_app:
            data_per_app[k]["item"]["id"] = self.user_db.obfuscator.server_to_client(
                data_per_app[k]["item"]["id"]
            )
            res.append(data_per_app[k])

        return res

    