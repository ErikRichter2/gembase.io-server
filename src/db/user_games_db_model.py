from server_core.utils.utils import Utils
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.games_db_model import GamesDbModel
from src.db.user_billing_db_model import UserBillingDbModel
from src.db.user_db_model import UserDbModel


class UserGamesDbModel(BaseUserDbModel):
    
    def __init__(self, user_db: UserDbModel):
        super(UserGamesDbModel, self).__init__(user_db=user_db)
    
    def save_concept_app(
            self,
            app_id_int: int,
            app_detail_changes: dict,
            files: list[object]
    ):
        if self.user_db.user_billing_db.is_module_locked(UserBillingDbModel.BILLING_MODULE_AUDITOR):
            raise Exception(f"Cannot save concept in demo")

        row = self.conn.select_one_or_none("""
        SELECT 1
          FROM scraped_data.apps_concepts a
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
                DELETE FROM scraped_data.apps_gallery ag
                WHERE ag.app_id_int = %s
                  AND ag.id = %s
                """, bulk_data)

        if len(files) > 0:
            max_order = 1
            row_max_order = self.conn.select_one_or_none("""
            SELECT MAX(img_order) as max_order
              FROM scraped_data.apps_gallery
             WHERE app_id_int = %s
            """, [app_id_int])

            if row_max_order["max_order"] is not None:
                max_order = row_max_order["max_order"] + 1

            for file in files:
                mime = file.content_type
                dms_row = DmsModel.save_file_to_dms(
                    conn=self.conn,
                    mime=mime,
                    file_type="image",
                    is_binary=True,
                    file=file,
                    user_id=self.user_id
                )

                self.conn.query("""
                INSERT INTO scraped_data.apps_gallery
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
        DELETE FROM scraped_data.apps_icons
         WHERE app_id_int = %s
        """, [app_id_int])

        if icon_bytes is not None:
            self.conn.query("""
            INSERT INTO scraped_data.apps_icons (app_id_int, icon) 
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

        self.user_db.user_portal_db.add_app_to_my_apps(
            app_id_int=app_id_int,
            unlocked_in_demo=True
        )

        app_detail = self.get_app_detail(
            app_id_int=app_id_int
        )

        return app_detail            
    
    def get_gallery(
            self,
            apps_ids_int: list[int],
            user_id: int
    ):
        res: list[dict] = []

        if len(apps_ids_int) == 0:
            return res

        app_ids_int_db = self.conn.values_arr_to_db_in(apps_ids_int, int_values=True)

        rows = self.conn.select_all(f"""
        SELECT ag.id, ag.app_id_int, ag.img_order, ag.store_url, dms.guid
          FROM scraped_data.apps_gallery ag
         inner join (
             SELECT a.app_id_int
               FROM scraped_data.apps a
             WHERE a.app_id_int IN ({app_ids_int_db})
             UNION
             SELECT ac.app_id_int
               FROM scraped_data.apps_concepts ac
             WHERE ac.app_id_int IN ({app_ids_int_db})
             AND ac.user_id = %s
         ) a ON a.app_id_int = ag.app_id_int
         LEFT JOIN app.dms dms
         ON dms.id = ag.dms_id
        """, [user_id])

        urls_per_app = {}
        for row in rows:
            app_id_int = row["app_id_int"]
            if app_id_int not in urls_per_app:
                d = {
                    "app_id_int": app_id_int,
                    "gallery": []
                }
                urls_per_app[app_id_int] = d
                res.append(d)
            img_data = {
                "id": row["id"],
                "img_order": row["img_order"]
            }
            dms_guid = row["guid"]
            if dms_guid is not None:
                img_data["dms_guid"] = dms_guid
            else:
                img_data["store_url"] = row["store_url"]
            d = urls_per_app[app_id_int]
            d["gallery"].append(img_data)

        return res    

    def get_tags_details_bulk(self, apps_ids_int: list[int]):

        app_ids_db = self.conn.values_arr_to_db_in(apps_ids_int, int_values=True)

        rows = self.conn.select_all("""
        SELECT a.app_id_int,
               a.tag_id_int, 
               a.tag_rank
          FROM tagged_data.tags_v a 
         WHERE a.app_id_int IN (%s)
         ORDER BY a.app_id_int, a.tag_rank
        """, [app_ids_db])

        res = {}
        for app_id_int in apps_ids_int:
            res[app_id_int] = []
        for row in rows:
            res[row["app_id_int"]].append({
                "tag_id_int": row["tag_id_int"],
                "tag_rank": row["tag_rank"]
            })

        return res

    def get_app_detail(
            self,
            app_id_int: int,
            include_gallery=False,
            include_tags=True,
    ) -> dict:
        return self.get_app_detail_bulk(
            apps_ids_int=[app_id_int],
            include_gallery=include_gallery,
            include_tags=include_tags,            
        )[0]
        
    def get_app_detail_bulk(
            self,
            apps_ids_int: list[int],
            user_id: int=-1,
            include_gallery=False,
            remove_desc=False,
            remove_from_app_ids_if_not_found=False,
            include_removed_from_store=False,
            include_tags=False,
    ):
        if len(apps_ids_int) == 0:
            return {}

        app_ids_db = self.conn.values_arr_to_db_in(apps_ids_int, int_values=True)

        app_details: dict[int, dict] = {}
        apps_found = []

        desc_query = "" if remove_desc else "a.description,"

        rows = self.conn.select_all(f"""
        SELECT {desc_query} a.app_id_int, a.app_id_in_store, a.title, a.icon, a.store, 
        a.platform, a.installs, a.rating, scraped_data.get_app_tier(a.app_id_int) as tier,
        d.dev_id_int, d.title as dev_title, d.dev_id_in_store, m.app_id_in_store as app_id_in_store_raw,
        UNIX_TIMESTAMP(a.scraped_t) as scraped_t, UNIX_TIMESTAMP(pt.t) as tagged_t,
        scraped_data.get_app_growth(a.app_id_int) as growth,
        YEAR(a.released) as released_year, tam.tam
          FROM scraped_data.apps a
         INNER JOIN app.map_app_id_to_store_id m
            ON m.app_id_int = a.app_id_int
         INNER JOIN scraped_data.devs_apps da
            ON da.app_id_int = a.app_id_int
           AND da.primary_dev = 1
         INNER JOIN scraped_data.devs d
            ON d.dev_id_int = da.dev_id_int
          LEFT JOIN tagged_data.platform_tagged pt
            ON pt.app_id_int = a.app_id_int
          LEFT JOIN platform.platform_values_apps p
            ON p.app_id_int = a.app_id_int
          LEFT JOIN platform.audience_angle_tam_per_app tam
            ON tam.app_id_int = a.app_id_int
           AND tam.angle_cnt = 1
         WHERE a.app_id_int IN ({app_ids_db})
           AND (a.removed_from_store IS NULL OR {1 if include_removed_from_store else 0} = 1)
         """)

        for row in rows:
            app_id_int = row["app_id_int"]
            apps_found.append(app_id_int)
            app_details[app_id_int] = self.get_app_detail.get_app_detail_data_from_db_row(row, GamesDbModel.APP_TYPE__STORE)

        rows = self.conn.select_all(f"""
        SELECT {desc_query} a.app_id_int, a.app_id_in_store, a.title, IF(i.app_id_int IS NULL, '', '[LOCAL_ICON_URL]') as icon, a.store, 
        a.platform, 0 as installs, 
        0 as rating, a.concept_counter, a.app_id_in_store as app_id_in_store_raw,
        UNIX_TIMESTAMP(ts.t) as tagged_t, a.tagged_by_user
          FROM scraped_data.apps_concepts a
          LEFT JOIN tagged_data.platform_tagged ts
            ON ts.app_id_int = a.app_id_int
          LEFT JOIN scraped_data.apps_icons i
          ON i.app_id_int = a.app_id_int
         WHERE a.app_id_int IN ({app_ids_db})
           AND (a.user_id = %s or %s = -1)
        """, [user_id, user_id])

        for row in rows:
            app_id_int = row["app_id_int"]
            apps_found.append(app_id_int)
            app_details[app_id_int] = self.get_app_detail_data_from_db_row(row, GamesDbModel.APP_TYPE__CONCEPT)

        if remove_from_app_ids_if_not_found:
            apps_ids_int.clear()
            for it in apps_found:
                apps_ids_int.append(it)

        if include_gallery:
            gallery = self.get_gallery(
                apps_ids_int=apps_ids_int,
                user_id=user_id
            )
            for it in gallery:
                for app_id_int in app_details:
                    if it["app_id_int"] == app_id_int:
                        app_details[app_id_int]["gallery"] = it["gallery"]

        if include_tags:
            tags_per_app_id_int = self.get_tags_details_bulk(apps_ids_int=apps_ids_int)
            tagging_state_per_app_id_int = self.get_tagging_state_bulk(
                apps_ids_int=apps_ids_int,
                user_id=user_id
            )
            for app_id_int in tags_per_app_id_int:
                if app_id_int in app_details:
                    app_details[app_id_int]["tags"] = tags_per_app_id_int[app_id_int]
            for app_id_int in tagging_state_per_app_id_int:
                if app_id_int in app_details:
                    app_details[app_id_int]["tagging_state"] = tagging_state_per_app_id_int[app_id_int]

        return app_details    

    def update_concept_app(self, app_id_int: int, app_detail_changes: dict):

        current_app_detail = self.get_app_detail(app_id_int=app_id_int)

        for p in ["title", "description", "store", "platform"]:
            if p in app_detail_changes:
                current_app_detail[p] = app_detail_changes[p]

        self.conn.query("""
        UPDATE scraped_data.apps_concepts a
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
            
    def set_manual_tags(self, app_id_int: int, tags_details: []):

        if not self.is_concept(app_id_int):
            raise Exception(f"Setting tags allowed only for concept app")

        self.set_tags(
            app_id_int=app_id_int,
            tags_details=tags_details,
            context="concept_manual_tags",
            manual=True
        )

        return self.get_tags(user_id, app_id_int)
    
    def is_concept(self, app_id_int: int, check_owner: int | None = None) -> bool:
        row = self.conn.select_one_or_none("""
        SELECT a.app_id_int, 
               a.user_id
          FROM scraped_data.apps_concepts a
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
        SELECT 1 FROM tagged_data.tags_override_from_users_apps WHERE app_id_int = %s
        """, [app_id_int]) is not None else 0

        hist_id = self.conn.insert("""
        INSERT INTO tagged_data.platform_tagged_history (app_id_int, context)
        VALUES (%s, %s)
        """, [app_id_int, context])
        self.conn.query("""
        INSERT INTO tagged_data.tags_history (hist_id, prompt_row_id, app_id_int, tag_id_int, tag_rank,
        added_from_store, removed_from_store, is_tag_rank_override, tag_rank_override)
        SELECT %s as hist_id, t.prompt_row_id, t.app_id_int, t.tag_id_int, t.tag_rank,
        t.added_from_store, t.removed_from_store, t.is_tag_rank_override, t.tag_rank_override
          FROM tagged_data.tags t
         WHERE t.app_id_int = %s
        """, [app_id_int, hist_id])

        if remove_tags is not None:
            remove_tags_db = self.conn.values_arr_to_db_in(remove_tags, int_values=True)
            self.conn.query(f"""
            DELETE FROM tagged_data.tags t
             WHERE t.app_id_int = %s
               AND t.tag_id_int IN ({remove_tags_db})
            """, [app_id_int])
        else:
            self.conn.query("""
            DELETE FROM tagged_data.tags t
             WHERE t.app_id_int = %s
            """, [app_id_int])

        self.conn.query("""
        DELETE FROM tagged_data.platform_tagged t
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
            INSERT INTO tagged_data.tags (app_id_int, tag_id_int, prompt_row_id, tag_rank, 
            added_from_store, removed_from_store, is_tag_rank_override, tag_rank_override, overriden_by_user) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, q_data)

        self.conn.query("""
        INSERT INTO tagged_data.platform_tagged (app_id_int, manual, prompts_b)
        SELECT %s as app_id_int, %s as manual, b.b
          FROM tagged_data.active_prompts_b b
        """, [app_id_int, manual])

        PlatformValuesDbModel(conn=self.conn).recreate_single_app_tags(
            user_id=self.user_id,
            app_id_int=app_id_int
        )
       
    def create_concept_app(
            self,
            app_id_int: int,
            app_detail_changes: dict,
            concept_counter: int
    ):
        self.conn.query("""
        INSERT INTO scraped_data.apps_concepts (app_id_int, app_id_in_store, user_id, title, description, icon, copied_from_app_id_int, platform, concept_counter) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [app_id_int, Utils.get_guid(), self.user_id, "Concept", "", "", app_id_int, GamesDbModel.PLATFORM__MOBILE, concept_counter])

        if app_detail_changes is not None:
            self.update_concept_app(
                app_id_int=app_id_int,
                app_detail_changes=app_detail_changes,
            )

        return app_id_int   
    
    def can_create_concept(self) -> bool:
        if self.user_db.user_billing_db.is_module_locked(
            module_id=UserBillingDbModel.BILLING_MODULE_AUDITOR
        ):
            if self.get_concepts_counter() >= 2:
                return False
        return True        
    
    def get_concepts_counter(self) -> int:
        return self.conn.select_one("""
        SELECT concepts_counter 
          FROM app.users 
         WHERE id = %s
        """, [self.user_id])["concepts_counter"]
        
    def set_concepts_counter(self, value: int):
        self.conn.query("""
        UPDATE app.users
           SET concepts_counter = %s
         WHERE id = %s
        """, [value, self.user_id])                  