from typing import Optional

import flask

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel
from src.db.calc_db_model import CalcDbModel
from src.db.quota.base_quota_context import BaseQuotaContext
from src.models.scraper.store_scrapers.google_play_scraper_model import GooglePlayScraperModel
from src.models.scraper.store_scrapers.steam_scraper_model import SteamScraperModel


class GamesDbModel(BaseDbModel):
    
    APP_TYPE__STORE = "store"
    APP_TYPE__CONCEPT = "concept"

    STORE__UNKNOWN = -1
    STORE__CONCEPT = 0
    STORE__GOOGLE_PLAY = 1
    STORE__STEAM = 2

    PLATFORM__MOBILE = 1
    PLATFORM__PC = 2

    CHANGED_TITLE = 1
    CHANGED_DESC = 2    

    TAG_RANK_NONE = 0
    TAG_RANK_PRIMARY = 1
    TAG_RANK_SECONDARY = 2
    TAG_RANK_TERTIARY = 3
    
    TAGGING_CONTEXT_ENUM = ["auditor", "games_explorer"]

    SUBCATEGORY_GENRE = "Genre"
    SUBCATEGORY_TOPICS = "Topics"
    SUBCATEGORY_BEHAVIORS = "Behaviors"
    SUBCATEGORY_NEEDS = "Needs"

    SUBCATEGORY_BEHAVIORS_ID = 4
    SUBCATEGORY_GENRE_ID = 21
    SUBCATEGORY_TOPICS_ID = 43
    SUBCATEGORY_NEEDS_ID = 27
    SUBCATEGORY_CORE_ID = 10
    SUBCATEGORY_ACTIVITIES_ID = 1
    SUBCATEGORY_PLATFORMS_ID = 29
    SUBCATEGORY_SOCIALS_ID = 39
    SUBCATEGORY_HOBBIES_ID = 22
    SUBCATEGORY_MOVIES_ID = 25
    SUBCATEGORY_ENTITIES_ID = 14
    SUBCATEGORY_ERAS_ID = 16
    SUBCATEGORY_ENVIRONMENT_ID = 15
    SUBCATEGORY_ROLES_ID = 37
    SUBCATEGORY_DOMAINS_ID = 12
    SUBCATEGORY_FOCUS_ID = 18
    SUBCATEGORY_MULTIPLAYER_ID = 26,
    SUBCATEGORY_COMPLEXITY_ID = 6,

    BEHAVIOR_EXPLORE = 522
    BEHAVIOR_UTILIZE = 519
    BEHAVIOR_IMPROVISE = 521
    BEHAVIOR_PLAN = 164
    BEHAVIOR_DESTROY = 173
    BEHAVIOR_CULTIVATE = 520
    BEHAVIOR_CHALLENGE = 525
    BEHAVIOR_BOND = 524
    BEHAVIOR_EXPRESS = 168
    BEHAVIOR_IMMERSE = 523

    TAG_TOPICS_REALITY = 234
    TAG_TOPICS_FANTASY = 233
    TAG_GENRE_SIMULATION = 28
    TAG_GENRE_STORY = 25

    ALLOWED_RANKED_SUBCATEGORIES = [SUBCATEGORY_GENRE_ID, SUBCATEGORY_TOPICS_ID]

    ALLOWED_TAG_RANKS = [TAG_RANK_NONE, TAG_RANK_PRIMARY, TAG_RANK_SECONDARY, TAG_RANK_TERTIARY]

    GAMES_EXPLORER_FILTER_STORES = [
        {"id": STORE__GOOGLE_PLAY, "label": "Google Play"},
        {"id": STORE__STEAM, "label": "Steam"}
    ]

    GAMES_EXPLORER_FILTER_PRICES = [
        {"tier": 1, "from": 0, "to": 1, "label": "$0 - $1"},
        {"tier": 2, "from": 2, "to": 3, "label": "$2 - $3"},
        {"tier": 3, "from": 4, "to": 5, "label": "$4 - $5"},
        {"tier": 4, "from": 6, "to": 10, "label": "$6 - $10"},
        {"tier": 5, "from": 11, "to": 15, "label": "$11 - $15"},
        {"tier": 6, "from": 16, "to": 20, "label": "$16 - $20"},
        {"tier": 7, "from": 21, "to": 30, "label": "$21 - $30"},
        {"tier": 8, "from": 31, "to": 40, "label": "$31 - $40"},
        {"tier": 9, "from": 41, "to": 50, "label": "$41 - $50"},
        {"tier": 10, "from": 51, "to": 100, "label": "$51 - $100"},
        {"tier": 11, "from": 101, "to": 999999, "label": "$100+"},
    ]    
        
    def __init__(self, conn: DbConnection):
        super(GamesDbModel, self).__init__(conn=conn)
    
    def get_app_detail_data_from_db_row(self, row, app_type):
        if "app_id_in_store" not in row:
            row["app_id_in_store"] = ""
        if "description" not in row:
            row["description"] = ""
        if "concept_counter" not in row:
            row["concept_counter"] = 0
        if "tier" not in row:
            row["tier"] = 0
        if "growth" not in row:
            row["growth"] = 0
        app_store_url = self.get_store_url(
            url=row["url"] if "url" in row else None,
            app_id_in_store=row["app_id_in_store_raw"],
            app_type=app_type,
            store=row["store"]
        )

        res = {
            "app_id_int": row["app_id_int"],
            'app_id_in_store': row["app_id_in_store"],
            'app_type': app_type,
            'title': row['title'],
            'icon': row['icon'],
            'description': row['description'],
            "store": row["store"],
            "platform": row["platform"],
            "installs": row["installs"],
            "rating": row["rating"],
            "concept_counter": row["concept_counter"],
            "tier": row["tier"],
            "growth": row["growth"],
            "app_store_url": app_store_url,
            "locked": False,
            "tagged_t": row["tagged_t"] if "tagged_t" in row else None,
            "scraped_t": row["scraped_t"] if "scraped_t" in row else None,
            "tagged_by_user": row["tagged_by_user"] if "tagged_by_user" in row else 0,
            "released_year": row["released_year"] if "released_year" in row else 0,
            "tam": row["tam"] if "tam" in row else 0,
            "tags": []
        }

        if "dev_id_int" in row:
            res["dev_id_int"] = row["dev_id_int"]
            if "dev_id_in_store" in row:
                res["dev_store_url"] = self.get_dev_store_url(
                    dev_id_in_store=row["dev_id_in_store"],
                    store=row["store"]
                )
        if "dev_title" in row:
            res["dev_title"] = row["dev_title"]

        return res
    
    def get_store_url(
            self,
            url: str | None,
            app_id_in_store: str,
            app_type: str,
            store: int) -> str:
        if url is not None:
            return url

        app_store_url = ""
        if store == GamesDbModel.STORE__STEAM:
            app_id_in_store = app_id_in_store.replace("steam__", "")
        if app_type != GamesDbModel.APP_TYPE__CONCEPT:
            if store == GamesDbModel.STORE__GOOGLE_PLAY:
                app_store_url = f"https://play.google.com/store/apps/details?id={app_id_in_store}&hl=en"
            elif store == GamesDbModel.STORE__STEAM:
                app_store_url = f"https://store.steampowered.com/app/{app_id_in_store}"
        else:
            app_store_url = app_id_in_store

        return app_store_url    
    
    def get_dev_store_url(self, dev_id_in_store: str, store: int) -> str:
        dev_store_url = ""

        if store == GamesDbModel.STORE__STEAM:
            dev_id_in_store = dev_id_in_store.replace("steam__", "")

        if store == GamesDbModel.STORE__GOOGLE_PLAY:
            if "developer?id" in dev_id_in_store or "dev?id" in dev_id_in_store:
                dev_store_url = f"https://play.google.com/store/apps/{dev_id_in_store}&hl=en"
            else:
                if Utils.int_safe(dev_id_in_store) == 0:
                    dev_store_url = f"https://play.google.com/store/apps/developer?id={dev_id_in_store}&hl=en"
                else:
                    dev_store_url = f"https://play.google.com/store/apps/dev?id={dev_id_in_store}&hl=en"
        elif store == GamesDbModel.STORE__STEAM:
            if "/" in dev_id_in_store:
                dev_store_url = f"https://store.steampowered.com/{dev_id_in_store}"
            else:
                dev_store_url = f"https://store.steampowered.com/developer/{dev_id_in_store}"

        return dev_store_url    
    
    

    def get_tagging_state_bulk(
            self,
            apps_ids_int: list[int],
            user_id: int
    ):
        data_per_app_id = {}

        rows_request = self.conn.select_all(f"""
        SELECT app_id_int, state, UNIX_TIMESTAMP(request_t) as request_t, 
               UNIX_TIMESTAMP(update_t) as update_t, progress,
               UNIX_TIMESTAMP(next_retry_t) as next_retry_t,
               error_data
          FROM gb_tagged_data.platform_tagging_request
         WHERE app_id_int IN ({self.conn.values_arr_to_db_in(apps_ids_int, int_values=True)})
        """)

        for row in rows_request:
            state = row["state"]
            retry_countdown = 0
            if state == "retry":
                retry_countdown = row["next_retry_t"] - Utils.timestamp_int()
            data_per_app_id[row["app_id_int"]] = {
                "state": row["state"],
                "request_t": row["request_t"],
                "update_t": row["update_t"],
                "progress": row["progress"],
                "retry_countdown": retry_countdown,
                "error_data": row["error_data"]
            }

        not_found = []
        for app_id_int in apps_ids_int:
            if app_id_int not in data_per_app_id:
                not_found.append(app_id_int)

        if len(not_found) > 0:
            rows_tagged = self.conn.select_all(f"""
                SELECT t.app_id_int, UNIX_TIMESTAMP(t.t) as tagged_t
                  FROM gb_tagged_data.platform_tagged t,
                       gb_tagged_data.active_prompts_b b
                 WHERE t.app_id_int IN ({self.conn.values_arr_to_db_in(not_found, int_values=True)})
                   AND t.prompts_b = b.b
                """)

            for row in rows_tagged:
                data_per_app_id[row["app_id_int"]] = {
                    "state": "done",
                    "tagged_t": row["tagged_t"]
                }

        for app_id_int in apps_ids_int:
            if app_id_int not in data_per_app_id:
                data_per_app_id[app_id_int] = {
                    "state": "not_tagged"
                }

        rows_pending_users_tags_override_request = self.conn.select_all(f"""
        SELECT DISTINCT app_id_int,
               FIRST_VALUE(state) over (PARTITION BY app_id_int ORDER BY request_id DESC) as state
          FROM gb_app.users_tags_override_requests
         WHERE user_id = %s 
           AND app_id_int IN ({self.conn.values_arr_to_db_in(apps_ids_int, int_values=True)})
        """, [user_id])

        for row in rows_pending_users_tags_override_request:
            data_per_app_id[row["app_id_int"]]["users_tags_override_request"] = {
                "state": row["state"]
            }

        return data_per_app_id
    
    def get_devs_details(self, devs_ids_int: list[int]) -> Optional[list[dict]]:
        if len(devs_ids_int) <= 0:
            return None

        devs_ids_int_db = self.conn.values_arr_to_db_in(devs_ids_int, int_values=True)
        rows = self.conn.select_all(f"""
            SELECT d.dev_id_int, d.title, d.dev_id_in_store, d.store, 'store' as type
              FROM gb_scraped_data.devs d
             WHERE d.dev_id_int IN ({devs_ids_int_db})
             UNION ALL
            SELECT dd.dev_id_int, dd.title, NULL as dev_id_in_store, 0 as store, 'concept' as type
              FROM gb_scraped_data.devs_concepts dd
             WHERE dd.dev_id_int IN ({devs_ids_int_db})
            """)

        res = {}
        for row in rows:
            row["dev_store_url"] = self.get_dev_store_url(
                dev_id_in_store=row["dev_id_in_store"],
                store=row["store"]
            )
            res[row["dev_id_int"]] = row

        res = Utils.dict_to_list(res)

        return res

    def scrap_apps_if_not_scraped(self, apps_ids_int: list[int]):
        for app_id_int in apps_ids_int:
            if not self.is_app_scraped(
                    app_id_int=app_id_int
            ):
                self.scrap_app_by_app_id(
                    app_id_int=app_id_int
                )

                ix = apps_ids_int.index(app_id_int)
                if ix < len(apps_ids_int) - 1:
                    return False

        return True
    
    def scrap_app_by_app_id(
            self,
            app_id_int: int
    ):
        row = self.conn.select_one_or_none("""
        SELECT app_id_in_store, store
          FROM gb_scraped_data.apps
         WHERE app_id_int = %s
        """, [app_id_int])

        if row is None:
            return None

        return self.scrap_app(
            app_id_in_store=row["app_id_in_store"],
            store=row["store"]
        )    
        
    def scrap_app(
            self,
            app_id_in_store: str,
            store: int,
            force=False
    ):
        app_id_int = self.get_app_id_int(
            app_id_in_store=app_id_in_store
        )

        if app_id_int is not None and self.is_app_scraped(
            app_id_int=app_id_int
        ):
            if not force:
                return

        if self.is_app_ignored(
            app_id_in_store=app_id_in_store,
            store=store
        ):
            return {
                "state": -3,
                "scraped_data": {
                    "error": "app_ignored",
                    "app_id_in_store": app_id_in_store,
                    "store": store
                }
            }

        if store == GamesDbModel.STORE__GOOGLE_PLAY:
            scraped_data = GooglePlayScraperModel.scrap_app(app_id_in_store)
        elif store == GamesDbModel.STORE__STEAM:
            scraped_data = SteamScraperModel.scrap_app(app_id_in_store)
        else:
            raise Exception(f"Unknown store: {store}")

        if scraped_data["state"] != 1:
            if scraped_data["state"] == -1:
                self.set_app_removed_from_store(
                    app_id_in_store=app_id_in_store,
                    store=store
                )
                self.conn.commit()
            else:
                self.log_scrap_error(
                    app_id_in_store=app_id_in_store,
                    store=store,
                    error_code=scraped_data["state"],
                    error_state=scraped_data["state_str"]
                )
            return {
                "state": scraped_data["state"],
                "scraped_data": scraped_data
            }

        self.conn.rollback()
        res = self.save_app_to_db_atomic(
            app_id_in_store=app_id_in_store,
            store=store,
            scraped_data=scraped_data)

        primary_dev_id_in_store = None
        if len(res["dev_ids_int"]) > 0:
            primary_dev_id_int = res["dev_ids_int"][0]
            primary_dev_id_in_store = scraped_data["dev_ids"][0]["dev_id_in_store"]

            if not self.is_dev_scraped(
                dev_id_int=primary_dev_id_int
            ):
                self.scrap_dev(
                    dev_id_in_store=primary_dev_id_in_store,
                    store=store
                )

        app_data = {
            "state": 1,
            "app_id_int": res["app_id_int"],
            "dev_ids_int": res["dev_ids_int"],
            "primary_dev_id_in_store": primary_dev_id_in_store
        }

        if app_data["state"] != 1:
            raise Exception(f"App {app_id_in_store} scrape error")

    def scrap_dev(
        self,
        dev_id_in_store: str,
        store: int,
        scrap_dev_apps=False
    ):
        
        scraped_data: dict = {}
        
        if store == GamesDbModel.STORE__GOOGLE_PLAY:
            scraped_data = GooglePlayScraperModel.scrap_dev(dev_id_in_store=dev_id_in_store)
        elif store == GamesDbModel.STORE__STEAM:
            scraped_data = SteamScraperModel.scrap_dev(dev_id_in_store=dev_id_in_store)
        else:
            raise Exception(f"Unknown store: {store}")

        if scraped_data["state"] != 1:
            error_state = ""
            if "state_str" in scraped_data:
                error_state = str(scraped_data["state_str"])
            GamesDbModel.log_scrap_error(
                dev_id_in_store=dev_id_in_store,
                store=store,
                error_code=scraped_data["state"],
                error_state=error_state
            )
            return {
                "state": scraped_data["state"],
                "scraped_data": scraped_data
            }

        if "title" not in scraped_data:
            raise Exception(f"Missing title in scraped data")
        if "source_data" not in scraped_data:
            raise Exception(f"Missing source_data in scraped data")

        dev_id = str(scraped_data["dev_id"])
        dev_id_int = self.get_or_create_dev_id_int(dev_id=dev_id)

        self.conn.query("""
        DELETE FROM gb_scraped_data.devs_devs WHERE parent_dev_id_int = %s AND child_dev_id_int = %s
        """, [dev_id_int, dev_id_int])

        self.conn.query("""
        INSERT INTO gb_scraped_data.devs_devs (parent_dev_id_int, child_dev_id_int) VALUES (%s, %s)
        """, [dev_id_int, dev_id_int])

        self.conn.query("""
            DELETE FROM gb_scraped_data.devs
                WHERE dev_id_int = %s
            """, [dev_id_int])

        self.conn.query("""
            INSERT INTO gb_scraped_data.devs (scraped_t, scrap_request_t, dev_id_int, dev_id_in_store, title, store, url) 
            VALUES (gb_app.now(), gb_app.now(), %s, %s, %s, %s, %s)
            """, [dev_id_int, scraped_data["dev_id_in_store"], scraped_data["title"],
                    store, scraped_data["url"]])

        self.conn.query("""
            DELETE FROM gb_scraped_data.devs_source_data
                WHERE dev_id_int = %s
            """, [dev_id_int])

        for data_type in scraped_data["source_data"]:
            self.conn.query("""
                INSERT INTO gb_scraped_data.devs_source_data (dev_id_int, data_type, data) VALUES (%s, %s, %s)
                """, [dev_id_int, data_type, Utils.compress(scraped_data["source_data"][data_type])])

        app_ids_int = []
        app_ids_in_store = []
        for app_id_in_store in scraped_data["dev_apps"]:
            app_id_int = self.get_or_create_app_id_int(
                app_id_in_store=app_id_in_store,
                store=store
            )
            app_ids_int.append(app_id_int)
            app_ids_in_store.append(app_id_in_store)
            row = self.conn.select_one_or_none("""
                SELECT dev_id_int
                    FROM gb_scraped_data.devs_apps
                    WHERE dev_id_int = %s
                    AND app_id_int = %s
                """, [dev_id_int, app_id_int])
            if row is None:
                self.conn.query("""
                    INSERT INTO gb_scraped_data.devs_apps (dev_id_int, app_id_int, primary_dev) VALUES (%s, %s, 0)
                    """, [dev_id_int, app_id_int])

            row = self.conn.select_one_or_none("""
                SELECT app_id_int
                    FROM gb_scraped_data.apps
                    WHERE app_id_int = %s
                """, [app_id_int])

            if row is None:
                platform = GamesDbModel.get_platform_for_store(store)
                self.conn.query("""
                    INSERT INTO gb_scraped_data.apps (app_id_int, app_id_in_store, title, platform, store) 
                    VALUES (%s, %s, %s, %s, %s)
                    """, [app_id_int, app_id_in_store, "(not_scraped)", platform, store])

        dev_detail = {
            "dev_id_int": dev_id_int,
            "app_ids_int": app_ids_int,
            "app_ids_in_store": app_ids_in_store
        }        

        if scrap_dev_apps:
            for app_ids_in_store in dev_detail["app_ids_in_store"]:
                self.scrap_app(
                    app_id_in_store=app_ids_in_store,
                    store=store
                )

        return {
            "state": 1,
            "dev_detail": dev_detail,
            "dev_id_int": dev_detail["dev_id_int"],
            "app_ids_int": dev_detail["app_ids_int"],
            "app_ids_in_store": dev_detail["app_ids_in_store"]
        }
        
    @staticmethod
    def log_scrap_error(
            store: int,
            error_code: int,
            error_state: str,
            app_id_int: int | None = None,
            app_id_in_store: str | None = None,
            dev_id_in_store: str | None = None,
    ):
        conn = DbConnection()
        conn.query("""
            INSERT INTO gb_scraped_data.scrap_errors (app_id_int, app_id_in_store, dev_id_in_store, store, error_code, error_state) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """, [app_id_int, app_id_in_store, dev_id_in_store, store, error_code, error_state])        
        conn.commit()
        conn.close()
        
    def get_or_create_dev_id_int(self, dev_id: str) -> int:
        dev_id = dev_id.lower()
        dev_id_int = self.get_dev_id_int(dev_id=dev_id)

        if dev_id_int is None:
            next_id = GamesDbModel.create_next_id_atomic()

            row = self.conn.select_one_or_none("""
                SELECT id
                  FROM gb_app.map_dev_id
                 WHERE dev_id = %s
                """, [dev_id])

            if row is None:
                dev_id_int = next_id
                self.conn.query("""
                    INSERT INTO gb_app.map_dev_id (id, dev_id) VALUES (%s, %s)
                    """, [dev_id_int, dev_id])
            else:
                dev_id_int = row["id"]

        return dev_id_int
    
    def get_dev_id_int(self, dev_id: str) -> int | None:
        row = self.conn.select_one_or_none("""
            SELECT id
              FROM gb_app.map_dev_id
             WHERE dev_id = %s
            """, [dev_id.lower()])
        if row is None:
            return None
        return row["id"]    
        
    @staticmethod
    def create_next_id_atomic():
        conn = DbConnection()
        app_id_int = conn.select_one("""
                SELECT gb_app.get_id_from_sequence() as next_id
                """)["next_id"]
        conn.commit()
        conn.close()
        return app_id_int        
    
    @staticmethod
    def get_platform_for_store(store: int) -> int:
        if store == GamesDbModel.STORE__CONCEPT:
            return GamesDbModel.PLATFORM__MOBILE
        elif store == GamesDbModel.STORE__GOOGLE_PLAY:
            return GamesDbModel.PLATFORM__MOBILE
        elif store == GamesDbModel.STORE__STEAM:
            return GamesDbModel.PLATFORM__PC

        raise Exception(f"Unknown store {store}")    
    
    def get_or_create_app_id_int(
            self,
            app_id_in_store: str,
            store: int
    ) -> int:
        app_id = app_id_in_store.lower()
        app_id_int = self.get_app_id_int(app_id_in_store)

        if app_id_int is None:
            next_id = GamesDbModel.create_next_id_atomic()

            row = self.conn.select_one_or_none("""
                SELECT id
                  FROM gb_app.map_app_id
                 WHERE app_id = %s
                """, [app_id])

            if row is None:
                app_id_int = next_id
                self.conn.query("""
                    INSERT INTO gb_app.map_app_id (id, app_id) VALUES (%s, %s)
                    """, [app_id_int, app_id])
                self.conn.query("""
                    INSERT INTO gb_app.map_app_id_to_store_id (app_id_int, store, app_id_in_store) 
                    VALUES (%s, %s, %s)
                    """, [app_id_int, store, app_id_in_store])
            else:
                app_id_int = row["id"]

        return app_id_int    
    
    def get_app_id_int(self, app_id_in_store: str) -> int | None:
        row = self.conn.select_one_or_none("""
                SELECT id
                  FROM gb_app.map_app_id
                 WHERE app_id = %s
                """, [app_id_in_store.lower()])
        if row is None:
            return None
        return row["id"]

    def get_app_devs(self, app_id_int: int) -> list[dict]:
        res = []
        rows = self.conn.select_all("""
        SELECT da.dev_id_int
          FROM gb_scraped_data.devs_apps da
         WHERE da.app_id_int = %s
        """, [app_id_int])
        for row in rows:
            res.append(row["dev_id_int"])
        return res

    def get_dev_apps(self, dev_id_int: int) -> list[dict]:
        res = []
        rows = self.conn.select_all("""
            SELECT da.app_id_int
              FROM gb_scraped_data.devs_apps da
             WHERE da.dev_id_int = %s
            """, [dev_id_int])
        for row in rows:
            res.append(row["app_id_int"])
        return res

    def is_dev_scraped(self, dev_id_int: int, days_since_last: int | None = None) -> bool:
        if self.is_dev_concept(dev_id_int=dev_id_int):
            return True
        days_q = ""
        if days_since_last is not None:
            days_q = f" AND DATE_ADD(d.scraped_t, INTERVAL {days_since_last} DAY) > gb_app.now() "
        row = self.conn.select_one_or_none(f"""
        SELECT d.dev_id_int
          FROM gb_scraped_data.devs d
         WHERE d.dev_id_int = %s
           AND d.scraped_t IS NOT NULL
           {days_q}
        """, [dev_id_int])
        return row is not None

    def is_app_scraped(self, app_id_int: int) -> bool:
        row = self.conn.select_one_or_none("""
        SELECT a.app_id_int
          FROM gb_scraped_data.apps a,
               gb_scraped_data.devs_apps da,
               gb_scraped_data.devs d
         WHERE a.app_id_int = %s
           AND a.scraped_t IS NOT NULL
           AND a.app_id_int = da.app_id_int
           AND da.primary_dev = 1
           AND da.dev_id_int = d.dev_id_int
        """, [app_id_int])

        if row is not None:
            return True

        row = self.conn.select_one_or_none("""
        SELECT a.app_id_int
          FROM gb_scraped_data.apps_concepts a
         WHERE a.app_id_int = %s
        """, [app_id_int])

        if row is not None:
            return True

        return False

    @staticmethod
    def get_dev_id_from_dev_id_in_store(dev_id_in_store: str, store: int) -> str:
        if store == GamesDbModel.STORE__STEAM:
            return SteamScraperModel.get_dev_id_from_dev_id_in_store(dev_id_in_store=dev_id_in_store)
        elif store == GamesDbModel.STORE__GOOGLE_PLAY:
            return GooglePlayScraperModel.get_dev_id_from_dev_id_in_store(dev_id_in_store=dev_id_in_store)
        return dev_id_in_store

    def is_app_ignored(
        self,
        app_id_in_store: str,
        store: int
    ):
        return self.conn.select_one_or_none(f"""
        SELECT 1
          FROM gb_scraped_data.ignored_apps ia
         WHERE INSTR(LOWER(%s), LOWER(ia.app_id_in_store)) > 0
           AND ia.store = %s
         LIMIT 1
        """, [app_id_in_store, store]) is not None

    def is_dev_concept(self, dev_id_int: int) -> bool:
        row = self.conn.select_one_or_none("""
                SELECT 1
                  FROM gb_scraped_data.devs_concepts a
                 WHERE a.dev_id_int = %s
                """, [dev_id_int])

        if row is None:
            return False

        return True
    
    def save_app_to_db_atomic(
            self,
            app_id_in_store: str,
            store: int,
            scraped_data: dict
    ):

        app_id_int = self.get_or_create_app_id_int(
            app_id_in_store=app_id_in_store,
            store=store
        )

        rows_current = self.conn.select_one_or_none("""
            SELECT title, description
                FROM gb_scraped_data.apps
                WHERE app_id_int = %s
            """, [app_id_int])

        changed = 0

        if rows_current is not None:
            if not Utils.compare_str(rows_current["title"], scraped_data["title"]):
                changed += GamesDbModel.CHANGED_TITLE
            if not Utils.compare_str(rows_current["description"], scraped_data["description"]):
                changed += GamesDbModel.CHANGED_DESC

        self.conn.query("""
            DELETE FROM gb_scraped_data.apps
                WHERE app_id_int = %s
            """, [app_id_int])

        platform = GamesDbModel.get_platform_for_store(store)

        self.conn.query("""
                    INSERT INTO gb_scraped_data.apps (ratings, reviews, scraped_t, scraped_request, 
                    app_id_int, app_id_in_store, title, store, icon, description, rank_val, 
                    platform, installs, released, rating, price, initial_price, url) 
                    VALUES (%s, %s, gb_app.now(), gb_app.now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    FROM_UNIXTIME(%s), %s, %s, %s, %s)
                    """, [scraped_data["ratings"], scraped_data["reviews"], app_id_int, app_id_in_store,
                            scraped_data["title"], store,
                            scraped_data["icon"], scraped_data["description"],
                            scraped_data["rank_val"], platform,
                            scraped_data["installs"],
                            scraped_data["released"], scraped_data["rating"],
                            scraped_data["price"], scraped_data["initial_price"], scraped_data["url"]])

        self.conn.query("""
        DELETE FROM gb_scraped_data.apps_store_tags WHERE app_id_int = %s
        """, [app_id_int])

        if "store_tags" in scraped_data:
            store_tags_ids = [store_tag.lower() for store_tag in scraped_data["store_tags"]]
            if store == GamesDbModel.STORE__STEAM:
                store_tags_ids = [f"steam__{store_tag_id}" for store_tag_id in store_tags_ids]

            existing_store_tags = []
            if len(store_tags_ids) > 0:
                rows_new_tags = self.conn.select_all(f"""
                SELECT store_tag
                    FROM gb_scraped_data.def_store_tags
                    WHERE store_tag IN ({self.conn.values_arr_to_db_in(store_tags_ids)})
                """)

                existing_store_tags = [row["store_tag"] for row in rows_new_tags]

            bulk_data = []
            for store_tag in scraped_data["store_tags"]:
                store_tag_id = store_tag.lower()
                if store == GamesDbModel.STORE__STEAM:
                    store_tag_id = f"steam__{store_tag_id}"
                if store_tag_id not in existing_store_tags:
                    bulk_data.append((store, store_tag_id, store_tag))

            self.conn.bulk("""
            INSERT INTO gb_scraped_data.def_store_tags (store, store_tag, store_tag_raw)
            VALUES (%s, %s, %s)
            """, bulk_data)

            bulk_data = [(app_id_int, store_tag) for store_tag in store_tags_ids]
            self.conn.bulk("""
            INSERT INTO gb_scraped_data.apps_store_tags (app_id_int, store_tag_id)
            SELECT %s as app_id_int, id
                FROM gb_scraped_data.def_store_tags
                WHERE store_tag = %s 
            """, bulk_data)

        self.conn.query("""
            DELETE FROM gb_scraped_data.apps_gallery
                WHERE app_id_int = %s
            """, [app_id_int])

        if "gallery" in scraped_data and len(scraped_data["gallery"]) > 0:
            bulk_data = []
            for i in range(len(scraped_data["gallery"])):
                bulk_data.append((app_id_int, i, scraped_data["gallery"][i]))
            self.conn.bulk("""
                INSERT INTO gb_scraped_data.apps_gallery
                (app_id_int, img_order, store_url)
                VALUES (%s, %s, %s)
                """, bulk_data)

        if "icon_bytes" in scraped_data and scraped_data["icon_bytes"] is not None:
            self.conn.query("delete from gb_scraped_data.apps_icons where app_id_int = %s", [app_id_int])
            self.conn.query("insert into gb_scraped_data.apps_icons (app_id_int, icon) VALUES (%s, %s)",
                        [app_id_int, scraped_data["icon_bytes"]])

        self.conn.query("""
            DELETE FROM gb_scraped_data.apps_source_data
                WHERE app_id_int = %s
            """, [app_id_int])

        for data_type in scraped_data["source_data"]:
            self.conn.query("""
                INSERT INTO gb_scraped_data.apps_source_data (app_id_int, data_type, data) VALUES (%s, %s, %s)
                """, [app_id_int, data_type, Utils.compress(scraped_data["source_data"][data_type])])

        self.conn.query("""
            INSERT INTO gb_scraped_data.apps_hist (app_id_int, score, installs, ratings, reviews, 
            changes, price, initial_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, [app_id_int, scraped_data["rating"], scraped_data["installs"],
                    scraped_data["ratings"], scraped_data["reviews"], changed,
                    scraped_data["price"], scraped_data["initial_price"]])

        dev_ids_int = []
        primary_dev_id_int = None

        for i in range(len(scraped_data["dev_ids"])):
            dev_id = scraped_data["dev_ids"][i]["dev_id"]
            dev_id_in_store = scraped_data["dev_ids"][i]["dev_id_in_store"]
            dev_id_int = self.get_or_create_dev_id_int(dev_id=dev_id)
            dev_ids_int.append(dev_id_int)

            row = self.conn.select_one_or_none("""
                SELECT dev_id_int
                    FROM gb_scraped_data.devs_apps
                    WHERE dev_id_int = %s
                    AND app_id_int = %s
                """, [dev_id_int, app_id_int])

            if row is None:
                self.conn.query("""
                    INSERT INTO gb_scraped_data.devs_apps (dev_id_int, app_id_int) 
                    VALUES (%s, %s)
                    """, [dev_id_int, app_id_int])

            dev_id_int = self.get_or_create_dev_id_int(dev_id=dev_id)

            if i == 0:
                primary_dev_id_int = dev_id_int

            row = self.conn.select_one_or_none("""
                SELECT dev_id_int
                    FROM gb_scraped_data.devs
                    WHERE dev_id_int = %s
                """, [dev_id_int])

            if row is None:
                self.conn.query("""
                    INSERT INTO gb_scraped_data.devs (dev_id_int, dev_id_in_store, title, store) 
                    VALUES (%s, %s, %s, %s)
                    """, [dev_id_int, dev_id_in_store, "(not_scraped)", store])

        if primary_dev_id_int is not None:
            self.conn.query("""
                UPDATE gb_scraped_data.devs_apps
                    SET primary_dev = IF(dev_id_int = %s, 1, 0)
                    WHERE app_id_int = %s
                """, [primary_dev_id_int, app_id_int])
            
        self.update_loyalty_installs(app_id_int)
        CalcDbModel(conn=self.conn).apps(
            app_ids_int=[app_id_int]
        )

        return {
            "app_id_int": app_id_int,
            "dev_ids_int": dev_ids_int
        }    
        
    def update_loyalty_installs(self, app_id_int: int):
        self.conn.query("""
        UPDATE gb_scraped_data.apps a
           SET a.loyalty_installs = gb_platform.calc_loyalty_installs(a.installs, TIMESTAMPDIFF(MONTH , released, gb_app.now()), 0.1),
               a.loyalty_installs_t = gb_app.now()
          WHERE a.app_id_int = %s
            AND a.installs > 10000
            AND a.released is not null
        """, [app_id_int])
                
    def update_loyalty_installs_bulk(self, only_where_null=False):
        if only_where_null:
            where_q = """
            loyalty_installs_t IS NULL
            """
        else:
            where_q = """
            (  
               loyalty_installs_t IS NULL 
            OR DATE_ADD(loyalty_installs_t, INTERVAL 1 MONTH ) <= gb_app.now()
            )
            """

        self.conn.query(f"""
        UPDATE gb_scraped_data.apps a
           SET a.loyalty_installs = gb_platform.calc_loyalty_installs(a.installs, TIMESTAMPDIFF(MONTH , released, gb_app.now()), 0.1),
               a.loyalty_installs_t = gb_app.now()
          WHERE a.installs > 10000
            AND a.released is not null
            AND {where_q}
        """)

    def set_app_removed_from_store(self, app_id_in_store: str, store: int):
        row = self.conn.select_one_or_none("""
        SELECT a.app_id_int, a.removed_from_store
          FROM gb_app.map_app_id_to_store_id m,
               gb_scraped_data.apps a
         WHERE m.app_id_in_store = %s
           AND m.store = %s
           AND m.app_id_int = a.app_id_int
        """, [app_id_in_store, store])
        if row is not None and row["removed_from_store"] is not None:
            app_id_int = row["app_id_int"]
            self.conn.query("""
            UPDATE gb_scraped_data.apps
               SET removed_from_store = gb_app.now()
             WHERE app_id_int = %s
            """, [app_id_int])
            
            

    def get_app_detail(
            self,
            app_id_int: int,
            user_id: int=-1,
            include_gallery=False,
            include_tags=True,
            remove_desc=False
    ) -> dict:
        return self.get_app_detail_bulk(
            apps_ids_int=[app_id_int],
            include_gallery=include_gallery,
            include_tags=include_tags, 
            user_id=user_id,
            remove_desc=remove_desc         
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
        a.platform, a.installs, a.rating, gb_scraped_data.get_app_tier(a.app_id_int) as tier,
        d.dev_id_int, d.title as dev_title, d.dev_id_in_store, m.app_id_in_store as app_id_in_store_raw,
        UNIX_TIMESTAMP(a.scraped_t) as scraped_t, UNIX_TIMESTAMP(pt.t) as tagged_t,
        gb_scraped_data.get_app_growth(a.app_id_int) as growth,
        YEAR(a.released) as released_year, tam.tam
          FROM gb_scraped_data.apps a
         INNER JOIN gb_app.map_app_id_to_store_id m
            ON m.app_id_int = a.app_id_int
         INNER JOIN gb_scraped_data.devs_apps da
            ON da.app_id_int = a.app_id_int
           AND da.primary_dev = 1
         INNER JOIN gb_scraped_data.devs d
            ON d.dev_id_int = da.dev_id_int
          LEFT JOIN gb_tagged_data.platform_tagged pt
            ON pt.app_id_int = a.app_id_int
          LEFT JOIN gb_platform.platform_values_apps p
            ON p.app_id_int = a.app_id_int
          LEFT JOIN gb_platform.audience_angle_tam_per_app tam
            ON tam.app_id_int = a.app_id_int
           AND tam.angle_cnt = 1
         WHERE a.app_id_int IN ({app_ids_db})
           AND (a.removed_from_store IS NULL OR {1 if include_removed_from_store else 0} = 1)
         """)
        
        games_db = GamesDbModel(conn=self.conn)

        for row in rows:
            app_id_int = row["app_id_int"]
            apps_found.append(app_id_int)
            app_details[app_id_int] = games_db.get_app_detail_data_from_db_row(row, GamesDbModel.APP_TYPE__STORE)

        rows = self.conn.select_all(f"""
        SELECT {desc_query} a.app_id_int, a.app_id_in_store, a.title, IF(i.app_id_int IS NULL, '', '[LOCAL_ICON_URL]') as icon, a.store, 
        a.platform, 0 as installs, 
        0 as rating, a.concept_counter, a.app_id_in_store as app_id_in_store_raw,
        UNIX_TIMESTAMP(ts.t) as tagged_t, a.tagged_by_user
          FROM gb_scraped_data.apps_concepts a
          LEFT JOIN gb_tagged_data.platform_tagged ts
            ON ts.app_id_int = a.app_id_int
          LEFT JOIN gb_scraped_data.apps_icons i
          ON i.app_id_int = a.app_id_int
         WHERE a.app_id_int IN ({app_ids_db})
           AND (a.user_id = %s or %s = -1)
        """, [user_id, user_id])

        for row in rows:
            app_id_int = row["app_id_int"]
            apps_found.append(app_id_int)
            app_details[app_id_int] = games_db.get_app_detail_data_from_db_row(row, GamesDbModel.APP_TYPE__CONCEPT)

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
            tagging_state_per_app_id_int = games_db.get_tagging_state_bulk(
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
          FROM gb_scraped_data.apps_gallery ag
         inner join (
             SELECT a.app_id_int
               FROM gb_scraped_data.apps a
             WHERE a.app_id_int IN ({app_ids_int_db})
             UNION
             SELECT ac.app_id_int
               FROM gb_scraped_data.apps_concepts ac
             WHERE ac.app_id_int IN ({app_ids_int_db})
             AND ac.user_id = %s
         ) a ON a.app_id_int = ag.app_id_int
         LEFT JOIN gb_app.dms dms
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
          FROM gb_tagged_data.tags_v a 
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
                
class TagsMapper:

    def __init__(self, conn: DbConnection):

        rows = conn.select_all("""
        SELECT p.tag_id_int, p.tag_id, p.node, p.subcategory_int, m.subcategory, p.is_prompt, p.adj
          FROM app.def_sheet_platform_product p,
               app.map_tag_subcategory m
         WHERE m.id = p.subcategory_int
        """)

        str_to_int = {}
        int_to_str = {}
        node_to_int_prompts = {}
        int_to_node = {}
        int_to_subc = {}
        int_to_subci = {}
        subci_to_int = {}
        i_2_def = {}

        for row in rows:
            i_2_def[row["tag_id_int"]] = row
            str_to_int[row["tag_id"]] = row["tag_id_int"]
            int_to_str[row["tag_id_int"]] = row["tag_id"]
            int_to_node[row["tag_id_int"]] = row["node"].strip().lower()

            if row["is_prompt"] == 1 and row["subcategory_int"] in [
                GamesDbModel.SUBCATEGORY_TOPICS_ID,
                GamesDbModel.SUBCATEGORY_GENRE_ID,
                GamesDbModel.SUBCATEGORY_BEHAVIORS_ID
            ]:
                node = row["node"].strip().lower()
                if node in node_to_int_prompts:
                    raise Exception(f"Prompt node '{node}' already exists")
                node_to_int_prompts[node] = row["tag_id_int"]

            int_to_subc[row["tag_id_int"]] = row["subcategory"].strip().lower()

            int_to_subci[row["tag_id_int"]] = row["subcategory_int"]
            if row["subcategory_int"] not in subci_to_int:
                subci_to_int[row["subcategory_int"]] = []
            subci_to_int[row["subcategory_int"]].append(row["tag_id_int"])

        map_tags = {
            "i2s": int_to_str,
            "s2i": str_to_int,
            "i2n": int_to_node,
            "i2subc": int_to_subc,
            "i2subci": int_to_subci,
            "subci2i": subci_to_int,
            "n2i_prompts": node_to_int_prompts,
            "i2def": i_2_def
        }

        self.map_tags = map_tags

    @staticmethod
    def instance(conn: DbConnection, flask_cache=False):
        if flask_cache:
            map_tags = getattr(flask.g, "map_tags", None)
            if map_tags is not None:
                return map_tags
            map_tags = TagsMapper(conn=conn).map_tags
            setattr(flask.g, "map_tags", map_tags)
            return map_tags
        else:
            return TagsMapper(conn=conn).map_tags            

