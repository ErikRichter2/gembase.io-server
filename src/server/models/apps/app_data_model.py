from gembase_server_core.db.db_connection import DbConnection
from src.server.models.platform_values.cache.platform_values_cache import PlatformValuesCache
from src.utils.gembase_utils import GembaseUtils


class AppModelException(Exception):

    def __init__(self, error_code: str, error_msg: str, **kwargs):
        super(AppModelException, self).__init__()
        self.error_id = error_code
        self.error_msg = error_msg


class AppDataModel:
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

    @staticmethod
    def get_devs_details(conn: DbConnection, devs_ids_int: list[int]) -> dict | None:
        if len(devs_ids_int) <= 0:
            return None

        devs_ids_int_db = conn.values_arr_to_db_in(devs_ids_int, int_values=True)
        rows = conn.select_all(f"""
            SELECT d.dev_id_int, d.title, d.dev_id_in_store, d.store, 'store' as type
              FROM scraped_data.devs d
             WHERE d.dev_id_int IN ({devs_ids_int_db})
             UNION ALL
            SELECT dd.dev_id_int, dd.title, NULL as dev_id_in_store, 0 as store, 'concept' as type
              FROM scraped_data.devs_concepts dd
             WHERE dd.dev_id_int IN ({devs_ids_int_db})
            """)

        res = {}
        for row in rows:
            row["dev_store_url"] = AppDataModel.get_dev_store_url(
                dev_id_in_store=row["dev_id_in_store"],
                store=row["store"]
            )
            res[row["dev_id_int"]] = row

        return res

    @staticmethod
    def get_dev_store_url(dev_id_in_store: str, store: int) -> str:
        dev_store_url = ""

        if store == AppDataModel.STORE__STEAM:
            dev_id_in_store = dev_id_in_store.replace("steam__", "")

        if store == AppDataModel.STORE__GOOGLE_PLAY:
            if "developer?id" in dev_id_in_store or "dev?id" in dev_id_in_store:
                dev_store_url = f"https://play.google.com/store/apps/{dev_id_in_store}&hl=en"
            else:
                if GembaseUtils.int_safe(dev_id_in_store) == 0:
                    dev_store_url = f"https://play.google.com/store/apps/developer?id={dev_id_in_store}&hl=en"
                else:
                    dev_store_url = f"https://play.google.com/store/apps/dev?id={dev_id_in_store}&hl=en"
        elif store == AppDataModel.STORE__STEAM:
            if "/" in dev_id_in_store:
                dev_store_url = f"https://store.steampowered.com/{dev_id_in_store}"
            else:
                dev_store_url = f"https://store.steampowered.com/developer/{dev_id_in_store}"

        return dev_store_url

    @staticmethod
    def get_platform_for_store(store: int) -> int:
        if store == AppDataModel.STORE__CONCEPT:
            return AppDataModel.PLATFORM__MOBILE
        elif store == AppDataModel.STORE__GOOGLE_PLAY:
            return AppDataModel.PLATFORM__MOBILE
        elif store == AppDataModel.STORE__STEAM:
            return AppDataModel.PLATFORM__PC

        raise Exception(f"Unknown store {store}")

    @staticmethod
    def get_dev_id_int(conn: DbConnection, dev_id: str) -> int | None:
        row = conn.select_one_or_none("""
            SELECT id
              FROM app.map_dev_id
             WHERE dev_id = %s
            """, [dev_id.lower()])
        if row is None:
            return None
        return row["id"]

    @staticmethod
    def get_dev_id_in_store(conn: DbConnection, dev_id_int: int) -> str | None:
        row = conn.select_one_or_none("""
                SELECT dev_id_in_store
                  FROM scraped_data.devs
                 WHERE dev_id_int = %s
                """, [dev_id_int])
        if row is None:
            return None
        return row["dev_id"]

    @staticmethod
    def create_next_id_atomic():
        conn = DbConnection()
        app_id_int = conn.select_one("""
                SELECT app.get_id_from_sequence() as next_id
                """)["next_id"]
        conn.commit()
        conn.close()
        return app_id_int

    @staticmethod
    def get_app_id_int(conn: DbConnection, app_id_in_store: str) -> int | None:
        row = conn.select_one_or_none("""
                SELECT id
                  FROM app.map_app_id
                 WHERE app_id = %s
                """, [app_id_in_store.lower()])
        if row is None:
            return None
        return row["id"]

    @staticmethod
    def get_or_create_app_id_int(
            conn: DbConnection,
            app_id_in_store: str,
            store: int
    ) -> int:
        app_id = app_id_in_store.lower()
        app_id_int = AppDataModel.get_app_id_int(conn, app_id_in_store)

        if app_id_int is None:
            next_id = AppDataModel.create_next_id_atomic()

            row = conn.select_one_or_none("""
                SELECT id
                  FROM app.map_app_id
                 WHERE app_id = %s
                """, [app_id])

            if row is None:
                app_id_int = next_id
                conn.query("""
                    INSERT INTO app.map_app_id (id, app_id) VALUES (%s, %s)
                    """, [app_id_int, app_id])
                conn.query("""
                    INSERT INTO app.map_app_id_to_store_id (app_id_int, store, app_id_in_store) 
                    VALUES (%s, %s, %s)
                    """, [app_id_int, store, app_id_in_store])
            else:
                app_id_int = row["id"]

        return app_id_int

    @staticmethod
    def get_or_create_dev_id_int(conn: DbConnection, dev_id: str) -> int:
        dev_id = dev_id.lower()
        dev_id_int = AppDataModel.get_dev_id_int(conn=conn, dev_id=dev_id)

        if dev_id_int is None:
            next_id = AppDataModel.create_next_id_atomic()

            row = conn.select_one_or_none("""
                SELECT id
                  FROM app.map_dev_id
                 WHERE dev_id = %s
                """, [dev_id])

            if row is None:
                dev_id_int = next_id
                conn.query("""
                    INSERT INTO app.map_dev_id (id, dev_id) VALUES (%s, %s)
                    """, [dev_id_int, dev_id])
            else:
                dev_id_int = row["id"]

        return dev_id_int

    @staticmethod
    def save_dev_to_db_atomic(dev_id: str, store: int, scraped_data: {}):
        conn = DbConnection()

        try:
            conn.query("""
                LOCK TABLE scraped_data.devs_devs WRITE,
                           scraped_data.devs_apps WRITE, 
                           scraped_data.devs WRITE,
                           scraped_data.devs_source_data WRITE,
                           scraped_data.apps WRITE,
                           app.map_dev_id WRITE,
                           app.map_app_id WRITE,
                           app.map_app_id_to_store_id WRITE
                """)

            dev_id_int = AppDataModel.get_or_create_dev_id_int(conn=conn, dev_id=dev_id)

            conn.query("""
            DELETE FROM scraped_data.devs_devs WHERE parent_dev_id_int = %s AND child_dev_id_int = %s
            """, [dev_id_int, dev_id_int])

            conn.query("""
            INSERT INTO scraped_data.devs_devs (parent_dev_id_int, child_dev_id_int) VALUES (%s, %s)
            """, [dev_id_int, dev_id_int])

            conn.query("""
                DELETE FROM scraped_data.devs
                 WHERE dev_id_int = %s
                """, [dev_id_int])

            conn.query("""
                INSERT INTO scraped_data.devs (scraped_t, scrap_request_t, dev_id_int, dev_id_in_store, title, store, url) 
                VALUES (NOW(), NOW(), %s, %s, %s, %s, %s)
                """, [dev_id_int, scraped_data["dev_id_in_store"], scraped_data["title"],
                      store, scraped_data["url"]])

            conn.query("""
                DELETE FROM scraped_data.devs_source_data
                 WHERE dev_id_int = %s
                """, [dev_id_int])

            for data_type in scraped_data["source_data"]:
                conn.query("""
                    INSERT INTO scraped_data.devs_source_data (dev_id_int, data_type, data) VALUES (%s, %s, %s)
                    """, [dev_id_int, data_type, GembaseUtils.compress(scraped_data["source_data"][data_type])])

            app_ids_int = []
            app_ids_in_store = []
            for app_id_in_store in scraped_data["dev_apps"]:
                app_id_int = AppDataModel.get_or_create_app_id_int(
                    conn=conn,
                    app_id_in_store=app_id_in_store,
                    store=store
                )
                app_ids_int.append(app_id_int)
                app_ids_in_store.append(app_id_in_store)
                row = conn.select_one_or_none("""
                    SELECT dev_id_int
                      FROM scraped_data.devs_apps
                     WHERE dev_id_int = %s
                       AND app_id_int = %s
                    """, [dev_id_int, app_id_int])
                if row is None:
                    conn.query("""
                        INSERT INTO scraped_data.devs_apps (dev_id_int, app_id_int, primary_dev) VALUES (%s, %s, 0)
                        """, [dev_id_int, app_id_int])

                row = conn.select_one_or_none("""
                    SELECT app_id_int
                      FROM scraped_data.apps
                     WHERE app_id_int = %s
                    """, [app_id_int])

                if row is None:
                    platform = AppDataModel.get_platform_for_store(store)
                    conn.query("""
                        INSERT INTO scraped_data.apps (app_id_int, app_id_in_store, title, platform, store) 
                        VALUES (%s, %s, %s, %s, %s)
                        """, [app_id_int, app_id_in_store, "(not_scraped)", platform, store])

            conn.unlock_tables()
        except Exception as err:
            conn.unlock_tables(commit=False)
            conn.close()
            raise err

        conn.close()

        return {
            "dev_id_int": dev_id_int,
            "app_ids_int": app_ids_int,
            "app_ids_in_store": app_ids_in_store
        }

    @staticmethod
    def log_scrap_error(
            conn: DbConnection,
            store: int,
            error_code: int,
            error_state: str,
            app_id_int: int | None = None,
            app_id_in_store: str | None = None,
            dev_id_in_store: str | None = None,
    ):
        conn.query("""
            INSERT INTO scraped_data.scrap_errors (app_id_int, app_id_in_store, dev_id_in_store, store, error_code, error_state) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """, [app_id_int, app_id_in_store, dev_id_in_store, store, error_code, error_state])

    @staticmethod
    def save_app_to_db_atomic(
            app_id_in_store: str,
            store: int,
            scraped_data: {}
    ):
        conn = DbConnection()

        try:
            conn.query("""
                LOCK TABLE scraped_data.apps WRITE, 
                           scraped_data.apps_source_data WRITE, 
                           scraped_data.devs_apps WRITE, 
                           scraped_data.devs WRITE,
                           app.map_dev_id WRITE,
                           app.map_app_id WRITE,
                           app.map_app_id_to_store_id WRITE,
                           scraped_data.apps_hist WRITE,
                           scraped_data.apps_gallery WRITE,
                           scraped_data.apps_store_tags WRITE,
                           scraped_data.def_store_tags WRITE,
                           scraped_data.apps_icons WRITE
                """)

            try:
                app_id_int = AppDataModel.get_or_create_app_id_int(
                    conn=conn,
                    app_id_in_store=app_id_in_store,
                    store=store
                )

            except Exception as err:
                conn.unlock_tables(commit=False)
                AppDataModel.log_scrap_error(
                    conn=conn,
                    store=store,
                    app_id_in_store=app_id_in_store,
                    error_code=1,
                    error_state=str(err)
                )
                conn.commit()
                raise AppModelException(error_code="app_id_in_store", error_msg=str(err))

            rows_current = conn.select_one_or_none("""
                SELECT title, description
                  FROM scraped_data.apps
                 WHERE app_id_int = %s
                """, [app_id_int])

            changed = 0

            if rows_current is not None:
                if not GembaseUtils.compare_str(rows_current["title"], scraped_data["title"]):
                    changed += AppDataModel.CHANGED_TITLE
                if not GembaseUtils.compare_str(rows_current["description"], scraped_data["description"]):
                    changed += AppDataModel.CHANGED_DESC

            conn.query("""
                DELETE FROM scraped_data.apps
                 WHERE app_id_int = %s
                """, [app_id_int])

            platform = AppDataModel.get_platform_for_store(store)

            conn.query("""
                        INSERT INTO scraped_data.apps (ratings, reviews, scraped_t, scraped_request, 
                        app_id_int, app_id_in_store, title, store, icon, description, rank_val, 
                        platform, installs, released, rating, price, initial_price, url,
                        ads, iap) 
                        VALUES (%s, %s, NOW(), NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        FROM_UNIXTIME(%s), %s, %s, %s, %s, %s, %s)
                        """, [scraped_data["ratings"], scraped_data["reviews"], app_id_int, app_id_in_store,
                              scraped_data["title"], store,
                              scraped_data["icon"], scraped_data["description"],
                              scraped_data["rank_val"], platform,
                              scraped_data["installs"],
                              scraped_data["released"], scraped_data["rating"],
                              scraped_data["price"], scraped_data["initial_price"], scraped_data["url"],
                              scraped_data["ads"], scraped_data["iap"]])

            conn.query("""
            DELETE FROM scraped_data.apps_store_tags WHERE app_id_int = %s
            """, [app_id_int])

            if "store_tags" in scraped_data:
                store_tags_ids = [store_tag.lower() for store_tag in scraped_data["store_tags"]]
                if store == AppDataModel.STORE__STEAM:
                    store_tags_ids = [f"steam__{store_tag_id}" for store_tag_id in store_tags_ids]

                existing_store_tags = []
                if len(store_tags_ids) > 0:
                    rows_new_tags = conn.select_all(f"""
                    SELECT store_tag
                      FROM scraped_data.def_store_tags
                     WHERE store_tag IN ({conn.values_arr_to_db_in(store_tags_ids)})
                    """)

                    existing_store_tags = [row["store_tag"] for row in rows_new_tags]

                bulk_data = []
                for store_tag in scraped_data["store_tags"]:
                    store_tag_id = store_tag.lower()
                    if store == AppDataModel.STORE__STEAM:
                        store_tag_id = f"steam__{store_tag_id}"
                    if store_tag_id not in existing_store_tags:
                        bulk_data.append((store, store_tag_id, store_tag))

                conn.bulk("""
                INSERT INTO scraped_data.def_store_tags (store, store_tag, store_tag_raw)
                VALUES (%s, %s, %s)
                """, bulk_data)

                bulk_data = [(app_id_int, store_tag) for store_tag in store_tags_ids]
                conn.bulk("""
                INSERT INTO scraped_data.apps_store_tags (app_id_int, store_tag_id)
                SELECT %s as app_id_int, id
                  FROM scraped_data.def_store_tags
                 WHERE store_tag = %s 
                """, bulk_data)

            conn.query("""
                DELETE FROM scraped_data.apps_gallery
                 WHERE app_id_int = %s
                """, [app_id_int])

            if "gallery" in scraped_data and len(scraped_data["gallery"]) > 0:
                bulk_data = []
                for i in range(len(scraped_data["gallery"])):
                    bulk_data.append((app_id_int, i, scraped_data["gallery"][i]))
                conn.bulk("""
                    INSERT INTO scraped_data.apps_gallery
                    (app_id_int, img_order, store_url)
                    VALUES (%s, %s, %s)
                    """, bulk_data)

            if "icon_bytes" in scraped_data and scraped_data["icon_bytes"] is not None:
                conn.query("delete from scraped_data.apps_icons where app_id_int = %s", [app_id_int])
                conn.query("insert into scraped_data.apps_icons (app_id_int, icon) VALUES (%s, %s)",
                           [app_id_int, scraped_data["icon_bytes"]])

            conn.query("""
                DELETE FROM scraped_data.apps_source_data
                 WHERE app_id_int = %s
                """, [app_id_int])

            for data_type in scraped_data["source_data"]:
                conn.query("""
                    INSERT INTO scraped_data.apps_source_data (app_id_int, data_type, data) VALUES (%s, %s, %s)
                    """, [app_id_int, data_type, GembaseUtils.compress(scraped_data["source_data"][data_type])])

            conn.query("""
                INSERT INTO scraped_data.apps_hist (app_id_int, score, installs, ratings, reviews, 
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

                try:
                    dev_id_int = AppDataModel.get_or_create_dev_id_int(
                        conn=conn,
                        dev_id=dev_id)
                except Exception as err:
                    conn.unlock_tables(commit=False)
                    AppDataModel.log_scrap_error(
                        conn=conn,
                        store=store,
                        dev_id_in_store=dev_id_in_store,
                        error_code=1,
                        error_state=str(err)
                    )
                    conn.commit()
                    raise AppModelException(error_code="dev_id_in_store", error_msg=str(err))

                dev_ids_int.append(dev_id_int)

                row = conn.select_one_or_none("""
                    SELECT dev_id_int
                      FROM scraped_data.devs_apps
                     WHERE dev_id_int = %s
                       AND app_id_int = %s
                    """, [dev_id_int, app_id_int])

                if row is None:
                    conn.query("""
                        INSERT INTO scraped_data.devs_apps (dev_id_int, app_id_int) 
                        VALUES (%s, %s)
                        """, [dev_id_int, app_id_int])

                dev_id_int = AppDataModel.get_or_create_dev_id_int(conn=conn, dev_id=dev_id)

                if i == 0:
                    primary_dev_id_int = dev_id_int

                row = conn.select_one_or_none("""
                    SELECT dev_id_int
                      FROM scraped_data.devs
                     WHERE dev_id_int = %s
                    """, [dev_id_int])

                if row is None:
                    conn.query("""
                        INSERT INTO scraped_data.devs (dev_id_int, dev_id_in_store, title, store) 
                        VALUES (%s, %s, %s, %s)
                        """, [dev_id_int, dev_id_in_store, "(not_scraped)", store])

                conn.unlock_tables()
                conn.commit()

            if primary_dev_id_int is not None:
                conn.query("""
                    UPDATE scraped_data.devs_apps
                       SET primary_dev = IF(dev_id_int = %s, 1, 0)
                     WHERE app_id_int = %s
                    """, [primary_dev_id_int, app_id_int])
                conn.commit()

            AppDataModel.update_loyalty_installs(conn, app_id_int)

            conn.unlock_tables()

            PlatformValuesCache.start_service_for_single_app(app_id_int=app_id_int)

        except Exception as err:
            conn.unlock_tables(commit=False)
            conn.close()
            raise err

        conn.close()

        return {
            "app_id_int": app_id_int,
            "dev_ids_int": dev_ids_int
        }

    @staticmethod
    def update_loyalty_installs(conn: DbConnection, app_id_int: int):
        conn.query("""
        UPDATE scraped_data.apps a
           SET a.loyalty_installs = platform.calc_loyalty_installs(a.installs, TIMESTAMPDIFF(MONTH , released, NOW()), 0.1),
               a.loyalty_installs_t = NOW()
          WHERE a.app_id_int = %s
            AND a.installs > 10000
            AND a.released is not null
        """, [app_id_int])

    @staticmethod
    def set_app_removed_from_store(conn: DbConnection, app_id_in_store: str, store: int):
        row = conn.select_one_or_none("""
        SELECT a.app_id_int, a.removed_from_store
          FROM app.map_app_id_to_store_id m,
               scraped_data.apps a
         WHERE m.app_id_in_store = %s
           AND m.store = %s
           AND m.app_id_int = a.app_id_int
        """, [app_id_in_store, store])
        if row is not None and row["removed_from_store"] is not None:
            app_id_int = row["app_id_int"]
            conn.query("""
            UPDATE scraped_data.apps
               SET removed_from_store = NOW()
             WHERE app_id_int = %s
            """, [app_id_int])

    @staticmethod
    def get_app_icon_bytes(conn: DbConnection, app_id_int: int) -> str | None:
        row = conn.select_one_or_none("""
        SELECT icon FROM scraped_data.apps_icons WHERE app_id_int = %s
        """, [app_id_int])

        if row is None:
            return None

        icon_bytes = row["icon"]
        return icon_bytes

    @staticmethod
    def is_dev_concept(conn: DbConnection, dev_id_int: int) -> bool:
        row = conn.select_one_or_none("""
                SELECT 1
                  FROM scraped_data.devs_concepts a
                 WHERE a.dev_id_int = %s
                """, [dev_id_int])

        if row is None:
            return False

        return True
