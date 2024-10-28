from gembase_server_core.db.db_connection import DbConnection
from src.server.models.apps.app_data_model import AppDataModel
from src.server.models.scraper.store_scrapers.google_play_scraper_model import GooglePlayScraperModel
from src.server.models.scraper.store_scrapers.steam_scraper_model import SteamScraperModel


class ScraperModel:

    @staticmethod
    def get_app_devs(conn: DbConnection, app_id_int: int) -> []:
        res = []
        rows = conn.select_all("""
        SELECT da.dev_id_int
          FROM scraped_data.devs_apps da
         WHERE da.app_id_int = %s
        """, [app_id_int])
        for row in rows:
            res.append(row["dev_id_int"])
        return res

    @staticmethod
    def get_dev_apps(conn: DbConnection, dev_id_int: int) -> []:
        res = []
        rows = conn.select_all("""
            SELECT da.app_id_int
              FROM scraped_data.devs_apps da
             WHERE da.dev_id_int = %s
            """, [dev_id_int])
        for row in rows:
            res.append(row["app_id_int"])
        return res

    @staticmethod
    def is_dev_scraped(conn: DbConnection, dev_id_int: int, days_since_last: int | None = None) -> bool:
        if AppDataModel.is_dev_concept(conn=conn, dev_id_int=dev_id_int):
            return True
        days_q = ""
        if days_since_last is not None:
            days_q = f" AND DATE_ADD(d.scraped_t, INTERVAL {days_since_last} DAY) > NOW() "
        row = conn.select_one_or_none(f"""
        SELECT d.dev_id_int
          FROM scraped_data.devs d
         WHERE d.dev_id_int = %s
           AND d.scraped_t IS NOT NULL
           {days_q}
        """, [dev_id_int])
        return row is not None

    @staticmethod
    def is_app_scraped(conn: DbConnection, app_id_int: int) -> bool:
        row = conn.select_one_or_none("""
        SELECT a.app_id_int
          FROM scraped_data.apps a,
               scraped_data.devs_apps da,
               scraped_data.devs d
         WHERE a.app_id_int = %s
           AND a.scraped_t IS NOT NULL
           AND a.app_id_int = da.app_id_int
           AND da.primary_dev = 1
           AND da.dev_id_int = d.dev_id_int
        """, [app_id_int])

        if row is not None:
            return True

        row = conn.select_one_or_none("""
        SELECT a.app_id_int
          FROM scraped_data.apps_concepts a
         WHERE a.app_id_int = %s
        """, [app_id_int])

        if row is not None:
            return True

        return False

    @staticmethod
    def get_dev_id_from_dev_id_in_store(dev_id_in_store: str, store: int) -> str:
        if store == AppDataModel.STORE__STEAM:
            return SteamScraperModel.get_dev_id_from_dev_id_in_store(dev_id_in_store=dev_id_in_store)
        elif store == AppDataModel.STORE__GOOGLE_PLAY:
            return GooglePlayScraperModel.get_dev_id_from_dev_id_in_store(dev_id_in_store=dev_id_in_store)
        return dev_id_in_store

    @staticmethod
    def scrap_dev(
        conn: DbConnection,
        dev_id_in_store: str,
        store: int,
        scrap_dev_apps=False
    ):
        if store == AppDataModel.STORE__GOOGLE_PLAY:
            scraped_data = GooglePlayScraperModel.scrap_dev(dev_id_in_store=dev_id_in_store)
        elif store == AppDataModel.STORE__STEAM:
            scraped_data = SteamScraperModel.scrap_dev(dev_id_in_store=dev_id_in_store)
        else:
            raise Exception(f"Unknown store: {store}")

        if scraped_data["state"] != 1:
            error_state = ""
            if "state_str" in scraped_data:
                error_state = scraped_data["state_str"]
            AppDataModel.log_scrap_error(
                conn=conn,
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

        conn.rollback()
        dev_id = scraped_data["dev_id"]
        res = AppDataModel.save_dev_to_db_atomic(
            dev_id=dev_id,
            store=store,
            scraped_data=scraped_data)

        if scrap_dev_apps:
            for app_ids_in_store in res["app_ids_in_store"]:
                ScraperModel.scrap_app(
                    conn=conn,
                    app_id_in_store=app_ids_in_store,
                    store=store
                )

        return {
            "state": 1,
            "dev_detail": res,
            "dev_id_int": res["dev_id_int"],
            "app_ids_int": res["app_ids_int"],
            "app_ids_in_store": res["app_ids_in_store"]
        }

    @staticmethod
    def is_app_ignored(
        conn: DbConnection,
        app_id_in_store: str,
        store: int
    ):
        return conn.select_one_or_none(f"""
        SELECT 1
          FROM scraped_data.ignored_apps ia
         WHERE INSTR(LOWER(%s), LOWER(ia.app_id_in_store)) > 0
           AND ia.store = %s
         LIMIT 1
        """, [app_id_in_store, store]) is not None

    @staticmethod
    def scrap_app(
            conn: DbConnection,
            app_id_in_store: str,
            store: int
    ):
        if ScraperModel.is_app_ignored(
            conn=conn,
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

        if store == AppDataModel.STORE__GOOGLE_PLAY:
            scraped_data = GooglePlayScraperModel.scrap_app(app_id_in_store)
        elif store == AppDataModel.STORE__STEAM:
            scraped_data = SteamScraperModel.scrap_app(app_id_in_store)
        else:
            raise Exception(f"Unknown store: {store}")

        if scraped_data["state"] != 1:
            if scraped_data["state"] == -1:
                AppDataModel.set_app_removed_from_store(
                    app_id_in_store=app_id_in_store,
                    store=store,
                    conn=conn
                )
                conn.commit()
            else:
                AppDataModel.log_scrap_error(
                    conn=conn,
                    app_id_in_store=app_id_in_store,
                    store=store,
                    error_code=scraped_data["state"],
                    error_state=scraped_data["state_str"]
                )
            return {
                "state": scraped_data["state"],
                "scraped_data": scraped_data
            }

        conn.rollback()
        res = AppDataModel.save_app_to_db_atomic(
            app_id_in_store=app_id_in_store,
            store=store,
            scraped_data=scraped_data)

        primary_dev_id_in_store = None
        if len(res["dev_ids_int"]) > 0:
            primary_dev_id_int = res["dev_ids_int"][0]
            primary_dev_id_in_store = scraped_data["dev_ids"][0]["dev_id_in_store"]

            if not ScraperModel.is_dev_scraped(
                conn=conn,
                dev_id_int=primary_dev_id_int
            ):
                ScraperModel.scrap_dev(
                    conn=conn,
                    dev_id_in_store=primary_dev_id_in_store,
                    store=store
                )

        return {
            "state": 1,
            "app_id_int": res["app_id_int"],
            "dev_ids_int": res["dev_ids_int"],
            "primary_dev_id_in_store": primary_dev_id_in_store
        }
