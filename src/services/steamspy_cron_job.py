import json
import time
from time import sleep

from gembase_server_core.db.db_connection import DbConnection
from src.external_api import steamspy_api
from src.server.models.apps.app_data_model import AppDataModel
from src.server.models.apps.app_model import AppModel
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.server.models.scraper.store_scrapers.steam_scraper_model import SteamScraperModel

from urllib.error import HTTPError

from src.utils.gembase_utils import GembaseUtils


log_data = ""


def get_steamspy_ccu_for_all_apps(conn: DbConnection, hist_id: int):

    idx = 0
    while True:
        try:
            all_data = steamspy_api.get_all_1000_owned(idx)
            if len(all_data) == 0:
                return
            for steam_id in all_data:
                app_id_in_store = SteamScraperModel.get_app_id_from_steam_id(steam_id)
                app_id_int = AppModel.get_or_create_app_id_int(
                    conn=conn,
                    app_id_in_store=app_id_in_store,
                    store=AppModel.STORE__STEAM
                )

                try:
                    data = all_data[steam_id]
                    owners_from, owners_to = SteamScraperModel.util_parse_steam_owners(data["owners"])
                    conn.query("""
                    DELETE FROM scraped_data.steamspy_all_apps_hist 
                     WHERE app_id_int = %s 
                       AND hist_id = %s
                    """, [app_id_int, hist_id])
                    conn.query("""
                    INSERT INTO scraped_data.steamspy_all_apps_hist (app_id_int, hist_id, ccu, owners_from, owners_to, positive, negative) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, [app_id_int, hist_id, data["ccu"], owners_from, owners_to, data["positive"], data["negative"]])
                    conn.commit()
                except Exception as err:
                    conn.query("""
                    INSERT INTO scraped_data.steamspy_all_apps_hist (app_id_int, hist_id, state) VALUES (%s, %s, %s)
                    """, [app_id_int, hist_id, -1])
                    conn.commit()
        except HTTPError:
            return
        idx += 1
        time.sleep(60)


def set_steamspy_hist_data(conn: DbConnection, app_id_int: int, app_id_in_store: str, hist_id: int):
    steam_app_id = SteamScraperModel.get_steam_id_from_app_id(app_id_in_store)
    steamspy_data = steamspy_api.get_app_details(steam_app_id)

    conn.query("""
    DELETE FROM scraped_data.steamspy_data_hist h 
     WHERE h.app_id_int = %s 
       AND h.hist_id = %s
    """, [app_id_int, hist_id])

    conn.query("""
    INSERT INTO scraped_data.steamspy_data_hist (app_id_int, hist_id, steamspy_data) 
    VALUES (%s, %s, %s)
    """, [app_id_int, hist_id, json.dumps(steamspy_data)])


def log(t: str, clear=False):
    global log_data
    if clear:
        log_data = t
    else:
        log_data += t
        log_data += "\n"
    GembaseUtils.log_service(log_data)


def default_method(*args, **kwargs):
    global log_data
    log_data = ""

    conn = ServiceWrapperModel.create_conn()

    log("STEAMSPY CRON JOB START")

    hist_id = conn.insert("""
    INSERT INTO scraped_data.steamspy_history 
    VALUES (DEFAULT, DEFAULT, DEFAULT)
    """)
    conn.commit()

    app_ids_to_scrap = []
    app_ids_in_store_to_scrap = []

    # steamspy
    log("get_top_100_in_2_weeks")
    data = steamspy_api.get_top_100_in_2_weeks()
    for steam_id in data:
        app_id_in_store = SteamScraperModel.get_app_id_from_steam_id(steam_id)
        app_id_int = AppModel.get_or_create_app_id_int(
            conn=conn,
            app_id_in_store=app_id_in_store,
            store=AppModel.STORE__STEAM
        )
        if app_id_int is not None:
            conn.query("""
            INSERT INTO scraped_data.steamspy_top_100_in_2_weeks (app_id_int, hist_id) VALUES (%s, %s)
            """, [app_id_int, hist_id])
            if app_id_int not in app_ids_to_scrap:
                app_ids_to_scrap.append(app_id_int)
                app_ids_in_store_to_scrap.append(app_id_in_store)
    conn.commit()

    sleep(5)

    log("get_top_100_forever")
    data = steamspy_api.get_top_100_forever()
    for steam_id in data:
        app_id_in_store = SteamScraperModel.get_app_id_from_steam_id(steam_id)
        app_id_int = AppModel.get_or_create_app_id_int(
            conn=conn,
            app_id_in_store=app_id_in_store,
            store=AppModel.STORE__STEAM
        )
        if app_id_int is not None:
            conn.query("""
            INSERT INTO scraped_data.steamspy_top_100_forever (app_id_int, hist_id) VALUES (%s, %s)
            """, [app_id_int, hist_id])
            if app_id_int not in app_ids_to_scrap:
                app_ids_to_scrap.append(app_id_int)
                app_ids_in_store_to_scrap.append(app_id_in_store)
    conn.commit()

    sleep(5)

    log("get_top_100_owned")
    data = steamspy_api.get_top_100_owned()
    for steam_id in data:
        app_id_in_store = SteamScraperModel.get_app_id_from_steam_id(steam_id)
        app_id_int = AppModel.get_or_create_app_id_int(
            conn=conn,
            app_id_in_store=app_id_in_store,
            store=AppModel.STORE__STEAM
        )
        if app_id_int is not None:
            conn.query("""
            INSERT INTO scraped_data.steamspy_top_100_owned (app_id_int, hist_id) VALUES (%s, %s)
            """, [app_id_int, hist_id])
            if app_id_int not in app_ids_to_scrap:
                app_ids_to_scrap.append(app_id_int)
                app_ids_in_store_to_scrap.append(app_id_in_store)
    conn.commit()

    # keep historic steamspy data for top100 apps
    log("set_steamspy_hist_data")
    for i in range(len(app_ids_to_scrap)):
        app_id_int = app_ids_to_scrap[i]
        app_id_in_store = app_ids_in_store_to_scrap[i]
        set_steamspy_hist_data(
            conn=conn,
            app_id_int=app_id_int,
            app_id_in_store=app_id_in_store,
            hist_id=hist_id
        )

        conn.commit()
        sleep(1)

    log("get_steamspy_ccu_for_all_apps")
    get_steamspy_ccu_for_all_apps(conn, hist_id)

    top_10k = conn.select_all("""
    SELECT m.app_id_in_store, h.app_id_int
      FROM scraped_data.steamspy_all_apps_hist h,
           app.map_app_id_to_store_id m
     WHERE h.hist_id = %s
       AND h.ccu > 0
       AND h.app_id_int = m.app_id_int
     ORDER BY h.ccu DESC
     LIMIT 10000
    """, [hist_id])

    for row in top_10k:
        if row["app_id_int"] not in app_ids_to_scrap:
            app_ids_to_scrap.append(row["app_id_int"])
            app_ids_in_store_to_scrap.append(row["app_id_in_store"])

    # refresh top10k apps
    log("refresh top10k apps")
    cnt_all = len(app_ids_to_scrap)
    cnt_scraped = 0

    for i in range(len(app_ids_to_scrap)):
        app_id_int = app_ids_to_scrap[i]
        app_id_in_store = app_ids_in_store_to_scrap[i]
        cnt_all -= 1

        if not ScraperModel.is_app_scraped(conn, app_id_int):
            try:
                ScraperModel.scrap_app(
                    conn=conn,
                    app_id_in_store=app_id_in_store,
                    store=AppModel.STORE__STEAM)
                conn.commit()
                cnt_scraped += 1
                time.sleep(2)
            except Exception as e:
                conn.rollback()
                AppDataModel.log_scrap_error(
                    conn=conn,
                    app_id_in_store=app_id_in_store,
                    store=AppModel.STORE__STEAM,
                    error_code=-1,
                    error_state=str(e)
                )
                conn.commit()

        log(f"scrap: {cnt_scraped}/{cnt_all}", True)

    conn.query("""
    UPDATE scraped_data.steamspy_history h
       SET h.state = 1
     WHERE h.id = %s
    """, [hist_id])

    conn.commit()

    conn.query("""
    UPDATE scraped_data.apps a
     INNER JOIN scraped_data.steamspy_all_apps_hist h ON h.app_id_int = a.app_id_int
       SET a.installs = round((h.owners_to + h.owners_from) / 2),
           a.loyalty_installs_t = NULL
     WHERE a.store = %s
       AND h.hist_id = %s
       AND a.installs != round((h.owners_to + h.owners_from) / 2)
    """, [AppModel.STORE__STEAM, hist_id])

    AppModel.update_loyalty_installs_bulk(
        conn=conn,
        only_where_null=True
    )

    conn.commit()

    ServiceWrapperModel.close_conn(conn.connection_id(), conn)
