from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.environment.runtime_constants import rr
from src.server.models.apps.app_data_model import AppModelException
from src.server.models.apps.app_model import AppModel
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.utils.gembase_utils import GembaseUtils


def __process_missing_primary_dev(conn: DbConnection) -> bool:

    conn.rollback()

    row = conn.select_one_or_none("""
    SELECT a.app_id_int, m.app_id_in_store, m.store
          FROM scraped_data.apps a,
               app.map_app_id_to_store_id m
         WHERE a.app_id_int = m.app_id_int
           AND a.scraped_t IS NOT null
           AND DATE_ADD(a.scraped_request, INTERVAL 2 WEEK ) < NOW()
           AND NOT EXISTS (
             SELECT 1
               FROM scraped_data.devs_apps da,
                    scraped_data.devs d
              WHERE da.app_id_int = a.app_id_int
                AND da.primary_dev = 1
                AND d.dev_id_int = da.dev_id_int
                AND d.scraped_t IS NOT NULL
         )
         ORDER BY a.scraped_request
         LIMIT 1
    """)

    if row is not None:

        app_id_int = row["app_id_int"]
        app_id_in_store = row["app_id_in_store"]

        conn.query("""
            UPDATE scraped_data.apps
               SET scraped_request = NOW()
             WHERE app_id_int = %s
            """, [app_id_int])

        conn.commit()

        GembaseUtils.log_service(f"Scrap {app_id_in_store} - {app_id_int}: missing primary dev")

        ScraperModel.scrap_app(
            conn=conn,
            app_id_in_store=app_id_in_store,
            store=row["store"]
        )

        GembaseUtils.log_service(f"Scrap {app_id_in_store} - {app_id_int}: DONE")

        return True

    return False


def __process_loyalty_installs():
    conn = DbConnection()
    AppModel.update_loyalty_installs_bulk(conn)
    conn.close()


def __process_from_batch(conn: DbConnection):
    conn.rollback()

    row = conn.select_one_or_none("""
    SELECT app_id_in_store, store FROM scraped_data.apps_to_scrap_batch
    WHERE state = 'queue'
    LIMIT 1
    """)

    if row is None:
        return False

    conn.query("""
    UPDATE scraped_data.apps_to_scrap_batch
    SET state = 'working'
    WHERE app_id_in_store = %s
    AND store = %s
    """, [row["app_id_in_store"], row["store"]])

    conn.commit()

    try:
        scraped_data = ScraperModel.scrap_app(
            conn=conn,
            app_id_in_store=row["app_id_in_store"],
            store=row["store"]
        )
        if scraped_data["state"] != 1:
            conn.query("""
                UPDATE scraped_data.apps_to_scrap_batch
                SET state = 'not_found'
                WHERE app_id_in_store = %s
                AND store = %s
                """, [row["app_id_in_store"], row["store"]])
        else:
            conn.query("""
                            DELETE FROM scraped_data.apps_to_scrap_batch
                            WHERE app_id_in_store = %s
                            AND store = %s
                            """, [row["app_id_in_store"], row["store"]])

    except AppModelException as err:
        if err.error_id == "dev_id_in_store" or err.error_id == "app_id_in_store":
            pass
        else:
            raise err

    conn.commit()
    return True


def __process_auto_queue(conn: DbConnection):

    conn.rollback()

    row = conn.select_one_or_none("""
        SELECT d.dev_id_int, m.dev_id, d.dev_id_in_store, d.store
          FROM scraped_data.devs d, app.map_dev_id m
         WHERE d.scrap_request_t IS NULL
         AND m.id = d.dev_id_int
        ORDER BY d.dev_id_int DESC
        LIMIT 1
        """)

    if row is None:
        row = conn.select_one_or_none("""
                SELECT d.dev_id_int, d.dev_id_in_store, d.store, m.dev_id
                  FROM scraped_data.devs d, app.map_dev_id m
                 WHERE d.scrap_request_t IS NOT NULL
                   AND DATE_ADD(d.scrap_request_t, INTERVAL 4 WEEK) < NOW()
                   AND m.id = d.dev_id_int
                ORDER BY d.scrap_request_t, d.dev_id_int DESC
                LIMIT 1
                """)

    if row is not None:
        conn.query("""
            UPDATE scraped_data.devs
               SET scrap_request_t = NOW()
             WHERE dev_id_int = %s
            """, [row["dev_id_int"]])

        conn.commit()

        dev_id_int = row["dev_id_int"]
        dev_id_in_store = row["dev_id_in_store"]

        GembaseUtils.log_service(f"Scrap DEV {dev_id_in_store} - {dev_id_int}: auto queue")

        try:
            conn.rollback()
            ScraperModel.scrap_dev(
                conn=conn,
                dev_id_in_store=row["dev_id_in_store"],
                store=row["store"]
            )
        except AppModelException as err:
            if err.error_id == "dev_id_in_store" or err.error_id == "app_id_in_store":
                pass
            else:
                raise err

        conn.commit()
        GembaseUtils.log_service(f"Scrap DEV {dev_id_in_store} - {dev_id_int}: DONE")

        return True

    row = conn.select_one_or_none("""
    SELECT a.app_id_int, a.app_id_in_store, a.store
      FROM scraped_data.apps a
     WHERE a.scraped_request IS NULL
    ORDER BY a.loyalty_installs DESC
    LIMIT 1
    """)

    if row is None:
        row = conn.select_one_or_none("""
        SELECT a.app_id_int, a.app_id_in_store, a.store
          FROM scraped_data.apps a
         WHERE a.scraped_request IS NOT NULL
           AND DATE_ADD(a.scraped_request, INTERVAL 4 WEEK) < NOW()
        ORDER BY a.scraped_request, a.loyalty_installs DESC
        LIMIT 1
        """)

    if row is None:
        return False

    conn.query("""
    UPDATE scraped_data.apps
       SET scraped_request = NOW()
     WHERE app_id_int = %s
    """, [row["app_id_int"]])

    conn.commit()

    app_id_int = row["app_id_int"]
    app_id_in_store = row["app_id_in_store"]

    GembaseUtils.log_service(f"Scrap {app_id_in_store} - {app_id_int}: auto queue")

    try:
        conn.rollback()
        ScraperModel.scrap_app(
            conn=conn,
            app_id_in_store=row["app_id_in_store"],
            store=row["store"]
        )
    except AppModelException as err:
        if err.error_id == "dev_id_in_store" or err.error_id == "app_id_in_store":
            pass
        else:
            raise err

    conn.commit()
    GembaseUtils.log_service(f"Scrap {app_id_in_store} - {app_id_int}: DONE")

    return True


def process_queue():

    conn = ServiceWrapperModel.create_conn()

    while True:
        conn.rollback()
        if not rr.is_prod():
            if not rr.is_debug():
                break
        if not __process_missing_primary_dev(conn):
            if not __process_auto_queue(conn):
                if not __process_from_batch(conn):
                    break

    ServiceWrapperModel.close_conn(
        conn_id=conn.connection_id(),
        conn=conn
    )


def default_method(*args, **kwargs):
    process_queue()
