from gembase_server_core.db.db_connection import DbConnection
from src.server.models.platform_values.cache.platform_values_cache import PlatformValuesCache


class PlatformValuesCron:

    @staticmethod
    def d(conn: DbConnection, update_progress=None):
        platform_values_cache = PlatformValuesCache(conn=conn, update_progress=update_progress)
        platform_values_cache.process()
        platform_values_cache.clear_old_result_data()
        conn.commit()
