import json

from src import external_api
from gembase_server_core.db.db_connection import DbConnection
from src.utils.gembase_utils import GembaseUtils


class GoogleSearchModel:

    @staticmethod
    def search(q: str, context, silent=False) -> {}:
        res = external_api.search(q, silent=silent)

        if res is None:
            return None

        conn = DbConnection()
        guid = GembaseUtils.get_guid()
        conn.query("""
        INSERT INTO audit.google_search (context, q, audit_guid) VALUES (%s, %s, %s)
        """, [json.dumps(context), q, guid])
        conn.commit()
        conn.close()

        return {
            "audit_guid": guid,
            "res": res
        }
