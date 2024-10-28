from gembase_server_core.db.db_connection import DbConnection
from src.server.models.dms.dms_constants import DmsConstants
from src.server.models.dms.dms_model import DmsCache


class SurveyConfigModel:

    CURRENT_SURVEY_VERSION = 3

    def __init__(self, conn: DbConnection):
        self.__conn = conn
        self.__config = DmsCache.get_json(conn=conn, guid=DmsConstants.survey_v2_config, create_copy=True)

    def get(self):

        self.__config["__version"] = SurveyConfigModel.CURRENT_SURVEY_VERSION

        rows_map_tags = self.__conn.select_all("""
        SELECT m.id, m.tag_id
          FROM app.map_tag_id m
        """)

        map_tags = {}
        for row in rows_map_tags:
            map_tags[row["tag_id"]] = row["id"]

        for page in ["genres", "topics", "needs", "behaviors"]:
            for row in self.__config[page]:
                if row["param"] == "item":
                    row["value"] = map_tags[row["value"]]

        app_ids = []
        for row in self.__config["apps"]:
            app_ids.append(row["id"])
        app_ids_db = self.__conn.values_arr_to_db_in(app_ids)
        rows_apps = self.__conn.select_all(f"""
        SELECT m.app_id_int, m.app_id_in_store
          FROM app.map_app_id_to_store_id m
         WHERE m.app_id_in_store IN ({app_ids_db})
        """)
        map_app = {}
        for row in rows_apps:
            map_app[row["app_id_in_store"]] = row["app_id_int"]
        for row in self.__config["apps"]:
            row["id"] = map_app[row["id"]]
            if row["genre"] is not None and row["genre"] != "":
                row["genre"] = map_tags[row["genre"]]
            if row["topic"] is not None and row["topic"] != "":
                row["topic"] = map_tags[row["topic"]]

        return self.__config
