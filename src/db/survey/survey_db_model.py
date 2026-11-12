from __future__ import annotations

import json
from typing import Optional
import uuid

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel
from src.db.dms_db_model import DmsCache
from src.db.survey.survey_page_db_model import SurveyPageDbModel
from src.env_data import ENV_DATA

class SurveyDbModel(BaseDbModel):
    
    def __init__(self, conn: DbConnection, config: dict, client_data: dict, server_data: dict, ext_data: dict, survey_instance_guid: str):
        super(SurveyDbModel, self).__init__(conn=conn)
        self.__config = config
        self.__client_data = client_data
        self.__server_data = server_data
        self.__ext_data = ext_data
        self.__pageModel: SurveyPageDbModel | None = None
        self.survey_instance_guid = survey_instance_guid

        self.init()


    @staticmethod
    def get_config_from_dms(conn: DbConnection):
        assert ENV_DATA.survey_config is not None
        return DmsCache.get_json(conn, guid=ENV_DATA.survey_config)

    @staticmethod
    def is_valid(conn: DbConnection, survey_instance_guid: str):
        row = conn.select_one_or_none("""
        SELECT si.id
          FROM survey_v2.survey_instance si,
               survey_v2.survey_config sc
         WHERE si.guid = %s
           AND si.survey_config = sc.guid
        """, [survey_instance_guid])

        return row is not None

    @staticmethod
    def create_instance(conn: DbConnection, survey_config: str, guid: str, lang: str | None = None):
        conn.insert("""
        INSERT INTO survey_v2.survey_instance (guid, survey_config, lang)
        VALUES (%s, %s, %s)
        """, [guid, survey_config, lang])
        return SurveyDbModel.load_instance(conn=conn, survey_instance_guid=guid)

    @staticmethod
    def load_instance(conn: DbConnection, survey_instance_guid: str):

        row = conn.select_one_or_none("""
        SELECT si.client_data, si.server_data, si.ext_data, sc.config, si.lang
          FROM survey_v2.survey_instance si,
               survey_v2.survey_config sc
         WHERE si.guid = %s
           AND si.survey_config = sc.guid
        """, [survey_instance_guid])

        if row is None:
            return None
        
        client_data = {}
        server_data = {}
        ext_data = {}
        # survey_model.__guid = survey_instance_guid
        # if row["lang"] is not None:
        #     survey_model.__lang = row["lang"]
        if row["client_data"] is not None:
            client_data = json.loads(row["client_data"])
        if row["server_data"] is not None:
            server_data = json.loads(row["server_data"])
        if row["ext_data"] is not None:
            ext_data = json.loads(row["ext_data"])
        
        survey_model = SurveyDbModel(
            conn=conn,
            config=json.loads(row["config"]),
            client_data=client_data,
            server_data=server_data,
            ext_data=ext_data,
            survey_instance_guid=survey_instance_guid
        )
        survey_model.init()

        return survey_model

    @staticmethod
    def generate_survey_group(conn: DbConnection, survey_config: str, pool: dict):
        rowid = conn.insert("""
        INSERT INTO survey_v2.survey_control (guid, survey_config, pool)
        VALUES (%s, %s, %s)
        """, [str(uuid.uuid4()), survey_config, json.dumps(pool)])

        conn.query("""
        DELETE FROM survey_v2.survey_whitelist w
         WHERE w.survey_control = %s
        """, [rowid])

        query = """
        INSERT INTO survey_v2.survey_whitelist (gid, survey_control, pool_id)
        VALUES (%s, %s, %s)
        """

        data = []
        for it in pool:
            pool_id = it["id"]
            pool_count = it["cnt"]
            for i in range(pool_count):
                data.append((str(uuid.uuid4()), rowid, pool_id))

        conn.bulk(query, data)

    @staticmethod
    def generate_survey_urls(conn: DbConnection, env: str | None, survey_control: str, add_params=True, preview=False):
        rows = conn.select_all("""
        SELECT w.gid
          FROM survey_v2.survey_control c,
               survey_v2.survey_whitelist w
         WHERE c.guid = %s
           AND c.id = w.survey_control
        """, [survey_control])

        res = []

        client_url = ENV_DATA.client_url

        for row in rows:
            r = "survey"
            if preview:
                r = "survey-preview"
            url = f"{client_url}/{r}?gid={row['gid']}"
            if add_params:
                url += "&cid=[ID]&s=[S]&a=[AGE]&y=[YOB]"
            else:
                url += f"&cid={row['gid']}"
            res.append({
                "guid": row["gid"],
                "url": url
            })

        return res

    def create_page_model(self, page: str):
        from src.db.survey.survey_page_db_model import SurveyAgePageModel, SurveyCompetitorsPageModel, SurveyConceptsDcmPageModel, SurveyDefaultPageModel, SurveyEndPageModel, SurveyGenreItemsPageModel, SurveyGenresDcmPageModel, SurveyLoyaltyPageModel, SurveyMultiplayerItemsPageModel, SurveyPageDbModel, SurveyPlayingPageModel, SurveySpendingPageModel, SurveySpendingV2PageModel, SurveyTopicDcmPageModel, SurveyTopicItemsPageModel

        surveyPageModels = {
            "genres": SurveyGenreItemsPageModel,
            "topics": SurveyTopicItemsPageModel,
            "genres_dcm": SurveyGenresDcmPageModel,
            "concepts_dcm": SurveyConceptsDcmPageModel,
            "topics_dcm": SurveyTopicDcmPageModel,
            "age": SurveyAgePageModel,
            "spending": SurveySpendingPageModel,
            "playing_time": SurveyPlayingPageModel,
            "loyalty": SurveyLoyaltyPageModel,
            "end": SurveyEndPageModel,
            "competitors": SurveyCompetitorsPageModel,
            "multiplayer": SurveyMultiplayerItemsPageModel,
            "spending_v2": SurveySpendingV2PageModel
        }
        
        for k in surveyPageModels:
            if k == page:
                return surveyPageModels[k](survey_model=self)
            
        return SurveyDefaultPageModel(survey_model=self)

    def init(self):
        if self.__server_data is None:
            config = self.get_config("order")
            first_page = config[0]["id"]
            #first_page = "routine"
            self.__server_data = {
                "state": {
                    "current_page": first_page
                },
                "pages": {

                }
            }
            self.dirty()
        if self.__client_data is None:
            self.__client_data = {
                "pages": {

                }}
            self.dirty()
        if self.__ext_data is None:
            self.__ext_data = {}
            self.dirty()
        self.init_page_model()

    def set_client_data(self, data):
        page = self.get_current_page()
        if "pages" not in self.__client_data:
            self.__client_data["pages"] = {}
        self.__client_data["pages"][page] = data
        self.dirty()

    def get_server_data_raw(self):
        return self.__server_data

    def get_client_data_raw(self):
        return self.__client_data

    def get_client_data(self, page: Optional[str] = None) -> dict:
        if page is None:
            page = self.get_current_page()
        if page in self.__client_data["pages"]:
            return self.__client_data["pages"][page]
        return {}

    def set_server_data(self, data):
        page = self.get_current_page()
        self.__server_data["pages"][page] = data
        self.dirty()

    def get_server_data(self, page: Optional[str] = None) -> dict:
        if page is None:
            page = self.get_current_page()
        if page in self.__server_data["pages"]:
            return self.__server_data["pages"][page]
        return {}

    def set_ext_data(self, data):
        self.__ext_data = data
        self.dirty()

    def get_ext_data(self):
        return self.__ext_data

    def init_page_model(self):
        self.__pageModel = self.create_page_model(self.get_current_page())
        if self.__pageModel is None:
            self.__pageModel = SurveyPageDbModel(survey_model=self)
        self.__pageModel.init()

    # def guid(self):
    #     return self.__guid

    def dirty(self):
        self.__dirty = True

    # def save(self):
    #     if not self.__dirty:
    #         return
    #
    #     self.__dirty = False
    #     gb_session().conn().query("""
    #     UPDATE survey_v2.survey_instance si
    #        SET si.client_data = %s,
    #            si.server_data = %s,
    #            si.ext_data = %s
    #      WHERE si.guid = %s
    #     """, [json.dumps(self.__client_data), json.dumps(self.__server_data), json.dumps(self.__ext_data), self.__guid])

    def export(self):
        assert self.__pageModel is not None
        res = self.__pageModel.export()
        res["progress_data"] = self.get_progress()
        #res["lang"] = self.__lang
        self.track_time("export")
        #self.save()
        return res

    def get_all_config(self):
        return self.__config

    def get_current_config(self):
        return self.get_config(self.get_current_page())

    def get_config(self, page: Optional[str] = None) -> dict:
        if page is not None:
            if page in self.__config:
                return self.__config[page]
            else:
                return {}
        return self.__config

    def set_current_page(self, page: str):
        current_page = self.get_current_page()
        if current_page != page:
            self.__server_data["state"]["current_page"] = page
            self.init_page_model()
            self.dirty()

    def get_current_page(self):
        return self.__server_data["state"]["current_page"]

    def track_time(self, track_type: str):
        timestamp = Utils.timestamp_int()
        ext_data = self.get_ext_data()
        if ext_data is None:
            ext_data = {}
        if "server_time" not in ext_data:
            ext_data["server_time"] = []
        current_page = self.get_current_page()
        track_page = None
        for it in ext_data["server_time"]:
            if it["id"] == current_page:
                track_page = it
                break
        if track_page is None:
            track_page = {
                "id": current_page
            }
            ext_data["server_time"].append(track_page)

        if track_type not in track_page:
            track_page[track_type] = {
                "f": timestamp,
                "l": timestamp
            }
        else:
            track_page[track_type]["l"] = timestamp

        self.set_ext_data(ext_data)

    def submit(self, data):
        assert self.__pageModel is not None
        self.track_time("submit")
        self.__pageModel.submit(data)
        #self.save()

    def get_config_param_value(self, param: str, value: str = "value", page: str | None = None) -> str | None:
        if page is None:
            page = self.get_current_page()
        config_data = self.get_config(page)
        if config_data is not None:
            try:
                for row in config_data:
                    if row["param"] == param:
                        return row[value]
            except:
                return None
        return None

    def get_config_text_for_param_with_value(self, param: str, value: str, page: str | None = None) -> str | None:
        if page is None:
            page = self.get_current_page()
        config_data = self.get_config(page)
        if config_data is not None:
            for row in config_data:
                if row["param"] == param and row["value"] == value:
                    return row["text"]
        return None

    def get_progress(self):
        current_page = self.get_current_page()
        ordered_pages = self.get_config("order")
        progress_current = 0
        progress_total = len(ordered_pages)
        for row in ordered_pages:
            progress_current += 1
            if row["id"] == current_page:
                break
        return {
            "current": progress_current,
            "total": progress_total
        }
