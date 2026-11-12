from server_core.db.db_connection import DbConnection
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.dms_db_model import DmsCache
from src.db.survey.survey_db_model import SurveyDbModel
from src.db.user_billing_db_model import UserBillingDbModel
from src.db.user_studies_db_model import UserStudiesDbModel
from src.env_data import ENV_DATA


class SurveyStudyPreviewModel(BaseUserDbModel):

    def __init__(self, conn: DbConnection, user_id: int, survey_guid: str, survey_data: dict):
        super(SurveyStudyPreviewModel, self).__init__(conn=conn, user_id=user_id)
        self.survey_guid = survey_guid
        self.config = None
        self.texts = None
        self.server_data = {}
        if survey_data is not None and "server" in survey_data:
            self.server_data = survey_data["server"]
        self.client_data = {}
        if survey_data is not None and "client" in survey_data:
            self.client_data = survey_data["client"]

    def get_texts(self):
        if self.texts is None:
            assert ENV_DATA.survey_texts is not None
            self.texts = DmsCache.get_json(conn=self.conn, guid=ENV_DATA.survey_texts)
        return self.texts

    def get_config(self):
        if self.config is None:
            self.config = UserStudiesDbModel.create_survey_config(
                conn=self.conn,
                study_guid=self.survey_guid
            )

        return self.config

    def __is_demo(self, m: SurveyDbModel):
        demo_end = False
        if m.get_current_page() == "genres" or m.get_current_page() == "end":
            if self.billing_db.is_module_locked(UserBillingDbModel.BILLING_MODULE_IDEAS):
                demo_end = True
        return demo_end

    def __return_demo_end(self, m: SurveyDbModel):
        m.set_current_page("end")
        return {
            "survey_data": {
                "client": m.get_client_data_raw(),
                "server": m.get_server_data_raw(),
            },
            "export": {
                "config_data": {
                    "id": "end",
                    "template": "end",
                    "end_type": "demo"
                }
            }
        }

    def get(self):

        m = SurveyDbModel(
            conn=self.conn,
            config=self.get_config(),
            server_data=self.server_data,
            client_data=self.client_data,
            ext_data={},
            survey_instance_guid=self.survey_guid
        )

        if self.__is_demo(m):
            return self.__return_demo_end(m)
        else:
            export = m.export()
            return {
                "survey_data": {
                    "client": m.get_client_data_raw(),
                    "server": m.get_server_data_raw(),
                },
                "export": export
            }

    def submit(self, data: dict):
        m = SurveyDbModel(
            conn=self.conn,
            config=self.get_config(),
            server_data=self.server_data,
            client_data=self.client_data,
            ext_data={},
            survey_instance_guid=self.survey_guid
        )

        demo_end = self.__is_demo(m)

        m.submit(data=data)

        if demo_end:
            return self.__return_demo_end(m)
        else:
            export = m.export()
            return {
                "survey_data": {
                    "client": m.get_client_data_raw(),
                    "server": m.get_server_data_raw(),
                },
                "export": export
            }
