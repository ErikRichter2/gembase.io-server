from gembase_server_core.db.db_connection import DbConnection
from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.dms.dms_constants import DmsConstants
from src.server.models.dms.dms_model import DmsCache
from src.server.models.studies.studies_helper import StudiesHelper
from src.server.models.survey.v2.survey_model_v2 import SurveyModelV2
from src.session.session import gb_session


class SurveyStudyPreviewModel:

    def __init__(self, conn: DbConnection, survey_guid: str, survey_data: {}):
        self.conn = conn
        self.survey_guid = survey_guid
        self.config = None
        self.texts = None
        self.server_data = None
        if survey_data is not None and "server" in survey_data:
            self.server_data = survey_data["server"]
        self.client_data = None
        if survey_data is not None and "client" in survey_data:
            self.client_data = survey_data["client"]

    def get_texts(self):
        if self.texts is None:
            self.texts = DmsCache.get_json(conn=self.conn, guid=DmsConstants.survey_v2_texts)
        return self.texts

    def get_config(self):
        if self.config is None:
            self.config = StudiesHelper.create_survey_config(
                conn=self.conn,
                study_guid=self.survey_guid
            )

        return self.config

    def __is_demo(self, m: SurveyModelV2):
        demo_end = False
        if m.get_current_page() == "genres" or m.get_current_page() == "end":
            if gb_session().models().billing().is_module_locked(
                BillingUtils.BILLING_MODULE_IDEAS,
                ignore_free_trial=True
            ):
                demo_end = True
        return demo_end

    def __return_demo_end(self, m: SurveyModelV2):
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

        m = SurveyModelV2(
            config=self.get_config(),
            server_data=self.server_data,
            client_data=self.client_data,
            ext_data=None
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

    def submit(self, data: {}):
        m = SurveyModelV2(
            config=self.get_config(),
            server_data=self.server_data,
            client_data=self.client_data,
            ext_data=None)

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
