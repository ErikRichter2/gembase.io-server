from gembase_server_core.db.db_connection import DbConnection
from src.server.models.survey.survey_config_model import SurveyConfigModel


class SurveysRawHelper:

    @staticmethod
    def create_config(conn: DbConnection):
        return SurveyConfigModel(conn=conn).get()
