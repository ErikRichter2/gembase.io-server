import json

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.survey.surveys_raw_helper import SurveysRawHelper


class StudiesHelper:

    @staticmethod
    def create_survey_config(
            conn: DbConnection,
            study_guid: str
    ):
        config = SurveysRawHelper.create_config(conn=conn)

        dcm_concepts = conn.select_one("""
        SELECT us.dcm_concepts
          FROM app.users_studies us
         WHERE us.guid = %s
        """, [study_guid])["dcm_concepts"]

        config["concepts_dcm_config"] = None

        if dcm_concepts is not None:

            dcm_concepts = json.loads(dcm_concepts)

            dcm_def = {}

            arr = []
            for header in dcm_concepts["headers"]:
                arr.append({
                    "id": len(arr) + 1,
                    "header": header
                })
            dcm_def["headers"] = arr

            arr = []
            for feature in dcm_concepts["features"]:
                arr.append({
                    "id": len(arr) + 1,
                    "feature": feature
                })
            dcm_def["features"] = arr

            config["concepts_dcm_config"] = dcm_def

        return config
