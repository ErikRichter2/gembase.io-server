import json

from gembase_server_core.commands.command_data import CommandData
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.studies.study_model import StudyModel
from src.utils.gembase_utils import GembaseUtils


class StudiesSessionModel(BaseSessionModel):

    def __init__(self, session):
        super(StudiesSessionModel, self).__init__(session)

    def get_study_model_from_command(self, data: CommandData) -> StudyModel:
        assert GembaseUtils.is_guid(data.payload["study_guid"])
        return StudyModel(
            study_guid=data.payload["study_guid"],
            session=self.session()
        )

    def get_studies(self):
        rows = self.conn().select_all("""
        SELECT us.id, us.guid, us.t, us.state, us.name, us.audiences, us.dcm_concepts, 
               us.progress_perc, us.internal_respondents, us.global_study
          FROM app.users_studies us
         WHERE us.user_id = %s
        """, [self.user_id()])

        res = []
        for row in rows:
            o = {
                "guid": row["guid"],
                "name": row["name"],
                "t": row["t"].timestamp(),
                "state": row["state"],
                "progress_perc": row["progress_perc"],
                "audiences": [],
                "dcm_concepts": None,
                "internal_respondents": row["internal_respondents"],
                "global_study": row["global_study"]
            }
            res.append(o)

            if row["audiences"] is not None:
                o["audiences"] = json.loads(row["audiences"])

            if row["dcm_concepts"] is not None:
                o["dcm_concepts"] = json.loads(row["dcm_concepts"])

        return res

    def create_study(self):

        cnt = self.conn().select_one("""
        SELECT count(1) as cnt FROM app.users_studies
        WHERE user_id = %s
        """, [self.user_id()])["cnt"]

        if cnt >= 50:
            raise Exception(f"Studies limit reached")

        row_dcm_concepts_def = self.conn().select_one_or_none("""
        SELECT d.headers, d.features
          FROM app.def_studies_dcm_concepts d
        """)

        concepts = None

        if row_dcm_concepts_def is not None:
            concepts = {
                "headers": json.loads(row_dcm_concepts_def["headers"]),
                "features": json.loads(row_dcm_concepts_def["features"])
            }

        guid = GembaseUtils.get_guid()
        self.conn().insert("""
        INSERT INTO app.users_studies (guid, user_id, name, state, dcm_concepts)
        VALUES (%s, %s, %s, 'edit', %s)
        """, [guid, self.user_id(), "Study", json.dumps(concepts)])

        return {
            "state": "done",
            "study_guid": guid,
            "studies": self.get_studies()
        }

    def get_studies_def(self):

        rows_countries = self.conn().select_all("""
        SELECT d.country_id, d.survey_cpi, m.country, d.name
          FROM app.def_countries d,
               app.map_country_id m
         WHERE m.id = d.country_id
           AND d.survey_cpi > 0
        """)

        rows_studies_traits = self.conn().select_all("""
        SELECT m.id, m.trait_id, d.name, d.study_default
          FROM app.def_studies_traits d,
               app.map_studies_traits m
         WHERE m.id = d.id
        """)

        return {
            "age": [
                {
                    "id": "0__25",
                    "from": 0,
                    "to": 25,
                    "label": "< 25"
                },
                {
                    "id": "26__40",
                    "from": 26,
                    "to": 40,
                    "label": "25 - 40"
                },
                {
                    "id": "41__55",
                    "from": 41,
                    "to": 55,
                    "label": "40 - 55"
                },
                {
                    "id": "56__999",
                    "from": 56,
                    "to": 999,
                    "label": "55+"
                }
            ],
            "countries": rows_countries,
            "traits": rows_studies_traits
        }
