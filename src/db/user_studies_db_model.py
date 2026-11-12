import json

from server_core.commands.command_data import CommandData
from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.surveys_db_model import SurveyRandomRespondentModel, SurveyResultsDbModel, SurveysDbModel
from src.env_data import ENV_DATA

class StudyDbModel(BaseUserDbModel):
    def __init__(self, conn: DbConnection, user_id: int, study_guid: str):
        super(StudyDbModel, self).__init__(conn=conn, user_id=user_id)
        self.study_guid = study_guid

    def save_study(
            self,
            data: dict
    ):
        dcm_concepts = json.dumps([] if "dcm_concepts" not in data else data["dcm_concepts"])
        audiences = json.dumps([] if "audiences" not in data else data["audiences"])

        self.conn.query("""
        UPDATE app.users_studies us
           SET us.name = %s,
               us.internal_respondents = %s,
               us.dcm_concepts = %s,
               us.audiences = %s
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [data["name"], data["internal_respondents"], dcm_concepts, audiences, self.user_id, self.study_guid])

        return {
            "state": "done"
        }

    def launch_study(self):

        row = self.conn.select_one("""
        SELECT us.id, us.audiences, us.dcm_concepts
          FROM app.users_studies us
         WHERE us.user_id = %s
           AND us.guid = %s
           AND us.state = 'edit'
           FOR UPDATE
        """, [self.user_id, self.study_guid])

        study_id = row["id"]

        self.conn.query("""
        UPDATE app.users_studies us
           SET us.state = 'preparing'
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [self.user_id, self.study_guid])

        self.__generate_respondents_guids()

        audiences = row["audiences"]
        config = UserStudiesDbModel.create_survey_config(
            conn=self.conn,
            study_guid=self.study_guid
        )

        respondents_goal = 0
        audiences_cnt = self.__get_cnt_per_audiences()
        for k in audiences_cnt:
            respondents_goal += audiences_cnt[k]

        self.conn.query("""
        INSERT INTO survey_raw.surveys (guid, state, user_id, audiences, config, respondents_goal)
        VALUES (%s, 'working', %s, %s, %s, %s)
        """, [self.study_guid, self.user_id, audiences, json.dumps(config), respondents_goal])

        self.conn.query("""
        INSERT INTO survey_raw.respondents_guids (survey_guid, audience_guid, respondent_guid)
        SELECT %s as survey_guid, g.audience_guid, g.respondent_guid
          FROM app.users_studies_respondents_guids g
         WHERE g.study_id = %s
        """, [self.study_guid, study_id])

        return {
            "state": "done"
        }

    def copy_study(self):
        guid = Utils.get_guid()
        self.conn.query("""
        INSERT INTO app.users_studies (guid, user_id, name, audiences, dcm_concepts, state) 
        SELECT %s as guid, us.user_id, CONCAT(us.name, ' - Copy'), us.audiences, us.dcm_concepts, 'edit' as state
          FROM app.users_studies us
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [guid, self.user_id, self.study_guid])

        return {
            "state": "done",
            "study_guid": guid,
            "studies": self.studies_db.get_studies()
        }

    def delete_study(self):
        self.conn.query("""
        DELETE FROM app.users_studies us
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [self.user_id, self.study_guid])

        return {
            "state": "done",
        }

    def generate_csv_for_internal_launch(self):

        self.__generate_respondents_guids()

        csv = "Url,Id\n"
        client_root = ENV_DATA.client_url + "/survey"

        rows = self.conn.select_all("""
        SELECT i.respondent_guid
          FROM app.users_studies su,
               app.users_studies_respondents_guids i
         WHERE su.user_id = %s
           AND su.guid = %s
           AND su.id = i.study_id
        """, [self.user_id, self.study_guid])

        for row in rows:
            csv += f"{client_root}?id={row['respondent_guid']},{row['respondent_guid']}\n"

        return csv

    def __get_cnt_per_audiences(self):
        row = self.conn.select_one("""
        SELECT us.id, us.internal_respondents, us.audiences
          FROM app.users_studies us
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [self.user_id, self.study_guid])

        cnt_per_audience = {}

        found_audience = False
        if row["audiences"] is not None:
            audiences = json.loads(row["audiences"])
            for it in audiences:
                found_audience = True
                cnt_per_audience[it["guid"]] = it["people"]

        if not found_audience:
            cnt_per_audience["internal"] = row["internal_respondents"]

        return cnt_per_audience

    def __generate_respondents_guids(self):
        study_id = self.conn.select_one("""
        SELECT us.id
          FROM app.users_studies us
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [self.user_id, self.study_guid])["id"]

        cnt_per_audiences = self.__get_cnt_per_audiences()

        for audience_guid in cnt_per_audiences:
            audience_cnt = cnt_per_audiences[audience_guid]
            to_generate_cnt = int(audience_cnt * 1.5)

            if to_generate_cnt > 0:

                existing_cnt = self.conn.select_one("""
                SELECT count(1) as cnt
                  FROM app.users_studies_respondents_guids us
                 WHERE us.study_id = %s
                   AND us.audience_guid = %s
                """, [study_id, audience_guid])["cnt"]

                diff = to_generate_cnt - existing_cnt
                if diff > 0:
                    bulk_data = []
                    for i in range(diff):
                        bulk_data.append((study_id, audience_guid, Utils.get_guid()))
                    self.conn.bulk("""
                    INSERT INTO app.users_studies_respondents_guids (study_id, audience_guid, respondent_guid)
                    VALUES (%s, %s, %s)
                    """, bulk_data)

    def simulate_survey(self):

        row_study = self.conn.select_one("""
        SELECT us.id, us.name, us.audiences
          FROM app.users_studies us
         WHERE us.guid = %s
           AND us.user_id = %s
        """, [self.study_guid, self.user_id])

        study_id = row_study["id"]
        study_name = row_study["name"]
        audiences = json.loads(row_study["audiences"])

        self.__generate_respondents_guids()

        rows_respondents_guids = self.conn.select_all("""
        SELECT usg.respondent_guid
          FROM app.users_studies_respondents_guids usg
         WHERE usg.study_id = %s
        """, [study_id])

        config = UserStudiesDbModel.create_survey_config(
            conn=self.conn,
            study_guid=self.study_guid
        )

        respondents_goal = 0
        audiences_cnt = self.__get_cnt_per_audiences()
        for k in audiences_cnt:
            respondents_goal += audiences_cnt[k]

        rows_favorite_apps = self.conn.select_all("""
        SELECT a.app_id_int
          FROM scraped_data.apps a
         ORDER BY a.installs DESC
         LIMIT 20
        """)
        apps_ids_int = []
        for row in rows_favorite_apps:
            apps_ids_int.append(row["app_id_int"])

        self.conn.query("""
        DELETE FROM survey_raw_simulated.results r
        WHERE r.survey_guid = %s
        """, [self.study_guid])

        cnt = 0
        for row in rows_respondents_guids:
            respondent = SurveyRandomRespondentModel(
                conn=self.conn,
                config=config,
                favorite_apps_ids_int=apps_ids_int
            )
            client_data = respondent.get_client_data()
            client_data = None if client_data is None else json.dumps(client_data)
            server_data = respondent.get_server_data()
            server_data = None if server_data is None else json.dumps(server_data)

            self.conn.query("""
            INSERT INTO survey_raw_simulated.results (survey_guid, respondent_guid, client_data, server_data, end_type) 
            VALUES (%s, %s, %s, %s, %s)
            """, [self.study_guid, row["respondent_guid"], client_data, server_data, respondent.end_type])
            self.conn.commit()

            if respondent.end_type == "completed":
                cnt += 1

            if cnt >= respondents_goal:
                break

        SurveyResultsDbModel(
            conn=self.conn,
            survey_guid=self.study_guid,
            name=study_name,
            config=config,
            audiences=audiences
        ).process_survey_results(is_simulate=True)

        self.conn.query("""
        UPDATE app.users_studies us
           SET us.state = 'done', us.progress_perc = 100
         WHERE us.guid = %s
           AND us.user_id = %s
        """, [self.study_guid, self.user_id])

        return self.studies_db.get_studies()


class UserStudiesDbModel(BaseUserDbModel):
    
    def __init__(self, conn: DbConnection, user_id: int):
        super(UserStudiesDbModel, self).__init__(conn=conn, user_id=user_id)

    def study_db(self, study_guid: str):
        return StudyDbModel(conn=self.conn, user_id=self.user_id, study_guid=study_guid)

    @staticmethod
    def create_survey_config(
            conn: DbConnection,
            study_guid: str
    ):
        config = SurveysDbModel(conn=conn).get_survey_config()

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

    def get_study_model_from_command(self, data: CommandData) -> StudyDbModel:
        assert Utils.is_guid(data.payload["study_guid"])
        return StudyDbModel(
            conn=self.conn,
            user_id=self.user_id,
            study_guid=data.payload["study_guid"]
        )

    def get_studies(self):
        rows = self.conn.select_all("""
        SELECT us.id, us.guid, us.t, us.state, us.name, us.audiences, us.dcm_concepts, 
               us.progress_perc, us.internal_respondents, us.global_study
          FROM app.users_studies us
         WHERE us.user_id = %s
        """, [self.user_id])

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

        cnt = self.conn.select_one("""
        SELECT count(1) as cnt FROM app.users_studies
        WHERE user_id = %s
        """, [self.user_id])["cnt"]

        if cnt >= 50:
            raise Exception(f"Studies limit reached")

        row_dcm_concepts_def = self.conn.select_one_or_none("""
        SELECT d.headers, d.features
          FROM app.def_studies_dcm_concepts d
        """)

        concepts = None

        if row_dcm_concepts_def is not None:
            concepts = {
                "headers": json.loads(row_dcm_concepts_def["headers"]),
                "features": json.loads(row_dcm_concepts_def["features"])
            }

        guid = Utils.get_guid()
        self.conn.insert("""
        INSERT INTO app.users_studies (guid, user_id, name, state, dcm_concepts)
        VALUES (%s, %s, %s, 'edit', %s)
        """, [guid, self.user_id, "Study", json.dumps(concepts)])

        return {
            "state": "done",
            "study_guid": guid,
            "studies": self.get_studies()
        }

    def get_studies_def(self):

        rows_countries = self.conn.select_all("""
        SELECT d.country_id, d.survey_cpi, m.country, d.name
          FROM app.def_countries d,
               app.map_country_id m
         WHERE m.id = d.country_id
           AND d.survey_cpi > 0
        """)

        rows_studies_traits = self.conn.select_all("""
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
