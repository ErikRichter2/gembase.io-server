import json

from gembase_server_core.private_data.private_data_model import PrivateDataModel
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.studies.studies_helper import StudiesHelper
from src.server.models.survey.survey_random_respondent_model import SurveyRandomRespondentModel
from src.server.models.survey.v2.survey_results_model import SurveyResultsModel
from src.session.session_instance import GbSessionInstance
from src.utils.gembase_utils import GembaseUtils


class StudyModel(BaseSessionModel):

    def __init__(self, study_guid: str, session: GbSessionInstance):
        super(StudyModel, self).__init__(session)
        self.__study_guid = study_guid

    def save_study(
            self,
            data: {}
    ):
        dcm_concepts = json.dumps([] if "dcm_concepts" not in data else data["dcm_concepts"])
        audiences = json.dumps([] if "audiences" not in data else data["audiences"])

        self.conn().query("""
        UPDATE app.users_studies us
           SET us.name = %s,
               us.internal_respondents = %s,
               us.dcm_concepts = %s,
               us.audiences = %s
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [data["name"], data["internal_respondents"], dcm_concepts, audiences, self.user_id(), self.__study_guid])

        return {
            "state": "done"
        }

    def launch_study(self):

        row = self.conn().select_one("""
        SELECT us.id, us.audiences, us.dcm_concepts
          FROM app.users_studies us
         WHERE us.user_id = %s
           AND us.guid = %s
           AND us.state = 'edit'
           FOR UPDATE
        """, [self.user_id(), self.__study_guid])

        study_id = row["id"]

        self.conn().query("""
        UPDATE app.users_studies us
           SET us.state = 'preparing'
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [self.user_id(), self.__study_guid])

        self.__generate_respondents_guids()

        audiences = row["audiences"]
        config = StudiesHelper.create_survey_config(
            conn=self.conn(),
            study_guid=self.__study_guid
        )

        respondents_goal = 0
        audiences_cnt = self.__get_cnt_per_audiences()
        for k in audiences_cnt:
            respondents_goal += audiences_cnt[k]

        self.conn().query("""
        INSERT INTO survey_raw.surveys (guid, state, user_id, audiences, config, respondents_goal)
        VALUES (%s, 'working', %s, %s, %s, %s)
        """, [self.__study_guid, self.user_id(), audiences, json.dumps(config), respondents_goal])

        self.conn().query("""
        INSERT INTO survey_raw.respondents_guids (survey_guid, audience_guid, respondent_guid)
        SELECT %s as survey_guid, g.audience_guid, g.respondent_guid
          FROM app.users_studies_respondents_guids g
         WHERE g.study_id = %s
        """, [self.__study_guid, study_id])

        return {
            "state": "done"
        }

    def copy_study(self):
        guid = GembaseUtils.get_guid()
        self.conn().query("""
        INSERT INTO app.users_studies (guid, user_id, name, audiences, dcm_concepts, state) 
        SELECT %s as guid, us.user_id, CONCAT(us.name, ' - Copy'), us.audiences, us.dcm_concepts, 'edit' as state
          FROM app.users_studies us
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [guid, self.user_id(), self.__study_guid])

        return {
            "state": "done",
            "study_guid": guid,
            "studies": self.session().models().studies().get_studies()
        }

    def delete_study(self):
        self.conn().query("""
        DELETE FROM app.users_studies us
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [self.user_id(), self.__study_guid])

        return {
            "state": "done",
        }

    def generate_csv_for_internal_launch(self):

        self.__generate_respondents_guids()

        csv = "Url,Id\n"
        client_root = PrivateDataModel.get_private_data()['gembase']['client']['url_root'] + "/survey"

        rows = self.conn().select_all("""
        SELECT i.respondent_guid
          FROM app.users_studies su,
               app.users_studies_respondents_guids i
         WHERE su.user_id = %s
           AND su.guid = %s
           AND su.id = i.study_id
        """, [self.user_id(), self.__study_guid])

        for row in rows:
            csv += f"{client_root}?id={row['respondent_guid']},{row['respondent_guid']}\n"

        return csv

    def __get_cnt_per_audiences(self):
        row = self.conn().select_one("""
        SELECT us.id, us.internal_respondents, us.audiences
          FROM app.users_studies us
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [self.user_id(), self.__study_guid])

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
        study_id = self.conn().select_one("""
        SELECT us.id
          FROM app.users_studies us
         WHERE us.user_id = %s
           AND us.guid = %s
        """, [self.user_id(), self.__study_guid])["id"]

        cnt_per_audiences = self.__get_cnt_per_audiences()

        for audience_guid in cnt_per_audiences:
            audience_cnt = cnt_per_audiences[audience_guid]
            to_generate_cnt = int(audience_cnt * 1.5)

            if to_generate_cnt > 0:

                existing_cnt = self.conn().select_one("""
                SELECT count(1) as cnt
                  FROM app.users_studies_respondents_guids us
                 WHERE us.study_id = %s
                   AND us.audience_guid = %s
                """, [study_id, audience_guid])["cnt"]

                diff = to_generate_cnt - existing_cnt
                if diff > 0:
                    bulk_data = []
                    for i in range(diff):
                        bulk_data.append((study_id, audience_guid, GembaseUtils.get_guid()))
                    self.conn().bulk("""
                    INSERT INTO app.users_studies_respondents_guids (study_id, audience_guid, respondent_guid)
                    VALUES (%s, %s, %s)
                    """, bulk_data)

    def simulate_survey(self):

        row_study = self.conn().select_one("""
        SELECT us.id, us.name, us.audiences
          FROM app.users_studies us
         WHERE us.guid = %s
           AND us.user_id = %s
        """, [self.__study_guid, self.user_id()])

        study_id = row_study["id"]
        study_name = row_study["name"]
        audiences = json.loads(row_study["audiences"])

        self.__generate_respondents_guids()

        rows_respondents_guids = self.conn().select_all("""
        SELECT usg.respondent_guid
          FROM app.users_studies_respondents_guids usg
         WHERE usg.study_id = %s
        """, [study_id])

        config = StudiesHelper.create_survey_config(
            conn=self.conn(),
            study_guid=self.__study_guid
        )

        respondents_goal = 0
        audiences_cnt = self.__get_cnt_per_audiences()
        for k in audiences_cnt:
            respondents_goal += audiences_cnt[k]

        rows_favorite_apps = self.conn().select_all("""
        SELECT a.app_id_int
          FROM scraped_data.apps a
         ORDER BY a.installs DESC
         LIMIT 20
        """)
        apps_ids_int = []
        for row in rows_favorite_apps:
            apps_ids_int.append(row["app_id_int"])

        self.conn().query("""
        DELETE FROM survey_raw_simulated.results r
        WHERE r.survey_guid = %s
        """, [self.__study_guid])

        cnt = 0
        for row in rows_respondents_guids:
            respondent = SurveyRandomRespondentModel(
                config=config,
                favorite_apps_ids_int=apps_ids_int
            )
            client_data = respondent.get_client_data()
            client_data = None if client_data is None else json.dumps(client_data)
            server_data = respondent.get_server_data()
            server_data = None if server_data is None else json.dumps(server_data)

            self.conn().query("""
            INSERT INTO survey_raw_simulated.results (survey_guid, respondent_guid, client_data, server_data, end_type) 
            VALUES (%s, %s, %s, %s, %s)
            """, [self.__study_guid, row["respondent_guid"], client_data, server_data, respondent.end_type])
            self.conn().commit()

            if respondent.end_type == "completed":
                cnt += 1

            if cnt >= respondents_goal:
                break

        SurveyResultsModel(
            conn=self.conn(),
            survey_guid=self.__study_guid,
            name=study_name,
            config=config,
            audiences=audiences
        ).process_survey_results(is_simulate=True)

        self.conn().query("""
        UPDATE app.users_studies us
           SET us.state = 'done', us.progress_perc = 100
         WHERE us.guid = %s
           AND us.user_id = %s
        """, [self.__study_guid, self.user_id()])

        return self.session().models().studies().get_studies()
