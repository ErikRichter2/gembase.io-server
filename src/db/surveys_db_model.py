import json
import random
from typing import Optional

import numpy as np

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.dms_db_model import DmsCache, DmsDbModel
from src.db.games_db_model import GamesDbModel, TagsMapper
from src.db.survey.survey_db_model import SurveyDbModel
from src.db.survey.survey_page_db_model import SurveyPageDbModel
from src.db.user_db_model import UserDbModel
from src.env_data import ENV_DATA
from src.models.user_obfuscator import UserObfuscator
from scipy import stats

class SurveysDbModel(BaseDbModel):

    SURVEY_CONTROL_GUID = "a236b79f-6d2e-4f23-ad92-d018cb518346"
    CURRENT_SURVEY_VERSION = 3

    def __init__(self, conn: DbConnection):
        super(SurveysDbModel, self).__init__(conn=conn)
        
    def get_survey_meta_id(self, survey_control_guid: str) -> Optional[int]:
        row = self.conn.select_one_or_none("""
        SELECT m.id
          FROM gb_survey_data.survey_meta m
         WHERE m.survey_control_guid = %s
        """, [survey_control_guid])

        if row is None:
            return None

        return row["id"]

    def postprocess(self, survey_control_guid: str):
        self.calc_tam_for_survey_tags(survey_control_guid=survey_control_guid)

    def calc_tam_for_survey_tags(self, survey_control_guid: str | None = None):
        all_surveys = -1
        if survey_control_guid is None:
            all_surveys = 1

        query = f"""
            DELETE FROM gb_survey_data.survey_tam_per_tag s
             WHERE s.survey_meta_id IN (
                SELECT m.id
                  FROM gb_survey_data.survey_meta m
                 WHERE (m.survey_control_guid = '{survey_control_guid}' OR {all_surveys} = 1)
             )
            """

        self.conn.query(query)

        query = f"""
            INSERT INTO gb_survey_data.survey_tam_per_tag (
            survey_meta_id, 
            tag_id_int,
            ltv, 
            filtered_cnt, 
            total_cnt, 
            tam_base)
            SELECT z.survey_meta_id,
                   z.tag_id_int,
                   z.ltv,
                   z.filtered_cnt,
                   z.total_cnt,
                   IF(z.filtered_cnt = 0 or z.total_cnt = 0 or z.ltv = 0, 0, z.filtered_cnt / z.total_cnt * z.ltv) as tam_base
              FROM (
                SELECT MAX(x.survey_meta_id) as survey_meta_id,
                       x.tag_id_int,
                       AVG(x.spending) AS ltv,
                       MAX(x.tag_id_cnt) AS filtered_cnt,
                       MAX(x.total_cnt) AS total_cnt
                  FROM (
                    SELECT m.id AS survey_meta_id,
                           p.tag_id_int,
                           i.spending,
                           ROW_NUMBER() OVER (PARTITION BY t.tag_id ORDER BY i.spending) AS tag_id_row_order,
                           COUNT(1) OVER (PARTITION BY t.tag_id) AS tag_id_cnt,
                           y.cnt AS total_cnt
                      FROM gb_survey_data.survey_meta m,
                           gb_survey_data.survey_info i USE INDEX(),
                           gb_survey_data.survey_tags t USE INDEX(),
                           gb_app.def_sheet_platform_product p,
                         (SELECT COUNT(1) AS cnt
                             FROM gb_survey_data.survey_meta mm,
                                  gb_survey_data.survey_info ii
                            WHERE (mm.survey_control_guid = '{survey_control_guid}' OR {all_surveys} = 1)
                              AND mm.id = ii.survey_meta_id ) y
                     WHERE (m.survey_control_guid = '{survey_control_guid}' OR {all_surveys} = 1)
                       AND m.id = t.survey_meta_id
                       AND m.id = i.survey_meta_id
                       AND i.survey_instance = t.survey_instance
                       AND t.tag_id = p.tag_id
                       AND t.tag_value = 100
                       AND p.is_prompt = 1
                     ORDER BY p.tag_id_int, i.spending
                       ) x
                 WHERE x.tag_id_row_order IN (FLOOR((x.tag_id_cnt + 1) / 2), FLOOR((x.tag_id_cnt + 2) / 2) )
                 GROUP BY x.tag_id_int ) z
            """

        self.conn.query(query)

    def get_player_explorer_data(
            self,
            user_id: int,
            input_filters: list[dict],
            show: str | None = None
    ):
        if len(input_filters) == 0:
            input_filters.append({})

        filters_cnt = len(input_filters)

        rows_per_filter = {}
        rows_favorite_app = {}
        #rows_competitors = {}
        rows_flags = {}
        rows_cnt = {}
        rows_age_groups = {}

        rows_studies = self.conn.select_all("""
        SELECT s.id as survey_id, 
               us.guid, 
               us.name, 
               us.default_study, 
               us.dcm_concepts
          FROM app.users_studies us,
               survey_results.surveys s
         WHERE (us.user_id = %s OR us.global_study = 1)
           AND (us.state = 'done' OR us.state = 'working')
           AND s.guid = us.guid
        """, [user_id])

        default_study = None
        surveys_items = []
        survey_guid_id_map = {}
        for row in rows_studies:
            survey_guid_id_map[row["guid"]] = row["survey_id"]
            if default_study is None and row["default_study"] == 1:
                default_study = row["guid"]
            surveys_items.append({
                "id": row["guid"],
                "label": row["name"]
            })

        rows_cnt_full = [{}] * filters_cnt
        rows_age_groups_full = [[]] * filters_cnt

        active_surveys = []

        for i in range(len(input_filters)):
            active_survey = None
            f = input_filters[i]

            if "surveys" in f and "active" in f["surveys"]:
                active_survey = f["surveys"]["active"]
                found = False
                for tmp in surveys_items:
                    if tmp["id"] == active_survey:
                        found = True
                        break
                if not found:
                    active_survey = None

            if active_survey is None:
                active_survey = default_study

            f["surveys"] = {
                "active": active_survey,
                "items": [
                    {
                        "opt": surveys_items
                    }
                ]
            }

            survey_id = survey_guid_id_map[active_survey]
            active_surveys.append(survey_id)

            if show == "concepts":
                for row in rows_studies:
                    if row["survey_id"] == survey_id and row["dcm_concepts"] is None:
                        show = None

            query = """
            SELECT count(1) as cnt, 
                   SUM(r.female) as females_cnt 
              FROM survey_results.respondents r
             WHERE r.survey_id = %s
            """
            rows_cnt_full[i] = self.conn.select_one(query, [survey_id])

            query = f"""
            SELECT age_group, 
                   count(1) as cnt 
              FROM (
                    SELECT CASE WHEN r.age >= 0 AND r.age <= 29 THEN 0
                                WHEN r.age >= 30 AND r.age <= 45 THEN 1
                                ELSE 2 END as age_group
                      FROM survey_results.respondents r
                     WHERE r.survey_id = %s
                    ) z1
              GROUP BY z1.age_group
              ORDER BY z1.age_group
            """
            rows_age_groups_full[i] = self.conn.select_all(query, [survey_id])

            filter_query = self.__get_survey_filter_query(
                survey_id=survey_id,
                input_filter=input_filters[i],
            )
            
            calc_conn = DbConnection()
            calc_utils = CalcUtils(conn=calc_conn)

            calc_utils.recreate_table(
                table_name=f"tmp_survey_info_filter_{i}",
                query=f"""
                CREATE TABLE platform_values.x__table_name__x (
                    respondent_id int unsigned not null,
                    constraint x__table_name__x_pk
                        primary key (respondent_id)
                )
                {filter_query}
                """
            )
            
            calc_conn.close()

            cnt_query = f"""
            SELECT count(1) as cnt, 
                   SUM(r.female) as females_cnt 
              FROM platform_values.tmp_survey_info_filter_{i} f,
                   survey_results.respondents r
             WHERE f.respondent_id = r.respondent_id
               AND r.survey_id = %s
            """
            rows_cnt[i] = self.conn.select_one(cnt_query, [survey_id])

            age_groups_query = f"""
            SELECT age_group, 
                   count(1) as cnt 
              FROM (
                    SELECT CASE WHEN r.age >= 0 AND r.age <= 29 THEN 0
                                WHEN r.age >= 30 AND r.age <= 45 THEN 1
                                ELSE 2 END as age_group
                      FROM platform_values.tmp_survey_info_filter_{i} f,
                           survey_results.respondents r
                     WHERE f.respondent_id = r.respondent_id
                       AND r.survey_id = %s
                   ) z1
             GROUP BY z1.age_group
             ORDER BY z1.age_group
            """
            rows_age_groups[i] = self.conn.select_all(age_groups_query, [survey_id])

            if show == "concepts":
                pass
            else:
                query = f"""
                SELECT z3.subcategory_int,
                       z3.tag_id_int,
                       round(z3.loved_ratio) as loved_ratio,
                       p.node,
                       m.subcategory,
                       m.client_name as subcategory_client_name,
                       p.description,
                       p.unlocked
                  FROM (
                        SELECT z2.subcategory_int,
                               z2.tag_id_int,
                               z2.loved_ratio,
                               ROW_NUMBER() over (PARTITION BY z2.subcategory_int ORDER BY z2.loved_ratio DESC) as row_num
                          FROM (
                                SELECT z1.subcategory_int,
                                       z1.tag_id_int,
                                       z1.loved_cnt,
                                       z1.total_cnt,
                                       round(z1.loved_cnt / z1.total_cnt * 100, 2) as loved_ratio
                                  FROM (
                                        SELECT p.subcategory_int,
                                               p.tag_id_int,
                                               SUM(t.loved) as loved_cnt,
                                               COUNT(1) as total_cnt
                                          FROM platform_values.tmp_survey_info_filter_{i} f,
                                               survey_results.tags t,
                                               app.def_sheet_platform_product p
                                         WHERE f.respondent_id = t.respondent_id
                                           AND t.survey_id = %s
                                           AND t.tag_id_int = p.tag_id_int
                                         GROUP BY p.subcategory_int, p.tag_id_int
                                   ) z1
                           ) z2
                       ) z3,
                       app.def_sheet_platform_product p,
                       app.map_tag_subcategory m
                 WHERE z3.tag_id_int = p.tag_id_int
                   AND p.subcategory_int = m.id
                   AND z3.row_num <= 30
                 ORDER BY z3.row_num
                """
                rows_per_filter[i] = self.conn.select_all(query, [survey_id])

                # flags
                rows_flags[i] = self.conn.select_all(f"""
                SELECT z2.tag_id_int,
                       z2.subcategory_int, 
                       z2.cnt, m.subcategory, 
                       m.client_name as subcategory_client_name, 
                       p.node, 
                       p.description, 
                       p.unlocked
                  FROM (
                        SELECT z1.tag_id_int,
                               z1.subcategory_int,
                               z1.cnt,
                               ROW_NUMBER() over (PARTITION BY z1.subcategory_int ORDER BY z1.cnt DESC) as row_num
                          FROM (
                                SELECT p.tag_id_int,
                                       p.subcategory_int,
                                       COUNT(1) as cnt
                                  FROM survey_results.flags fl,
                                       platform_values.tmp_survey_info_filter_{i} f,
                                       app.def_sheet_platform_product p
                                 WHERE fl.survey_id = %s
                                   AND fl.respondent_id = f.respondent_id
                                   AND fl.tag_id_int = p.tag_id_int
                                 GROUP BY 
                                       p.tag_id_int, 
                                       p.subcategory_int
                               ) z1
                       ) z2,
                       app.map_tag_subcategory m,
                       app.def_sheet_platform_product p
                 WHERE z2.row_num <= 30
                   AND z2.subcategory_int = m.id
                   AND p.tag_id_int = z2.tag_id_int
                 ORDER BY z2.subcategory_int, z2.cnt DESC
                """, [survey_id])

        charts_data = []

        charts_sorting = [
            [
                GamesDbModel.SUBCATEGORY_GENRE_ID,
                #GamesDbModel.SUBCATEGORY_CORE_ID,
                GamesDbModel.SUBCATEGORY_BEHAVIORS_ID,
                GamesDbModel.SUBCATEGORY_MULTIPLAYER_ID,
                GamesDbModel.SUBCATEGORY_COMPLEXITY_ID,
                GamesDbModel.SUBCATEGORY_ACTIVITIES_ID,
                GamesDbModel.SUBCATEGORY_PLATFORMS_ID,
                GamesDbModel.SUBCATEGORY_SOCIALS_ID,
                GamesDbModel.SUBCATEGORY_HOBBIES_ID,
                GamesDbModel.SUBCATEGORY_MOVIES_ID,
            ],
            [
                GamesDbModel.SUBCATEGORY_TOPICS_ID,
                GamesDbModel.SUBCATEGORY_DOMAINS_ID,
                GamesDbModel.SUBCATEGORY_NEEDS_ID,
                GamesDbModel.SUBCATEGORY_FOCUS_ID,
                GamesDbModel.SUBCATEGORY_ERAS_ID,
                GamesDbModel.SUBCATEGORY_ENVIRONMENT_ID,
                GamesDbModel.SUBCATEGORY_ENTITIES_ID,
                GamesDbModel.SUBCATEGORY_ROLES_ID,
            ]
        ]
        
        obfuscator = UserDbModel(conn=self.conn, user_id=user_id).obfuscator

        if show == "concepts":
            charts_data = self.__get_concepts_data(survey_id=active_surveys[0])
        else:
            dataset_2_tag_id_int_map = {}
            for i in range(1, len(rows_per_filter)):
                for row in rows_per_filter[i]:
                    if row[UserObfuscator.TAG_ID_INT] not in dataset_2_tag_id_int_map:
                        dataset_2_tag_id_int_map[row[UserObfuscator.TAG_ID_INT]] = []
                    dataset_2_tag_id_int_map[row[UserObfuscator.TAG_ID_INT]].append(float(row["loved_ratio"]))

            charts_data_tag_id = {}

            rows_goal_values = self.conn.select_all("""
            SELECT subcategory_int, 
                   goal_value 
              FROM app.def_sheet_subcategory_goals
            """)

            for row in rows_per_filter[0]:

                subcategory_int = row["subcategory_int"]
                subcategory_name = row["subcategory_client_name"]
                if subcategory_name is None:
                    subcategory_name = row["subcategory"]

                goal_value = None
                for row_goal_value in rows_goal_values:
                    if row_goal_value["subcategory_int"] == subcategory_int:
                        goal_value = float(row_goal_value["goal_value"]) * 100
                        break

                chart_locked = False

                if subcategory_int not in charts_data_tag_id:
                    charts_data_tag_id[subcategory_int] = {
                        "id": subcategory_int,
                        "name": subcategory_name,
                        "data": [],
                        "locked": chart_locked
                    }
                    if goal_value is not None:
                        charts_data_tag_id[subcategory_int]["goal_value"] = goal_value
                    if subcategory_int == GamesDbModel.SUBCATEGORY_BEHAVIORS_ID:
                        charts_data_tag_id[subcategory_int]["template"] = "behaviors"
                    charts_data.append(charts_data_tag_id[subcategory_int])

                if not chart_locked:
                    values = [0.0] * filters_cnt
                    values[0] = float(row["loved_ratio"])
                    if row[UserObfuscator.TAG_ID_INT] in dataset_2_tag_id_int_map:
                        for i in range(len(dataset_2_tag_id_int_map[row[UserObfuscator.TAG_ID_INT]])):
                            values[i + 1] = dataset_2_tag_id_int_map[row[UserObfuscator.TAG_ID_INT]][i]
                    description = ""
                    if row["description"] is not None:
                        description = row["description"]
                        
                    charts_data_tag_id[subcategory_int]["data"].append({
                        UserObfuscator.TAG_ID_INT: row[UserObfuscator.TAG_ID_INT],
                        "id": obfuscator.server_to_client(row[UserObfuscator.TAG_ID_INT]),
                        "values": values,
                        "name": row["node"],
                        "desc": description,
                    })

                    # sort behaviors
                    if subcategory_int == GamesDbModel.SUBCATEGORY_BEHAVIORS_ID:
                        tmp_arr = []

                        for tag_id_int in [
                            GamesDbModel.BEHAVIOR_UTILIZE,
                            GamesDbModel.BEHAVIOR_PLAN,
                            GamesDbModel.BEHAVIOR_CULTIVATE,
                            GamesDbModel.BEHAVIOR_BOND,
                            GamesDbModel.BEHAVIOR_IMMERSE,
                            GamesDbModel.BEHAVIOR_EXPLORE,
                            GamesDbModel.BEHAVIOR_IMPROVISE,
                            GamesDbModel.BEHAVIOR_DESTROY,
                            GamesDbModel.BEHAVIOR_CHALLENGE,
                            GamesDbModel.BEHAVIOR_EXPRESS,
                        ]:
                            for it in charts_data_tag_id[subcategory_int]["data"]:
                                if it[UserObfuscator.TAG_ID_INT] == tag_id_int:
                                    tmp_arr.append(it)
                                    break
                        charts_data_tag_id[subcategory_int]["data"] = tmp_arr

            dataset_2_map = {}
            for i in range(1, len(rows_flags)):
                for row in rows_flags[i]:
                    total_rows = int(rows_cnt_full[i]["cnt"])
                    dataset_2_map[row[UserObfuscator.TAG_ID_INT]] = round((float(row["cnt"]) / total_rows) * 100)

            for row in rows_flags[0]:
                total_rows = int(rows_cnt_full[0]["cnt"])
                ratio = round((float(row["cnt"]) / total_rows) * 100)
                subcategory_id = row["subcategory_int"]
                subcategory_name = row["subcategory_client_name"]
                goal_value = None
                for row_goal_value in rows_goal_values:
                    if row_goal_value["subcategory_int"] == subcategory_id:
                        goal_value = float(row_goal_value["goal_value"]) * 100
                        break
                if subcategory_name is None:
                    subcategory_name = row["subcategory"]
                dataset_2_value = 0
                if row[UserObfuscator.TAG_ID_INT] in dataset_2_map:
                    dataset_2_value = dataset_2_map[row[UserObfuscator.TAG_ID_INT]]
                description = ""
                if row["description"] is not None:
                    description = row["description"]
                if subcategory_id not in charts_data_tag_id:
                    charts_data_tag_id[subcategory_id] = {
                        "id": subcategory_id,
                        "name": subcategory_name,
                        "data": [],
                        "locked": False,
                    }
                    if goal_value is not None:
                        charts_data_tag_id[subcategory_id]["goal_value"] = goal_value
                    charts_data.append(charts_data_tag_id[subcategory_id])
                charts_data_tag_id[subcategory_id]["data"].append({
                    "id": obfuscator.server_to_client(row[UserObfuscator.TAG_ID_INT]),
                    "values": [
                        ratio,
                        dataset_2_value
                    ],
                    "name": row["node"],
                    "desc": description
                })

        # stats

        stats_arr = []
        for i in range(len(input_filters)):

            total_rows = int(rows_cnt_full[i]["cnt"])
            females_cnt_total = int(rows_cnt_full[i]["females_cnt"])
            females_total_ratio = round(females_cnt_total / total_rows * 100, 2)

            filtered_rows = int(rows_cnt[i]["cnt"])

            stats = {}

            stats["full_rows"] = total_rows
            stats["filtered_rows"] = filtered_rows

            stats["full_females"] = females_total_ratio
            stats["filtered_females"] = 0
            if filtered_rows > 0:
                females_filtered_cnt = int(rows_cnt[i]["females_cnt"])
                females_filtered_ratio = round(females_filtered_cnt / filtered_rows * 100, 2)
                stats['filtered_females'] = females_filtered_ratio

            stats["full_age"] = [0, 0, 0]
            for row in rows_age_groups_full[i]:
                stats["full_age"][row["age_group"]] = int(round(int(row["cnt"]) / total_rows * 100, 2))
            stats["filtered_age"] = [0, 0, 0]
            if filtered_rows > 0:
                for row in rows_age_groups[i]:
                    stats["filtered_age"][row["age_group"]] = int(round(int(row["cnt"]) / filtered_rows * 100, 2))

            stats_arr.append(stats)

        charts_data_final = [[], []]
        found = []
        for i in range(len(charts_sorting)):
            for chart_id in charts_sorting[i]:
                for chart_data in charts_data:
                    if chart_data["id"] == chart_id:
                        found.append(chart_data["id"])
                        charts_data_final[i].append(chart_data)
                        break

        # for chart_data in charts_data:
        #     if chart_data["id"] not in found:
        #         charts_data_final[0].append(chart_data)

        return {
            "show": show,
            "charts": charts_data_final,
            "filters": input_filters,
            "stats": stats_arr
        }

    def __get_survey_filter_query(
            self,
            survey_id: int,
            input_filter: dict
    ) -> str:

        row_age = self.conn.select_one("""
        SELECT MIN(r.age) as min_age,
               MAX(r.age) as max_age
          FROM survey_results.respondents r
         WHERE r.survey_id = %s
        """, [survey_id])

        min_age = max(18, row_age["min_age"])
        max_age = min(99, row_age["max_age"])

        if "age" not in input_filter:
            input_filter["age"] = {
                "from": min_age,
                "to": max_age
            }

        input_filter["age"]["min"] = min_age
        input_filter["age"]["max"] = max_age

        if "females" not in input_filter:
            input_filter["females"] = {
                "min": 0,
                "max": 100,
                "value": 50
            }

        row_females = self.conn.select_one("""
        SELECT SUM(r.female) as female_cnt,
               COUNT(1) as total_cnt
          FROM survey_results.respondents r
         WHERE r.survey_id = %s
           AND r.age >= %s
           AND r.age <= %s
        """, [survey_id, input_filter["age"]["from"], input_filter["age"]["to"]])

        total_cnt = int(row_females["total_cnt"])
        females_cnt = int(row_females["female_cnt"])
        males_cnt = total_cnt - females_cnt
        perc = int(input_filter["females"]["value"])
        ratio = perc / 100

        max_females_cnt = int(total_cnt * ratio)
        max_males_cnt = int(total_cnt - max_females_cnt)

        if max_females_cnt > females_cnt:
            max_females_cnt = females_cnt
            max_males_cnt = int((max_females_cnt / perc * 100) * (1 - ratio))

        if max_males_cnt > males_cnt:
            max_males_cnt = males_cnt
            max_females_cnt = int((max_males_cnt / (100 - perc) * 100) * ratio)

        # ======= spending =======

        spending_groups = [
            {
                "id": "0_0",
                "label": "$0",
                "from": 0,
                "to": 0
            },
            {
                "id": "1_100",
                "label": "$1 - $100",
                "from": 1,
                "to": 100
            },
            {
                "id": "101_1000",
                "label": "$101 - $1000",
                "from": 101,
                "to": -1
            }
        ]

        active_ids = []
        if "spending" not in input_filter:
            input_filter["spending"] = {
                "active": [],
                "items": spending_groups
            }
            for it in spending_groups:
                active_ids.append(it["id"])
        else:
            for id in input_filter["spending"]["active"]:
                active_ids.append(id)

        input_filter['spending']['items'] = spending_groups

        input_filter['spending']['active'] = []
        for active_id in active_ids:
            input_filter['spending']['active'].append(active_id)

        spending_condition = []
        if len(active_ids) == 0:
            spending_condition.append("1 = 2")
        else:
            for active_id in active_ids:
                for it in spending_groups:
                    if it["id"] == active_id:
                        spending_condition.append(f"(r.spending >= {it['from']} AND (r.spending <= {it['to']} OR {it['to']} = -1))")

        spending_condition_db = f"({' OR '.join(spending_condition)})"

        loved = [-1]
        if "loved" in input_filter and len(input_filter["loved"][UserObfuscator.TAG_IDS_INT]) > 0:
            loved = input_filter["loved"][UserObfuscator.TAG_IDS_INT]
        loved_db = self.conn.values_arr_to_db_in(loved, int_values=True)
        not_hated = [-1]
        if "not_hated" in input_filter and len(input_filter["not_hated"][UserObfuscator.TAG_IDS_INT]) > 0:
            not_hated = input_filter["not_hated"][UserObfuscator.TAG_IDS_INT]
        not_hated_db = self.conn.values_arr_to_db_in(not_hated, int_values=True)

        query = f"""
        SELECT z1.respondent_id
          FROM (
                SELECT r.survey_id,
                       r.respondent_id,
                       r.female,
                       ROW_NUMBER() over (PARTITION BY r.female) as row_num_female
                  FROM survey_results.respondents r
                 WHERE r.survey_id = {survey_id}
                   AND r.age >= {input_filter["age"]["from"]}
                   AND r.age <= {input_filter["age"]["to"]}   
                   AND {spending_condition_db}
           ) z1
           INNER JOIN survey_results.respondents r
              ON r.respondent_id = z1.respondent_id
             AND r.survey_id = z1.survey_id
            INNER JOIN (
                 SELECT BIT_OR(z1.b) as b, SUM(z1.cnt) as cnt
                   FROM (
                        SELECT d.b, 1 as cnt
                          FROM platform.def_tags_bin d
                         WHERE d.tag_id_int IN ({loved_db})
                         UNION ALL
                         SELECT z.b, 0 as cnt
                           FROM platform.zero_bin_value z
                   ) z1
               ) loved_bin
            INNER JOIN (
                 SELECT BIT_OR(z1.b) as b, SUM(z1.cnt) as cnt
                   FROM (
                        SELECT d.b, 1 as cnt
                          FROM platform.def_tags_bin d
                         WHERE d.tag_id_int IN ({not_hated_db})
                         UNION ALL
                         SELECT z.b, 0 as cnt
                           FROM platform.zero_bin_value z
                   ) z1
               ) not_hated_bin
           INNER JOIN survey_results.tags_bin tb
              ON tb.survey_id = r.survey_id
             AND tb.respondent_id = r.respondent_id
             AND (BIT_COUNT(tb.b_loved & loved_bin.b) = loved_bin.cnt OR loved_bin.cnt = 0)
             AND (BIT_COUNT(tb.b_hated & not_hated_bin.b) = 0 OR not_hated_bin.cnt = 0)
           WHERE ((z1.female = 1 AND z1.row_num_female <= {max_females_cnt})
              OR (z1.female = 0 AND z1.row_num_female <= {max_males_cnt}))
        """

        return query

    def __get_concepts_data(
            self,
            survey_id: int
    ):
        config = json.loads(self.conn.select_one("""
        SELECT s.config
          FROM survey_results.surveys s
         WHERE s.id = %s
        """, [survey_id])["config"])

        dcm_def = config["concepts_dcm_config"]

        header_q = {}
        max_q = None
        for header in dcm_def["headers"]:
            rnd = random.randrange(30, 80)
            header_q[header["id"]] = rnd
            if max_q is None or header_q[max_q] < rnd:
                max_q = header["id"]

        market_reach_data = []
        for header in dcm_def["headers"]:

            q = header_q[header["id"]]
            q1 = random.randrange(2, 10)
            q2 = random.randrange(2, 10)

            market_reach_data.append({
                "id": str(header["id"]),
                "category": header["header"]["title"],
                "name": header["header"]["title"],
                "desc": header["header"]["description"],
                "qmin": int(q - q1 * 1.5),
                "q25": int(q - q1),
                "q50": q,
                "q75": int(q + q2),
                "qmax": int(q + q2 * 1.5)
            })

        market_reach_data.sort(key=lambda x: x["q50"], reverse=True)

        map_best_features_data = {}
        for i in range(len(dcm_def["headers"])):
            map_best_features_data[dcm_def["headers"][i]["id"]] = []
            for feature in dcm_def["features"]:
                map_best_features_data[dcm_def["headers"][i]["id"]].append({
                    "id": str(feature["id"]),
                    "category": feature["feature"]["text"],
                    "name": feature["feature"]["text"],
                    "desc": "",
                    "values": [random.randint(-100, 100)],
                    "feature_pool_id": feature["feature"]["pool_id"]
                })
            map_best_features_data[dcm_def["headers"][i]["id"]].sort(key=lambda x: x["values"][0], reverse=True)

        best_features_data = []
        for k in map_best_features_data:
            best_features_data.append({
                "id": str(k),
                "data": map_best_features_data[k]
            })

        map_topics_overlap_data = {}
        for i in range(len(dcm_def["headers"])):
            for j in range(0, len(dcm_def["headers"])):
                if i == j:
                    continue

                header_i = dcm_def["headers"][i]
                header_j = dcm_def["headers"][j]

                if header_i["id"] not in map_topics_overlap_data:
                    map_topics_overlap_data[header_i["id"]] = []
                map_topics_overlap_data[header_i["id"]].append({
                    "id": str(header_j["id"]),
                    "category": header_j["header"]["title"],
                    "name": header_j["header"]["title"],
                    "desc": header_j["header"]["description"],
                    "values": [random.randint(0, 100)]
                })

        topics_overlap_data = []
        for k in map_topics_overlap_data:
            topics_overlap_data.append({
                "id": str(k),
                "data": map_topics_overlap_data[k]
            })

        return [
            {
                "id": "market_reach",
                "name": "Market reach",
                "goal_value": 50,
                "data": market_reach_data
            },
            {
                "id": "best_features",
                "name": "Best features",
                "goal_value": 50,
                "data": [],
                "best_features_data": best_features_data
            },
            {
                "id": "topics_overlap",
                "name": "Topics overlap",
                "goal_value": 50,
                "data": [],
                "topics_overlap_data": topics_overlap_data
            }
        ]

    def get_survey_config(self):
        assert ENV_DATA.survey_config is not None
        config = DmsCache.get_json(conn=self.conn, guid=ENV_DATA.survey_config, create_copy=True)
        config["__version"] = self.CURRENT_SURVEY_VERSION

        rows_map_tags = self.conn.select_all("""
        SELECT m.id, m.tag_id
          FROM app.map_tag_id m
        """)

        map_tags = {}
        for row in rows_map_tags:
            map_tags[row["tag_id"]] = row["id"]

        for page in ["genres", "topics", "needs", "behaviors"]:
            for row in config[page]:
                if row["param"] == "item":
                    row["value"] = map_tags[row["value"]]

        app_ids = []
        for row in config["apps"]:
            app_ids.append(row["id"])
        app_ids_db = self.conn.values_arr_to_db_in(app_ids)
        rows_apps = self.conn.select_all(f"""
        SELECT m.app_id_int, m.app_id_in_store
          FROM app.map_app_id_to_store_id m
         WHERE m.app_id_in_store IN ({app_ids_db})
        """)
        map_app = {}
        for row in rows_apps:
            map_app[row["app_id_in_store"]] = row["app_id_int"]
        for row in config["apps"]:
            row["id"] = map_app[row["id"]]
            if row["genre"] is not None and row["genre"] != "":
                row["genre"] = map_tags[row["genre"]]
            if row["topic"] is not None and row["topic"] != "":
                row["topic"] = map_tags[row["topic"]]

        return config


class SurveyResultsDbModel(BaseDbModel):

    __survey_dcm_map = {
        "a236b79f-6d2e-4f23-ad92-d018cb518346": "0ce01ac6-d0b0-4b79-b89c-a68f4c7a783b"
    }

    def __init__(self, conn: DbConnection, survey_guid: str, name: str, config: dict, audiences: dict):
        super(SurveyResultsDbModel, self).__init__(conn=conn)
        self.__survey_guid = survey_guid
        self.__name = name
        self.__map_tag_id = {}
        self.__config = config
        self.__audiences = audiences
        self.__sheet_config = DmsCache.get_from_cache(conn=conn, guid=ENV_DATA.survey_config)
        self.__sheet_texts = DmsCache.get_from_cache(conn=conn, guid=ENV_DATA.survey_texts)["data"]

    def clear_results(self):

        row_survey_id = self.conn.select_one_or_none("""
        SELECT s.id
          FROM survey_results.surveys s
         WHERE s.guid = %s
        """, [self.__survey_guid])

        survey_id = None if row_survey_id is None else row_survey_id["id"]

        if survey_id is not None:

            self.conn.query("""
            DELETE FROM survey_results.surveys s WHERE s.id = %s
            """, [survey_id])

            self.conn.query("""
            DELETE FROM survey_results.respondents s WHERE s.survey_id = %s
            """, [survey_id])

            self.conn.query("""
            DELETE FROM survey_results.flags s WHERE s.survey_id = %s
            """, [survey_id])

            self.conn.query("""
            DELETE FROM survey_results.tags s WHERE s.survey_id = %s
            """, [survey_id])

            self.conn.query("""
            DELETE FROM survey_results.competitors s WHERE s.survey_id = %s
            """, [survey_id])

            self.conn.query("""
            DELETE FROM survey_results.dcm_genres s WHERE s.survey_id = %s
            """, [survey_id])

            self.conn.query("""
            DELETE FROM survey_results.dcm_topics s WHERE s.survey_id = %s
            """, [survey_id])

            self.conn.query("""
            DELETE FROM survey_results.dcm_concepts s WHERE s.survey_id = %s
            """, [survey_id])

        return survey_id

    def __get_map_tag_id(self):
        if self.__map_tag_id is None:
            self.__map_tag_id = TagsMapper.instance(conn=self.conn)
        return self.__map_tag_id["s2i"]

    def __migrate(self, raw_results: list[dict]):
        version = 0

        if "__version" in self.__config:
            version = self.__config["__version"]

        if version == SurveysDbModel.CURRENT_SURVEY_VERSION:
            return

        if version == 0:

            # tags str to int
            for it in raw_results:
                pages = it["client_data"]["pages"]

                # tags str to int
                to_delete = []
                to_change = []
                for page in ["genres", "topics", "needs", "behaviors"]:
                    if page in pages:
                        for tag_id in pages[page]:
                            if tag_id == "":
                                to_delete.append({
                                    "page": page,
                                    "tag_id": tag_id
                                })
                                continue
                            tag_id_new = tag_id
                            if page == "behaviors":
                                def migr_behaviors(old: str) -> str:
                                    if old == "behaviors__experiment":
                                        return "behaviors__explore"
                                    if old == "behaviors__discover":
                                        return "behaviors__explore"
                                    if old == "behaviors__experience":
                                        return "behaviors__immerse"
                                    if old == "behaviors__combat":
                                        return "behaviors__destroy"
                                    if old == "behaviors__complete":
                                        return "behaviors__utilize"
                                    if old == "behaviors__collect":
                                        return "behaviors__utilize"
                                    if old == "behaviors__empower":
                                        return "behaviors__cultivate"
                                    if old == "behaviors__solve":
                                        return "behaviors__improvise"
                                    if old == "behaviors__cooperate":
                                        return "behaviors__bond"
                                    if old == "behaviors__compete":
                                        return "behaviors__challenge"
                                    return old
                                tag_id_new = migr_behaviors(tag_id)
                            tag_id_int = self.__get_map_tag_id()[tag_id_new]
                            to_change.append({
                                "page": page,
                                "tag_id": tag_id,
                                "tag_id_int": tag_id_int
                            })
                for k in to_delete:
                    del pages[k["page"]][k["tag_id"]]
                for k in to_change:
                    pages[k["page"]][k["tag_id_int"]] = pages[k["page"]][k["tag_id"]]
                    del pages[k["page"]][k["tag_id"]]

            # apps str to int
            app_ids = []
            for it in raw_results:
                pages = it["client_data"]["pages"]
                if "competitors" in pages:
                    for competitor in pages["competitors"]:
                        if competitor not in app_ids:
                            app_ids.append(competitor)
                if "favorite_game" in pages:
                    if "favorite_game" in pages["favorite_game"]:
                        if pages["favorite_game"]["favorite_game"] not in app_ids:
                            app_ids.append(pages["favorite_game"]["favorite_game"])

            if len(app_ids) > 0:
                rows_apps = self.conn.select_all(f"""
                SELECT m.app_id_int, m.app_id_in_store
                  FROM app.map_app_id_to_store_id m
                 WHERE m.app_id_in_store IN ({self.conn.values_arr_to_db_in(app_ids)})
                """)
                map_app = {}
                for row in rows_apps:
                    map_app[row["app_id_in_store"]] = row["app_id_int"]

                for it in raw_results:
                    to_change = []
                    pages = it["client_data"]["pages"]
                    if "competitors" in pages:
                        for competitor in pages["competitors"]:
                            to_change.append(competitor)
                    if "favorite_game" in pages:
                        if "favorite_game" in pages["favorite_game"]:
                            if pages["favorite_game"]["favorite_game"] in map_app:
                                pages["favorite_game"]["favorite_game"] = map_app[pages["favorite_game"]["favorite_game"]]
                            else:
                                pages["favorite_game"]["favorite_game"] = 0

                    for k in to_change:
                        pages["competitors"][map_app[k]] = pages["competitors"][k]
                        del pages["competitors"][k]

            # genres dcm core id to core id int
            for it in raw_results:
                pages = it["client_data"]["pages"]
                if "topics_dcm" in pages:
                    server_dcm = it["server_data"]["pages"]["topics_dcm"]
                    for tmp in server_dcm["dcm"]:
                        tmp["topic"] = self.__get_map_tag_id()[tmp["topic"]]
                        tmp["choices"][0]["items"][0] = self.__get_map_tag_id()[tmp["choices"][0]["items"][0]]
                        tmp["choices"][0]["items"][1] = self.__get_map_tag_id()[tmp["choices"][0]["items"][1]]
                        tmp["choices"][1]["items"][0] = self.__get_map_tag_id()[tmp["choices"][1]["items"][0]]
                        tmp["choices"][1]["items"][1] = self.__get_map_tag_id()[tmp["choices"][1]["items"][1]]
                if "genres_dcm" in pages:
                    server_dcm = it["server_data"]["pages"]["genres_dcm"]
                    for tmp in server_dcm["dcm"]:
                        tmp["genres"][0] = self.__get_map_tag_id()[tmp["genres"][0]]
                        if len(tmp["genres"]) == 2:
                            tmp["genres"][1] = self.__get_map_tag_id()[tmp["genres"][1]]
                        tmp["choices"][0] = self.__get_map_tag_id()[tmp["choices"][0]]
                        tmp["choices"][1] = self.__get_map_tag_id()[tmp["choices"][1]]
                    for k in pages["genres_dcm"]:
                        if pages["genres_dcm"][k] != "none":
                            pages["genres_dcm"][k] = self.__get_map_tag_id()[pages["genres_dcm"][k]]

    def process_survey_results(self, is_simulate=False):

        print("STARTED")

        row_survey_id = self.conn.select_one_or_none("""
        SELECT s.id
          FROM survey_results.surveys s
         WHERE s.guid = %s
        """, [self.__survey_guid])

        survey_id = None if row_survey_id is None else row_survey_id["id"]

        map_respondent_guid_to_id = {}

        if survey_id is None:
            survey_id = self.conn.insert("""
            INSERT INTO survey_results.surveys (guid) VALUES (%s)
            """, [self.__survey_guid])

        if is_simulate:
            rows_raw_results = self.conn.select_all("""
            SELECT r.respondent_guid, r.client_data, r.server_data
              FROM survey_raw_simulated.results r
             WHERE r.survey_guid = %s
               AND r.end_type = 'completed'
            """, [self.__survey_guid])
        else:
            rows_raw_results = self.conn.select_all("""
            SELECT r.respondent_guid, r.client_data, r.server_data, r.ext_data
              FROM survey_raw.results r
             WHERE r.survey_guid = %s
               AND r.state = 'completed'
            """, [self.__survey_guid])

        bulk_data_respondents = []
        bulk_data_flags = []
        bulk_data_tags = []
        bulk_data_competitors = []
        bulk_data_genres_dcm = []
        bulk_data_topics_dcm = []
        bulk_data_concepts_dcm = []

        respondent_id = 0

        genres_core_choices_map = {}
        topics_choices_map = {}

        raw_results = []
        for row in rows_raw_results:
            raw_results.append({
                "respondent_guid": row["respondent_guid"],
                "client_data": json.loads(row["client_data"]),
                "server_data": json.loads(row["server_data"])
            })

        self.__migrate(raw_results=raw_results)

        for result in raw_results:
            respondent_id += 1

            respondent_guid = result["respondent_guid"]
            client_data = result["client_data"]
            server_data = result["server_data"]

            map_respondent_guid_to_id[respondent_guid] = respondent_id

            pages = client_data["pages"]

            if "gender" not in pages or "gender" not in pages["gender"]:
                print(f"Error: {respondent_guid}")
                continue

            respondent = {
                "guid": respondent_guid
            }

            # female
            respondent["female"] = 1 if pages["gender"]["gender"] == "f" else 0

            # age
            age: str = pages["age"]["age"]
            age_arr = age.split("__")
            if age_arr[0] == "-1":
                age_val = 18
            elif age_arr[1] == "-1":
                age_val = 65
            else:
                age_val = (int(age_arr[1]) + int(age_arr[0])) // 2
            respondent["age"] = int(age_val)

            # role
            respondent["role"] = int(pages["role"]["role"])

            # favorite_game
            respondent["favorite_game"] = int(pages["favorite_game"]["favorite_game"])

            # spending
            page = "spending"
            spending_arr = pages[page][page].split("__")
            if spending_arr[0] == "-1":
                spending_arr[0] = "0"
            elif spending_arr[1] == "-1":
                spending_arr[1] = "1000"
            spending_val = (int(spending_arr[0]) + int(spending_arr[1])) // 2
            respondent["spending"] = spending_val

            # playing time
            page = "playing_time"
            playing_arr = pages[page][page].split("__")
            if playing_arr[0] == "-1" and playing_arr[1] == "-1":
                playing_arr[0] = "0"
                playing_arr[1] = "0"
            elif playing_arr[0] == "-1":
                playing_arr[0] = "0"
            elif playing_arr[1] == "-1":
                playing_arr[1] = "15"
            respondent["playing_time"] = (int(playing_arr[0]) + int(playing_arr[1])) // 2

            # loyalty
            page = "loyalty"
            arr = pages[page][page].split("__")
            if arr[0] == "-1" and arr[1] == "-1":
                pass
            elif arr[0] == "-1":
                arr[0] = "0"
            elif arr[1] == "-1":
                arr[1] = "5"
            respondent["loyalty"] = (int(arr[0]) + int(arr[1])) // 2

            bulk_data_respondents.append((
                survey_id,
                respondent_id,
                respondent["guid"],
                respondent["female"],
                respondent["age"],
                respondent["role"],
                respondent["favorite_game"],
                respondent["spending"],
                respondent["playing_time"],
                respondent["loyalty"]
            ))

            # flags
            for page in ["devices", "routine", "hobbies", "movies", "socials"]:
                if page in pages:
                    if page in pages[page]:
                        for tmp in pages[page][page]:
                            tag_id_int = 0
                            if tmp in self.__get_map_tag_id():
                                tag_id_int = self.__get_map_tag_id()[tmp]
                            bulk_data_flags.append((survey_id, respondent_id, tmp, None, tag_id_int))
            for page in ["multiplayer"]:
                if page in pages:
                    for flag_id in pages[page]:
                        tag_id_int = 0
                        if flag_id in self.__get_map_tag_id():
                            tag_id_int = self.__get_map_tag_id()[flag_id]
                        val = int(pages[page][flag_id])
                        bulk_data_flags.append((survey_id, respondent_id, flag_id, val, tag_id_int))

            # tags
            for tmp in [["genres", 3], ["topics", 3], ["needs", 2], ["behaviors", 2]]:
                page = tmp[0]
                answers_cnt = tmp[1]
                if page in pages:
                    for tag_id_int in pages[page]:
                        val = int((int(pages[page][tag_id_int]) - 1) / answers_cnt * 100)
                        loved = SurveyUtils.is_loved(val)
                        hated = SurveyUtils.is_hated(val)
                        bulk_data_tags.append((survey_id, respondent_id, int(tag_id_int), val, loved, hated))

            # competitors
            if "competitors" in pages:
                for competitor in pages["competitors"]:
                    val = int((int(pages["competitors"][competitor]) - 1) / (5 - 1) * 100)
                    loved = SurveyUtils.is_loved(val)
                    bulk_data_competitors.append((survey_id, respondent_id, competitor, val, loved))

            # genres_dcm
            if "genres_dcm" in pages:
                server_dcm = server_data["pages"]["genres_dcm"]
                ix = 0
                for tmp in server_dcm["dcm"]:
                    ix += 1
                    genre1 = tmp["genres"][0]
                    genre2 = None
                    if len(tmp["genres"]) == 2:
                        genre2 = tmp["genres"][1]
                    core1 = tmp["choices"][0]
                    core2 = tmp["choices"][1]
                    chosen = pages["genres_dcm"][str(tmp["id"])]
                    if chosen == "none":
                        chosen = None
                    else:
                        if respondent_id not in genres_core_choices_map:
                            genres_core_choices_map[respondent_id] = []
                        if core1 not in genres_core_choices_map[respondent_id]:
                            genres_core_choices_map[respondent_id].append(core1)
                        if core2 not in genres_core_choices_map[respondent_id]:
                            genres_core_choices_map[respondent_id].append(core2)

                    bulk_data_genres_dcm.append((
                        survey_id,
                        respondent_id,
                        ix,
                        genre1,
                        genre2,
                        core1,
                        core2,
                        chosen))

            # topics_dcm
            if "topics_dcm" in pages:
                server_dcm = server_data["pages"]["topics_dcm"]
                ix = 0
                for tmp in server_dcm["dcm"]:
                    ix += 1
                    topic = tmp["topic"]
                    f1a = tmp["choices"][0]["items"][0]
                    f1b = tmp["choices"][0]["items"][1]
                    f2a = tmp["choices"][1]["items"][0]
                    f2b = tmp["choices"][1]["items"][1]
                    chosen = pages["topics_dcm"][str(tmp["id"])]
                    chosen_arr = chosen.split("__")
                    chosen_a = None
                    chosen_b = None
                    if chosen_arr[0] != "none":
                        if int(chosen_arr[1]) == 0:
                            chosen_a = f1a
                            chosen_b = f1b
                        else:
                            chosen_a = f2a
                            chosen_b = f2b

                        if respondent_id not in topics_choices_map:
                            topics_choices_map[respondent_id] = []
                        if chosen_a not in topics_choices_map[respondent_id]:
                            topics_choices_map[respondent_id].append(chosen_a)
                        if chosen_b not in topics_choices_map[respondent_id]:
                            topics_choices_map[respondent_id].append(chosen_b)

                    bulk_data_topics_dcm.append((
                        survey_id,
                        respondent_id,
                        ix,
                        topic,
                        f1a,
                        f1b,
                        f2a,
                        f2b,
                        chosen_a,
                        chosen_b,
                    ))

            # concepts_dcm
            if "concepts_dcm" in pages:
                server_dcm = server_data["pages"]["concepts_dcm"]
                ix = 0
                for tmp in server_dcm["dcm"]:
                    ix += 1
                    chosen = pages["concepts_dcm"][str(ix)]
                    for j in range(len(tmp["choices"])):
                        choice = tmp["choices"][j]
                        bulk_data_concepts_dcm.append((
                            survey_id,
                            respondent_id,
                            ix,
                            j + 1,
                            choice["header"],
                            choice["feature_1"],
                            choice["feature_2"],
                            chosen == choice["id"]
                        ))
                    bulk_data_concepts_dcm.append((
                        survey_id,
                        respondent_id,
                        ix,
                        3,
                        0,
                        0,
                        0,
                        chosen == "none"
                    ))

        if is_simulate:
            topics_dcm_tags = SurveyResultsDbModel.__get_simulate_topics_dcm_results(
                survey_id=survey_id,
                topics_choices_map=topics_choices_map
            )
            bulk_data_tags += topics_dcm_tags
            genres_dcm_tags = SurveyResultsDbModel.__get_simulate_genres_dcm_results(
                genres_core_choices_map=genres_core_choices_map,
                survey_id=survey_id
            )
            bulk_data_tags += genres_dcm_tags
        else:
            dcm_tags = self.__get_external_dcm_results(
                survey_id=survey_id,
                map_respondent_guid_to_id=map_respondent_guid_to_id
            )

            if dcm_tags is not None:
                bulk_data_tags += dcm_tags

        self.clear_results()

        self.conn.query("""
        INSERT INTO survey_results.surveys (id, guid, is_simulated, audiences, config, name) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        """, [survey_id, self.__survey_guid, is_simulate, json.dumps(self.__audiences),
              json.dumps(self.__config), self.__name])

        self.conn.bulk("""
        INSERT INTO survey_results.respondents (survey_id, respondent_id, guid, female, age, role, favorite_game, spending, playing_time, loyalty) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """, bulk_data_respondents)

        self.conn.bulk("""
        INSERT INTO survey_results.flags (survey_id, respondent_id, flag_id, flag_value, tag_id_int) 
        VALUES (%s, %s, %s, %s, %s) 
        """, bulk_data_flags)

        self.conn.bulk("""
        INSERT INTO survey_results.tags (survey_id, respondent_id, tag_id_int, tag_value, loved, hated) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        """, bulk_data_tags)

        self.conn.bulk("""
        INSERT INTO survey_results.competitors (survey_id, respondent_id, app_id_int, value, loved) 
        VALUES (%s, %s, %s, %s, %s) 
        """, bulk_data_competitors)

        self.conn.bulk("""
        INSERT INTO survey_results.dcm_genres (survey_id, respondent_id, dcm_order, genre_1, genre_2,
        core_1, core_2, chosen) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
        """, bulk_data_genres_dcm)

        self.conn.bulk("""
        INSERT INTO survey_results.dcm_topics (survey_id, respondent_id, dcm_order, topic, feature_1a,
        feature_1b, feature_2a, feature_2b, chosen_a, chosen_b) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """, bulk_data_topics_dcm)

        self.conn.bulk("""
        INSERT INTO survey_results.dcm_concepts (survey_id, respondent_id, dcm_order,
        card_index, header_id, feature_1_id, feature_2_id, chosen) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
        """, bulk_data_concepts_dcm)

        bin_bytes_cnt = self.conn.select_one("""
        SELECT b.cnt
          FROM platform.bin_bytes_cnt b
        """)["cnt"]

        self.conn.query(f"""
        alter table survey_results.tags_bin modify b binary({bin_bytes_cnt}) not null;
        """)
        self.conn.query(f"""
        alter table survey_results.tags_bin modify b_loved binary({bin_bytes_cnt}) not null;
        """)
        self.conn.query(f"""
        alter table survey_results.tags_bin modify b_hated binary({bin_bytes_cnt}) not null;
        """)

        self.conn.query("""
        DELETE FROM survey_results.tags_bin tb WHERE tb.survey_id = %s
        """, [survey_id])
        self.conn.query("""
        INSERT INTO survey_results.tags_bin (survey_id, respondent_id, b, b_loved, b_hated)
        SELECT t.survey_id, t.respondent_id, BIT_OR(d.b), BIT_OR(IF(t.loved, d.b, z.b)), BIT_OR(IF(t.hated, d.b, z.b))
          FROM survey_results.tags t,
               platform.def_tags_bin d,
               platform.zero_bin_value z
         WHERE t.survey_id = %s
           AND d.tag_id_int = t.tag_id_int
         GROUP BY t.survey_id, t.respondent_id
        """, [survey_id])

        self.conn.commit()

        print("DONE !")

    @staticmethod
    def __get_simulate_genres_dcm_results(survey_id: int, genres_core_choices_map: dict):
        data = []
        for respondent_id in genres_core_choices_map:
            for tag_id_int in genres_core_choices_map[respondent_id]:
                rnd = random.random()
                val = 0
                if rnd < 0.3:
                    val = 0
                elif rnd < 0.6:
                    val = 50
                else:
                    val = 100
                data.append((survey_id, respondent_id, tag_id_int, val, SurveyUtils.is_loved(val), SurveyUtils.is_hated(val)))
        return data

    @staticmethod
    def __get_simulate_topics_dcm_results(survey_id: int, topics_choices_map: dict):
        data = []
        for respondent_id in topics_choices_map:
            for tag_id_int in topics_choices_map[respondent_id]:
                rnd = random.random()
                val = 0
                if rnd < 0.3:
                    val = 0
                elif rnd < 0.6:
                    val = 50
                else:
                    val = 100
                data.append((survey_id, respondent_id, tag_id_int, val, SurveyUtils.is_loved(val), SurveyUtils.is_hated(val)))
        return data

    def __get_external_dcm_results(self, survey_id: int, map_respondent_guid_to_id: dict):

        if self.__survey_guid not in SurveyResultsDbModel.__survey_dcm_map:
            return

        df = DmsDbModel(conn=self.conn).read_df_from_external_data(SurveyResultsDbModel.__survey_dcm_map[self.__survey_guid])

        for col in df.columns:
            if col == "survey_instance" or col == "index" or col == "base":
                continue
            df_tmp = df[col][np.abs(stats.zscore(df[col])) < 3]
            min_val = np.min(df_tmp.to_numpy())
            max_val = np.max(df_tmp.to_numpy())
            normalized_col = "n___" + col
            df[normalized_col] = df.apply(lambda r: (r[col] - min_val) / (max_val - min_val), axis=1)
            df[normalized_col].clip(lower=0, upper=1, inplace=True)

        q_data = []
        for index, row in df.iterrows():
            for col in df.columns:
                if "n__" in col:
                    tag_id = col.replace("n___", "")
                    if tag_id in self.__get_map_tag_id():
                        tag_id_int = self.__get_map_tag_id()[tag_id]
                        val = row[col]
                        val = int(val * 100)
                        if val >= 80:
                            val = 100
                        elif val <= 20:
                            val = 0
                        else:
                            val = 50

                        q_data.append((survey_id, map_respondent_guid_to_id[row["survey_instance"]], tag_id_int, val, SurveyUtils.is_loved(val), SurveyUtils.is_hated(val)))

        return q_data


class SurveyRandomRespondentModel:

    def __init__(self, conn: DbConnection, config: dict, favorite_apps_ids_int: list[int]):
        self.__config = config
        self.end_type = None

        self.__model: SurveyDbModel = SurveyDbModel(
            conn=conn,
            config=self.__config,
            client_data={},
            server_data={},
            ext_data={},
            survey_instance_guid='0'
        )

        while True:
            export = self.__model.export()

            while True:
                submit = {}
                page = export["config_data"]["id"]
                if page == "gender":
                    submit[page] = "m" if random.random() < 0.5 else "f"
                elif page == "favorite_game":
                    submit[page] = Utils.random_from_array(favorite_apps_ids_int)
                else:
                    for section in export["config_data"]["sections"]:
                        if section["answers_group"]["type"] == "radio":
                            if page == "genres_dcm" or page == "topics_dcm" or page == "concepts_dcm":
                                if random.random() < 0.1:
                                    submit[section["id"]] = "none"
                                else:
                                    rnd = Utils.random_from_array(section["answers_group"][page]["choices"])
                                    if rnd is not None:
                                        submit[section["id"]] = rnd["id"]
                            else:
                                rnd = Utils.random_from_array(section["answers_group"]["answers"])
                                if rnd is not None:
                                    submit[section["id"]] = rnd["id"]
                        elif section["answers_group"]["type"] == "check":
                            submit[section["id"]] = []
                            arr = section["answers_group"]["answers"].copy()
                            random.shuffle(arr)
                            for i in range(random.randrange(0, len(arr))):
                                submit[section["id"]].append(arr[i]["id"])

                ended, end_type = SurveyPageDbModel.is_screenout(
                    page=export["config_data"]["id"],
                    config_data=self.__config,
                    client_data=submit
                )
                if ended:
                    if end_type == "trap":
                        if random.random() < 0.01:
                            break
                    else:
                        if random.random() < 0.1:
                            break
                else:
                    break

            self.__model.submit(submit)
            export = self.__model.export()
            if "end_type" in export["config_data"]:
                self.end_type = export["config_data"]["end_type"]
                break

    def get_client_data(self):
        return self.__model.get_client_data_raw()

    def get_server_data(self):
        return self.__model.get_server_data_raw()
