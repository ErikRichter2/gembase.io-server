import json
import random

from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.tags.tags_constants import TagsConstants
from src.server.models.user.user_obfuscator import UserObfuscator


class PlayerExplorerSessionModel(BaseSessionModel):

    def __init__(self, session):
        super(PlayerExplorerSessionModel, self).__init__(session)
        self.__module_locked = self.session().models().billing().is_module_locked(
            BillingUtils.BILLING_MODULE_INSIGHT
        )

    def get_data(
            self,
            input_filters: [],
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

        rows_studies = self.conn().select_all("""
        SELECT m.id as survey_id, 
               us.guid, 
               us.name, 
               us.default_study, 
               us.dcm_concepts
          FROM app.users_studies us,
               survey_data.survey_meta m
         WHERE (us.user_id = %s OR us.global_study = 1)
           AND (us.state = 'done' OR us.state = 'working')
           AND m.survey_control_guid = us.guid
        """, [self.user_id()])

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
              FROM platform.platform_values_survey_info r
             WHERE r.survey_meta_id = %s
            """
            rows_cnt_full[i] = self.conn().select_one(query, [survey_id])

            query = f"""
            SELECT age_group, 
                   count(1) as cnt 
              FROM (
                    SELECT CASE WHEN r.age >= 0 AND r.age <= 29 THEN 0
                                WHEN r.age >= 30 AND r.age <= 45 THEN 1
                                ELSE 2 END as age_group
                      FROM platform.platform_values_survey_info r
                     WHERE r.survey_meta_id = %s
                    ) z1
              GROUP BY z1.age_group
              ORDER BY z1.age_group
            """
            rows_age_groups_full[i] = self.conn().select_all(query, [survey_id])

            filter_query = self.__get_survey_filter_query(
                survey_id=survey_id,
                input_filter=input_filters[i],
            )

            PlatformValuesHelper.recreate_table(
                conn=self.conn(),
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

            cnt_query = f"""
            SELECT count(1) as cnt, 
                   SUM(r.female) as females_cnt 
              FROM platform_values.tmp_survey_info_filter_{i} f,
                   platform.platform_values_survey_info r
             WHERE f.respondent_id = r.survey_instance_int
               AND r.survey_meta_id = %s
            """
            rows_cnt[i] = self.conn().select_one(cnt_query, [survey_id])

            age_groups_query = f"""
            SELECT age_group, 
                   count(1) as cnt 
              FROM (
                    SELECT CASE WHEN r.age >= 0 AND r.age <= 29 THEN 0
                                WHEN r.age >= 30 AND r.age <= 45 THEN 1
                                ELSE 2 END as age_group
                      FROM platform_values.tmp_survey_info_filter_{i} f,
                           platform.platform_values_survey_info r
                     WHERE f.respondent_id = r.survey_instance_int
                       AND r.survey_meta_id = %s
                   ) z1
             GROUP BY z1.age_group
             ORDER BY z1.age_group
            """
            rows_age_groups[i] = self.conn().select_all(age_groups_query, [survey_id])

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
                                               platform.platform_values_survey_tags t,
                                               app.def_sheet_platform_product p
                                         WHERE f.respondent_id = t.survey_instance_int
                                           AND t.survey_meta_id = %s
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
                rows_per_filter[i] = self.conn().select_all(query, [survey_id])

                # flags
                rows_flags[i] = self.conn().select_all(f"""
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
                                  FROM platform.platform_values_survey_flags fl,
                                       platform_values.tmp_survey_info_filter_{i} f,
                                       app.def_sheet_platform_product p
                                 WHERE fl.survey_id = %s
                                   AND fl.survey_instance_int = f.respondent_id
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
                TagsConstants.SUBCATEGORY_GENRE_ID,
                #TagsConstants.SUBCATEGORY_CORE_ID,
                TagsConstants.SUBCATEGORY_BEHAVIORS_ID,
                TagsConstants.SUBCATEGORY_MULTIPLAYER_ID,
                TagsConstants.SUBCATEGORY_COMPLEXITY_ID,
                TagsConstants.SUBCATEGORY_ACTIVITIES_ID,
                #TagsConstants.SUBCATEGORY_PLATFORMS_ID,
                TagsConstants.SUBCATEGORY_SOCIALS_ID,
                TagsConstants.SUBCATEGORY_HOBBIES_ID,
                TagsConstants.SUBCATEGORY_MOVIES_ID,
            ],
            [
                TagsConstants.SUBCATEGORY_TOPICS_ID,
                TagsConstants.SUBCATEGORY_DOMAINS_ID,
                TagsConstants.SUBCATEGORY_NEEDS_ID,
                TagsConstants.SUBCATEGORY_FOCUS_ID,
                TagsConstants.SUBCATEGORY_ERAS_ID,
                TagsConstants.SUBCATEGORY_ENVIRONMENT_ID,
                TagsConstants.SUBCATEGORY_ENTITIES_ID,
                TagsConstants.SUBCATEGORY_ROLES_ID,
            ]
        ]

        if show == "concepts":
            charts_data = self.__get_concepts_data(survey_guid=active_survey)
        else:
            dataset_2_tag_id_int_map = {}
            for i in range(1, len(rows_per_filter)):
                for row in rows_per_filter[i]:
                    if row[UserObfuscator.TAG_ID_INT] not in dataset_2_tag_id_int_map:
                        dataset_2_tag_id_int_map[row[UserObfuscator.TAG_ID_INT]] = []
                    dataset_2_tag_id_int_map[row[UserObfuscator.TAG_ID_INT]].append(float(row["loved_ratio"]))

            charts_data_tag_id = {}

            rows_goal_values = self.conn().select_all("""
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

                if self.__module_locked:
                    if subcategory_int != TagsConstants.SUBCATEGORY_GENRE_ID and subcategory_int != TagsConstants.SUBCATEGORY_TOPICS_ID:
                        chart_locked = True

                if subcategory_int not in charts_data_tag_id:
                    charts_data_tag_id[subcategory_int] = {
                        "id": subcategory_int,
                        "name": subcategory_name,
                        "data": [],
                        "locked": chart_locked
                    }
                    if goal_value is not None:
                        charts_data_tag_id[subcategory_int]["goal_value"] = goal_value
                    if subcategory_int == TagsConstants.SUBCATEGORY_BEHAVIORS_ID:
                        charts_data_tag_id[subcategory_int]["template"] = "behaviors"
                    charts_data.append(charts_data_tag_id[subcategory_int])

                if not chart_locked:
                    values = [0] * filters_cnt
                    values[0] = float(row["loved_ratio"])
                    if row[UserObfuscator.TAG_ID_INT] in dataset_2_tag_id_int_map:
                        for i in range(len(dataset_2_tag_id_int_map[row[UserObfuscator.TAG_ID_INT]])):
                            values[i + 1] = dataset_2_tag_id_int_map[row[UserObfuscator.TAG_ID_INT]][i]
                    description = ""
                    if row["description"] is not None:
                        description = row["description"]

                    locked = self.__module_locked and row["unlocked"] != 1

                    if not locked:
                        charts_data_tag_id[subcategory_int]["data"].append({
                            UserObfuscator.TAG_ID_INT: row[UserObfuscator.TAG_ID_INT],
                            "id": self.session().user().obfuscator().server_to_client(row[UserObfuscator.TAG_ID_INT]),
                            "values": values,
                            "name": row["node"],
                            "desc": description,
                        })

                    # sort behaviors
                    if subcategory_int == TagsConstants.SUBCATEGORY_BEHAVIORS_ID:
                        tmp_arr = []

                        for tag_id_int in [
                            TagsConstants.BEHAVIOR_UTILIZE,
                            TagsConstants.BEHAVIOR_PLAN,
                            TagsConstants.BEHAVIOR_CULTIVATE,
                            TagsConstants.BEHAVIOR_BOND,
                            TagsConstants.BEHAVIOR_IMMERSE,
                            TagsConstants.BEHAVIOR_EXPLORE,
                            TagsConstants.BEHAVIOR_IMPROVISE,
                            TagsConstants.BEHAVIOR_DESTROY,
                            TagsConstants.BEHAVIOR_CHALLENGE,
                            TagsConstants.BEHAVIOR_EXPRESS,
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
                        "locked": self.__module_locked,
                    }
                    if goal_value is not None:
                        charts_data_tag_id[subcategory_id]["goal_value"] = goal_value
                    charts_data.append(charts_data_tag_id[subcategory_id])
                if not self.__module_locked:
                    charts_data_tag_id[subcategory_id]["data"].append({
                        "id": self.session().user().obfuscator().server_to_client(row[UserObfuscator.TAG_ID_INT]),
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
                stats["full_age"][row["age_group"]] = round(int(row["cnt"]) / total_rows * 100, 2)
            stats["filtered_age"] = [0, 0, 0]
            if filtered_rows > 0:
                for row in rows_age_groups[i]:
                    stats["filtered_age"][row["age_group"]] = round(int(row["cnt"]) / filtered_rows * 100, 2)

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
            input_filter: {}
    ) -> str:

        row_age = self.conn().select_one("""
        SELECT MIN(r.age) as min_age,
               MAX(r.age) as max_age
          FROM platform.platform_values_survey_info r
         WHERE r.survey_meta_id = %s
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

        row_females = self.conn().select_one("""
        SELECT SUM(r.female) as female_cnt,
               COUNT(1) as total_cnt
          FROM platform.platform_values_survey_info r
         WHERE r.survey_meta_id = %s
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

        if self.__module_locked:
            input_tags_not_hated = [] if "not_hated" not in input_filter else input_filter["not_hated"][UserObfuscator.TAG_IDS_INT]
            input_tags_loved = [] if "loved" not in input_filter else input_filter["loved"][UserObfuscator.TAG_IDS_INT]
            allowed_tags_def = self.session().models().billing().get_allowed_tags(
                BillingUtils.BILLING_MODULE_INSIGHT
            )
            tags_not_hated_new = []
            tags_loved_new = []
            for it in allowed_tags_def:
                if it["is_changeable"] == 1 and it["tag_id_int"] in input_tags_not_hated:
                    tags_not_hated_new.append(it["tag_id_int"])
                if it["is_changeable"] == 1 and it["tag_id_int"] in input_tags_loved:
                    tags_loved_new.append(it["tag_id_int"])
            input_filter["loved"][UserObfuscator.TAG_IDS_INT] = tags_loved_new
            input_filter["not_hated"][UserObfuscator.TAG_IDS_INT] = tags_not_hated_new

        loved = [-1]
        if "loved" in input_filter and len(input_filter["loved"][UserObfuscator.TAG_IDS_INT]) > 0:
            loved = input_filter["loved"][UserObfuscator.TAG_IDS_INT]
        loved_db = self.conn().values_arr_to_db_in(loved, int_values=True)
        not_hated = [-1]
        if "not_hated" in input_filter and len(input_filter["not_hated"][UserObfuscator.TAG_IDS_INT]) > 0:
            not_hated = input_filter["not_hated"][UserObfuscator.TAG_IDS_INT]
        not_hated_db = self.conn().values_arr_to_db_in(not_hated, int_values=True)

        query = f"""
        SELECT z1.survey_instance_int as respondent_id
          FROM (
                SELECT r.survey_meta_id,
                       r.survey_instance_int,
                       r.female,
                       ROW_NUMBER() over (PARTITION BY r.female) as row_num_female
                  FROM platform.platform_values_survey_info r
                 WHERE r.survey_meta_id = {survey_id}
                   AND r.age >= {input_filter["age"]["from"]}
                   AND r.age <= {input_filter["age"]["to"]}   
                   AND {spending_condition_db}
           ) z1
           INNER JOIN platform.platform_values_survey_info r
              ON r.survey_instance_int = z1.survey_instance_int
             AND r.survey_meta_id = z1.survey_meta_id
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
           INNER JOIN platform.platform_values_survey_tags_bin tb
              ON tb.survey_id = r.survey_meta_id
             AND tb.survey_instance_int = r.survey_instance_int
             AND (BIT_COUNT(tb.b_loved & loved_bin.b) = loved_bin.cnt OR loved_bin.cnt = 0)
             AND (BIT_COUNT(tb.b_rejected & not_hated_bin.b) = 0 OR not_hated_bin.cnt = 0)
           WHERE ((z1.female = 1 AND z1.row_num_female <= {max_females_cnt})
              OR (z1.female = 0 AND z1.row_num_female <= {max_males_cnt}))
        """

        return query

    def __get_concepts_data(
            self,
            survey_guid: int
    ):
        config = json.loads(self.conn().select_one("""
        SELECT s.config
          FROM survey_results.surveys s
         WHERE s.guid = %s
        """, [survey_guid])["config"])

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
