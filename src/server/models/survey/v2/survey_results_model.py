import json
import random

import numpy as np

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.dms.dms_constants import DmsConstants
from src.server.models.dms.dms_model import DmsCache
from src.server.models.survey.survey_utils import SurveyUtils
from src.server.models.tags.tags_mapper import TagsMapper
from src.server.models.survey.survey_config_model import SurveyConfigModel
from src.server.models.dms.dms_model import DmsModel
from scipy import stats


class SurveyResultsModel:

    __survey_dcm_map = {
        "a236b79f-6d2e-4f23-ad92-d018cb518346": "0ce01ac6-d0b0-4b79-b89c-a68f4c7a783b"
    }

    def __init__(self, conn: DbConnection, survey_guid: str, name: str, config: any, audiences: any):
        self.__conn = conn
        self.__survey_guid = survey_guid
        self.__name = name
        self.__map_tag_id: {} = None
        self.__config = config
        self.__audiences = audiences
        self.__sheet_config = DmsCache.get_from_cache(conn=conn, guid=DmsConstants.survey_v2_config)
        self.__sheet_texts = DmsCache.get_from_cache(conn=conn, guid=DmsConstants.survey_v2_texts)["data"]

    def clear_results(self):

        row_survey_id = self.__conn.select_one_or_none("""
        SELECT s.id
          FROM survey_results.surveys s
         WHERE s.guid = %s
        """, [self.__survey_guid])

        survey_id = None if row_survey_id is None else row_survey_id["id"]

        if survey_id is not None:

            self.__conn.query("""
            DELETE FROM survey_results.surveys s WHERE s.id = %s
            """, [survey_id])

            self.__conn.query("""
            DELETE FROM survey_results.respondents s WHERE s.survey_id = %s
            """, [survey_id])

            self.__conn.query("""
            DELETE FROM survey_results.flags s WHERE s.survey_id = %s
            """, [survey_id])

            self.__conn.query("""
            DELETE FROM survey_results.tags s WHERE s.survey_id = %s
            """, [survey_id])

            self.__conn.query("""
            DELETE FROM survey_results.competitors s WHERE s.survey_id = %s
            """, [survey_id])

            self.__conn.query("""
            DELETE FROM survey_results.dcm_genres s WHERE s.survey_id = %s
            """, [survey_id])

            self.__conn.query("""
            DELETE FROM survey_results.dcm_topics s WHERE s.survey_id = %s
            """, [survey_id])

            self.__conn.query("""
            DELETE FROM survey_results.dcm_concepts s WHERE s.survey_id = %s
            """, [survey_id])

        return survey_id

    def __get_map_tag_id(self):
        if self.__map_tag_id is None:
            self.__map_tag_id = TagsMapper.instance(self.__conn)
        return self.__map_tag_id["s2i"]

    def __migrate(self, raw_results: []):
        version = 0

        if "__version" in self.__config:
            version = self.__config["__version"]

        if version == SurveyConfigModel.CURRENT_SURVEY_VERSION:
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
                rows_apps = self.__conn.select_all(f"""
                SELECT m.app_id_int, m.app_id_in_store
                  FROM app.map_app_id_to_store_id m
                 WHERE m.app_id_in_store IN ({self.__conn.values_arr_to_db_in(app_ids)})
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

        row_survey_id = self.__conn.select_one_or_none("""
        SELECT s.id
          FROM survey_results.surveys s
         WHERE s.guid = %s
        """, [self.__survey_guid])

        survey_id = None if row_survey_id is None else row_survey_id["id"]

        map_respondent_guid_to_id = {}

        if survey_id is None:
            survey_id = self.__conn.insert("""
            INSERT INTO survey_results.surveys (guid) VALUES (%s)
            """, [self.__survey_guid])

        if is_simulate:
            rows_raw_results = self.__conn.select_all("""
            SELECT r.respondent_guid, r.client_data, r.server_data
              FROM survey_raw_simulated.results r
             WHERE r.survey_guid = %s
               AND r.end_type = 'completed'
            """, [self.__survey_guid])
        else:
            rows_raw_results = self.__conn.select_all("""
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
            topics_dcm_tags = SurveyResultsModel.__get_simulate_topics_dcm_results(
                survey_id=survey_id,
                topics_choices_map=topics_choices_map
            )
            bulk_data_tags += topics_dcm_tags
            genres_dcm_tags = SurveyResultsModel.__get_simulate_genres_dcm_results(
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

        self.__conn.query("""
        INSERT INTO survey_results.surveys (id, guid, is_simulated, audiences, config, name) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        """, [survey_id, self.__survey_guid, is_simulate, json.dumps(self.__audiences),
              json.dumps(self.__config), self.__name])

        self.__conn.bulk("""
        INSERT INTO survey_results.respondents (survey_id, respondent_id, guid, female, age, role, favorite_game, spending, playing_time, loyalty) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """, bulk_data_respondents)

        self.__conn.bulk("""
        INSERT INTO survey_results.flags (survey_id, respondent_id, flag_id, flag_value, tag_id_int) 
        VALUES (%s, %s, %s, %s, %s) 
        """, bulk_data_flags)

        self.__conn.bulk("""
        INSERT INTO survey_results.tags (survey_id, respondent_id, tag_id_int, tag_value, loved, hated) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        """, bulk_data_tags)

        self.__conn.bulk("""
        INSERT INTO survey_results.competitors (survey_id, respondent_id, app_id_int, value, loved) 
        VALUES (%s, %s, %s, %s, %s) 
        """, bulk_data_competitors)

        self.__conn.bulk("""
        INSERT INTO survey_results.dcm_genres (survey_id, respondent_id, dcm_order, genre_1, genre_2,
        core_1, core_2, chosen) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
        """, bulk_data_genres_dcm)

        self.__conn.bulk("""
        INSERT INTO survey_results.dcm_topics (survey_id, respondent_id, dcm_order, topic, feature_1a,
        feature_1b, feature_2a, feature_2b, chosen_a, chosen_b) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """, bulk_data_topics_dcm)

        self.__conn.bulk("""
        INSERT INTO survey_results.dcm_concepts (survey_id, respondent_id, dcm_order,
        card_index, header_id, feature_1_id, feature_2_id, chosen) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
        """, bulk_data_concepts_dcm)

        bin_bytes_cnt = self.__conn.select_one("""
        SELECT b.cnt
          FROM platform.bin_bytes_cnt b
        """)["cnt"]

        self.__conn.query(f"""
        alter table survey_results.tags_bin modify b binary({bin_bytes_cnt}) not null;
        """)
        self.__conn.query(f"""
        alter table survey_results.tags_bin modify b_loved binary({bin_bytes_cnt}) not null;
        """)
        self.__conn.query(f"""
        alter table survey_results.tags_bin modify b_hated binary({bin_bytes_cnt}) not null;
        """)

        self.__conn.query("""
        DELETE FROM survey_results.tags_bin tb WHERE tb.survey_id = %s
        """, [survey_id])
        self.__conn.query("""
        INSERT INTO survey_results.tags_bin (survey_id, respondent_id, b, b_loved, b_hated)
        SELECT t.survey_id, t.respondent_id, BIT_OR(d.b), BIT_OR(IF(t.loved, d.b, z.b)), BIT_OR(IF(t.hated, d.b, z.b))
          FROM survey_results.tags t,
               platform.def_tags_bin d,
               platform.zero_bin_value z
         WHERE t.survey_id = %s
           AND d.tag_id_int = t.tag_id_int
         GROUP BY t.survey_id, t.respondent_id
        """, [survey_id])

        self.__conn.commit()

        print("DONE !")

    @staticmethod
    def __get_simulate_genres_dcm_results(survey_id: int, genres_core_choices_map: []):
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
    def __get_simulate_topics_dcm_results(survey_id: int, topics_choices_map: []):
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

    def __get_external_dcm_results(self, survey_id: int, map_respondent_guid_to_id: {}):

        if self.__survey_guid not in SurveyResultsModel.__survey_dcm_map:
            return

        df = DmsModel.read_df_from_external_data(self.__conn, SurveyResultsModel.__survey_dcm_map[self.__survey_guid])

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
