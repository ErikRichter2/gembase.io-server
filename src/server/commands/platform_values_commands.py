import json

from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from src.server.models.platform_values.models.base.platform_values_calc_base_model import PlatformValuesCalcBaseModel
from src.server.models.platform_values.models.factory.platform_values_calc_model_factory import \
    PlatformValuesCalcModelFactory
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.session.models.platform_session_model import PlatformSessionModel
from src.server.models.survey.survey_data_model import SurveyDataModel
from src.server.models.tags.tags_mapper import TagsMapper
from src.server.models.user.user_data import UserData
from src.server.models.user.user_obfuscator import UserObfuscator
from src.session.session import is_logged, gb_session, is_admin


@command("platform_values:init_calc", [is_logged])
def platform_values__init_calc(data: CommandData):
    conn = gb_session().conn()
    user_id = gb_session().user_id()
    survey_id = SurveyDataModel.get_survey_meta_id(
        conn=conn,
        survey_control_guid=PlatformSessionModel.SURVEY_CONTROL_GUID
    )

    calc = data.payload["calc"]
    input_data = data.payload["input_data"]

    m = PlatformValuesCalcModelFactory.create(
        conn=conn,
        user_id=user_id,
        survey_id=survey_id,
        calc=calc
    ).set_input_client_data(
        input_data=input_data,
        obfuscator=gb_session().user().obfuscator()
    )

    cached_result = m.get_cached_result(
        obfuscator=gb_session().user().obfuscator(),
        is_admin=gb_session().is_admin()
    )
    if cached_result is not None:
        return cached_result

    m.add_to_queue()

    data.payload["hash_key"] = m.hash_key
    data.payload["calc"] = calc
    return platform_values__check_calc(data)


@command("platform_values:check_calc", [is_logged])
def platform_values__check_calc(data: CommandData):
    conn = gb_session().conn()
    user_id = gb_session().user_id()
    survey_id = SurveyDataModel.get_survey_meta_id(
        conn=conn,
        survey_control_guid=PlatformSessionModel.SURVEY_CONTROL_GUID
    )

    generate_client_data = False
    if "generate_client_data" in data.payload:
        generate_client_data = data.payload["generate_client_data"]

    hash_key = None
    if "hash_key" in data.payload:
        hash_key = data.payload["hash_key"]

    if hash_key is None:
        raise Exception(f"Hash key not found")

    calc = data.payload["calc"]
    batch_user_id = UserData.demo_batch_user_id(conn=gb_session().conn())

    current_version = PlatformValuesHelper.get_calc_version(conn=conn)

    def get_request_data():

        request_row = conn.select_one_or_none("""
            SELECT platform_id, state, progress_data, version, input_data, user_id
              FROM platform_values.requests
             WHERE user_id IN (%s, %s)
               AND hash_key = %s
               AND survey_id = %s
               AND calc = %s
               AND state NOT IN ('error', 'killed')
             ORDER BY platform_id DESC
             LIMIT 1
            """, [user_id, batch_user_id, hash_key, survey_id, calc])

        if request_row is None:
            return None

        if request_row["version"] == current_version:
            return request_row
        else:
            PlatformValuesCalcBaseModel.create_queue(
                user_id=request_row["user_id"],
                calc=calc,
                hash_key=hash_key,
                survey_id=survey_id,
                input_data=request_row["input_data"]
            )

            return None

    row_request = get_request_data()

    if row_request is None:
        queue_request = conn.select_one_or_none("""
            SELECT platform_id
              FROM platform_values.requests_queue
             WHERE user_id = %s
               AND hash_key = %s
               AND survey_id = %s
               AND calc = %s
             ORDER BY t DESC
             LIMIT 1
            """, [user_id, hash_key, survey_id, calc])

        if queue_request is None:
            return {
                "metadata": {
                    "state": "error",
                    "hash_key": hash_key,
                    "error_code": "not_found"
                }
            }

        return {
            "metadata": {
                "state": "queue",
                "hash_key": hash_key
            }
        }

    conn.rollback()

    state = row_request["state"]

    res = {
        "payload": {},
        "metadata": {
            "hash_key": hash_key,
            "state": state
        }
    }

    if state == "working":
        if "progress_data" in row_request and row_request["progress_data"] is not None:
            res["payload"]["progress_data"] = json.loads(row_request["progress_data"])

    if state == "done" or generate_client_data:
        res["payload"]["result_data"] = PlatformValuesCalcModelFactory.create(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id,
            calc=calc,
        ).generate_client_data(
            platform_id=row_request["platform_id"],
            obfuscator=gb_session().user().obfuscator(),
            is_admin=gb_session().is_admin()
        )

    return res


@command("platform_values:get_most_hated_tags", [is_logged])
def platform_values__get_most_hated_tags(data: CommandData):
    return {
        "top_loved": gb_session().models().platform().get_top_loved_apps(
            audience_angle_id_int=data.get(UserObfuscator.AUDIENCE_ANGLE_ID_INT)
        ),
        "most_hated": gb_session().models().platform().get_most_hated_tags(
            loved_tag_ids_int=data.payload["loved_tags"][UserObfuscator.TAG_IDS_INT],
            hated_tag_ids_int=data.payload["hated_tags"][UserObfuscator.TAG_IDS_INT]
        )
    }


@command("platform_values:get_ts_similarity_log", [is_admin])
def platform_values__get_ts_similarity_log(data: CommandData):
    platform_id = data.payload["platform_id"]
    competitor_app_id = data.payload["competitor"][UserObfuscator.APP_ID_INT]

    row_platform = gb_session().conn().select_one("""
    SELECT r.input_data, r.survey_id
      FROM platform_values.requests r
     WHERE r.platform_id = %s
    """, [platform_id])

    map_tags = TagsMapper(conn=gb_session().conn()).map_tags

    c_tags = [it["tag_id_int"] for it in gb_session().models().tags().get_tags(app_id_int=competitor_app_id)["tags"]]

    input_data = json.loads(row_platform["input_data"])

    my_tags = [it["tag_id_int"] for it in input_data["tag_details"]]
    my_tags_db = gb_session().conn().values_arr_to_db_in(my_tags, int_values=True)

    if "tags_weights" in input_data and input_data["tags_weights"] is not None:
        for it in input_data["tags_weights"]:
            subcategory_int = it["subcategory_int"]
            for tag_id_int in map_tags["subci2i"][subcategory_int]:
                map_tags["competitors_pool_w"][tag_id_int] = it["weight"]

    survey_id = row_platform["survey_id"]

    query = f"""
    SELECT z1.tag_id_int, 
           ROUND(z1.loved_cnt / my_angle.loved_cnt * 100) as w
      FROM (
            SELECT d.tag_id_int, 
                   COUNT(1) AS loved_cnt
              FROM (
                    SELECT stb.survey_instance_int
                      FROM (
                               SELECT aa.b as b, aa.angle_cnt
                                 FROM platform.audience_angle aa,
                                      platform_values.results_audience_angles__competitors r
                                WHERE r.platform_id = {platform_id}
                                  AND r.audience_angle_id = aa.id
                           ) my_angle,
                           platform.platform_values_survey_tags_bin stb,
                           (
                              SELECT BIT_OR(db.b) as b
                                FROM platform.def_tags_bin db
                               WHERE db.tag_id_int IN ({my_tags_db})
                           ) my_tags
                     WHERE stb.survey_id = {survey_id}
                       AND BIT_COUNT(stb.b_loved & my_angle.b) = my_angle.angle_cnt
                       AND BIT_COUNT(stb.b_rejected & my_tags.b) = 0
                   ) z1,
                   platform.platform_values_survey_tags st,
                   platform.def_tags d
             WHERE st.survey_meta_id = {survey_id}
               AND st.survey_instance_int = z1.survey_instance_int
               AND st.tag_id_int = d.tag_id_int
               AND st.loved = 1
             GROUP BY d.tag_id_int
           ) z1,
           (
               SELECT aa.b as b, aa.angle_cnt, aap.loved_cnt
                 FROM platform.audience_angle aa,
                      platform.audience_angle_potential aap,
                      platform_values.results_audience_angles__competitors r
                WHERE r.platform_id = {platform_id}
                  AND r.audience_angle_id = aa.id
                  AND aap.id = aa.id
           ) my_angle
    """

    rows_survey_w = gb_session().conn().select_all(query)

    survey_w = {}
    for row in rows_survey_w:
        survey_w[row["tag_id_int"]] = float(row["w"])

    my_and_c_tags = []
    my_and_not_c_tags = []
    c_and_not_my_tags = []

    for ct in c_tags:
        if ct in my_tags:
            my_and_c_tags.append(ct)

    for ct in c_tags:
        if ct not in my_tags:
            c_and_not_my_tags.append(ct)

    for mt in my_tags:
        if mt not in c_tags:
            my_and_not_c_tags.append(mt)

    res = {
        "same_tags": {UserObfuscator.TAG_IDS_INT: my_and_c_tags},
        "only_my_tags": {UserObfuscator.TAG_IDS_INT: my_and_not_c_tags},
        "only_c_tags": {UserObfuscator.TAG_IDS_INT: c_and_not_my_tags},
        "c_w": [{
            UserObfuscator.TAG_ID_INT: tag_id_int,
            "w": map_tags["competitors_pool_w"][tag_id_int]
        } for tag_id_int in map_tags["competitors_pool_w"]],
        "ts_sim_w": [{
            UserObfuscator.TAG_ID_INT: tag_id_int,
            "w": map_tags["threatscore_similarity_w"][tag_id_int]
        } for tag_id_int in map_tags["threatscore_similarity_w"]],
        "survey_w": [{
            UserObfuscator.TAG_ID_INT: tag_id_int,
            "w": survey_w[tag_id_int]
        } for tag_id_int in survey_w]
    }

    return res
