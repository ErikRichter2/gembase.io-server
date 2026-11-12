import json

from server_core.commands.command_data import CommandData
from server_core.commands.commands_handler import CommandsHandler
from server_core.utils.utils import Utils
from src.db.calc.calc_utils import CalcUtils
from src.db.calc.factory.base_calc_interface import BaseCalcInterface
from src.db.calc.factory.calc_factory import CalcFactory
from src.db.surveys_db_model import SurveysDbModel
from src.models.user_obfuscator import UserObfuscator
from src.session.users_session import UsersSession


calc_commands_handler = CommandsHandler(
    session_class=UsersSession
)


@calc_commands_handler.register("calc:init_calc")
def calc__init(session: UsersSession, data: CommandData):
    conn = session.conn()
    user_id = session.user_db.user_id
    survey_id = SurveysDbModel(conn=session.conn()).get_survey_meta_id(SurveysDbModel.SURVEY_CONTROL_GUID)
    
    if survey_id is None:
        raise Exception('Unknown survey_id')

    calc_type = data.ensure('calcType', Utils.is_string)
    calc_options = data.payload["calcOptions"]
    input_data = calc_options["input"]

    m = CalcFactory.create(
        conn=conn,
        user_id=user_id,
        survey_id=survey_id,
        calc=calc_type
    ).set_input_client_data(
        input_data=input_data,
        obfuscator=session.user_db.obfuscator
    )

    cached_result = m.get_cached_result(obfuscator=session.user_db.obfuscator)
    if cached_result is not None:
        return cached_result

    m.add_to_queue()

    data.payload["hash_key"] = m.hash_key
    data.payload["calc"] = calc_type
    return calc__check_calc(session, data)


@calc_commands_handler.register("calc:check_calc")
def calc__check_calc(session: UsersSession, data: CommandData):
    conn = session.conn()
    user_id = session.user_db.user_id
    survey_id = SurveysDbModel(conn=session.conn()).get_survey_meta_id(SurveysDbModel.SURVEY_CONTROL_GUID)
    
    if survey_id is None:
        raise Exception('Unknown survey_id')

    generate_client_data = False
    if "generate_client_data" in data.payload:
        generate_client_data = data.payload["generate_client_data"]

    hash_key = None
    if "hash_key" in data.payload:
        hash_key = data.payload["hash_key"]

    if hash_key is None:
        raise Exception(f"Hash key not found")

    calc = data.payload["calc"]
    batch_user_id = session.user_db.demo_batch_user_id()

    current_version = CalcUtils(conn=conn).get_calc_version()

    def get_request_data():

        request_row = conn.select_one_or_none("""
            SELECT platform_id, state, progress_data, version, input_data, user_id
              FROM gb_calc.requests
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
            BaseCalcInterface.create_queue(
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
              FROM gb_calc.requests_queue
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
        res["payload"]["result_data"] = CalcFactory.create(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id,
            calc=calc,
        ).generate_client_data(
            platform_id=row_request["platform_id"],
            obfuscator=session.user_db.obfuscator
        )

    return res


@calc_commands_handler.register("calc:get_most_hated_tags")
def calc__get_most_hated_tags(session: UsersSession, data: CommandData):
    return session.user_db.games_db.get_most_hated_tags(
        loved_tag_ids_int=data.payload["loved_tags"][UserObfuscator.TAG_IDS_INT],
        hated_tag_ids_int=data.payload["hated_tags"][UserObfuscator.TAG_IDS_INT]
    )
