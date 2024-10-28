import json

import flask
import requests

from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.environment.runtime_constants import rr
from gembase_server_core.private_data.private_data_model import PrivateDataModel
from src import external_api
from src.external_api.gmail import GbEmailService
from src.server.models.app_store_search.app_store_search import AppStoreSearch
from src.server.models.dms.dms_constants import DmsConstants
from src.server.models.dms.dms_model import DmsCache, DmsModel
from src.server.models.survey.v2.survey_model_v2 import SurveyModelV2, SurveyConfigModelV2
from src.server.models.survey.v2.survey_page_model import get_export_for_end_page
from src.server.models.quota.survey_search_app_quota_context import SurveySearchAppQuotaContext
from src.session.session import gb_session, is_admin
from src.utils.gembase_utils import GembaseUtils


def return_end_response(state: int, config_guid: str):
    if state == 0 or state == 1:
        return None

    row = gb_session().conn().select_one_or_none("""
        SELECT cfg.config
          FROM survey_v2.survey_config cfg
         WHERE cfg.guid = %s
        """, [config_guid])

    if row is None:
        flask.abort(404)

    if state == 2:
        export_data = get_export_for_end_page("quota", json.loads(row["config"]))
    elif state == 3:
        export_data = get_export_for_end_page("screenout", json.loads(row["config"]))
    elif state == 4:
        export_data = get_export_for_end_page("completed", json.loads(row["config"]))
    elif state == 5:
        export_data = get_export_for_end_page("trap", json.loads(row["config"]))
    else:
        raise Exception(f"Unknow state: {state}")

    return {
        "state": 1,
        "data": export_data
    }


def check_for_end_type_in_export(guid: str, export):
    if "end_type" in export["config_data"]:
        end_type = export["config_data"]["end_type"]
        if end_type == "screenout":
            state = 3
        elif end_type == "completed":
            state = 4
        elif end_type == "trap":
            state = 5
        else:
            raise Exception(f"Unknown end_type: {end_type}")

        gb_session().conn().query("""
        UPDATE survey_v2.survey_whitelist w
           SET w.state = %s,
               w.t = NOW()
         WHERE w.gid = %s
        """, [state, guid])


@command("survey:v2:public:get")
def survey_v2_public_get(data: CommandData):

    if "guid" not in data.payload:
        flask.abort(404)

    guid = data.payload["guid"]

    row = gb_session().conn().select_one_or_none("""
    SELECT w.state, c.survey_config
      FROM survey_v2.survey_whitelist w,
           survey_v2.survey_control c
     WHERE w.gid = %s
       AND w.survey_control = c.id
    """, [guid])

    if row is not None:
        state = row["state"]
        if state != 0 and state != 1:
            return return_end_response(state, row["survey_config"])

    model = SurveyModelV2.load_instance(guid)

    if model is None:

        if "cid" not in data.payload:
            flask.abort(404)
        cid = data.payload["cid"]

        param_s = None
        param_a = None
        param_y = None
        if "s" in data.payload:
            param_s = data.payload["s"]
        if "a" in data.payload:
            param_a = data.payload["a"]
        if "y" in data.payload:
            param_y = data.payload["y"]

        row_w = gb_session().conn().select_one_or_none("""
        SELECT w.id, w.gid as instance_guid, c.pool, w.pool_id, c.survey_config, w.state
          FROM survey_v2.survey_whitelist w,
               survey_v2.survey_control c
         WHERE w.gid = %s
           AND w.survey_control = c.id
           AND c.state != -1
        """, [guid])

        if row_w is None:
            flask.abort(404)

        pool = json.loads(row_w["pool"])
        pool_item = None
        for it in pool:
            if it["id"] == row_w["pool_id"]:
                pool_item = it
                break

        if pool_item is None:
            flask.abort(404)

        quota = pool_item["quota"]
        lang = pool_item["lang"]

        row_quota = gb_session().conn().select_one("""
        SELECT count(1) as cnt
          FROM survey_v2.survey_whitelist w
         WHERE w.pool_id = %s
           AND w.state = 4
        """, [row_w["pool_id"]])

        url_params = {
            "a": param_a,
            "y": param_y,
            "s": param_s
        }

        gb_session().conn().query("""
        UPDATE survey_v2.survey_whitelist w
           SET w.state = 1, 
               w.cid = %s,
               w.url_params = %s
         WHERE w.id = %s
        """, [cid, json.dumps(url_params), row_w["id"]])

        if row_quota["cnt"] >= quota:
            gb_session().conn().query("""
            UPDATE survey_v2.survey_whitelist w 
               SET w.state = 2,
                   w.t = NOW()
             WHERE w.id = %s
            """, [row_w["id"]])

            try:
                GbEmailService.send_mail(
                    subject=f"Gembase.io - ALERT Survey Quota Full ALERT",
                    body=f"Completed: {row_quota['cnt']}, Quota: {quota}",
                    to_address=["xxx@xxx.xxx"]
                )
            except Exception:
                pass

            return return_end_response(2, row_w["survey_config"])

        model = SurveyModelV2.create_instance(row_w["survey_config"], row_w["instance_guid"], lang)

    try:
        export = model.export()
        check_for_end_type_in_export(model.guid(), export)
        return {
            "state": 1,
            "data": export
        }
    except Exception as e:
        conn = DbConnection()
        conn.query("""
        UPDATE survey_v2.survey_instance si
           SET si.error_text = %s
         WHERE si.guid = %s
        """, [str(e), guid])
        conn.commit()
        conn.close()
        return {
            "state": -1
        }


def survey_v2_public_submit_recaptcha_v2_fail(data: CommandData):
    if "guid" not in data.payload:
        flask.abort(404)

    guid = data.payload["guid"]

    row = gb_session().conn().select_one_or_none("""
    SELECT w.state, c.survey_config
      FROM survey_v2.survey_whitelist w,
           survey_v2.survey_control c
     WHERE w.gid = %s
       AND w.survey_control = c.id
    """, [guid])

    if row is None:
        flask.abort(404)

    gb_session().conn().query("""
    UPDATE survey_v2.survey_whitelist w
       SET w.state = %s,
           w.t = NOW()
     WHERE w.gid = %s
    """, [5, guid])

    log_survey_error(guid, "recaptcha_v2", "")

    return return_end_response(5, row["survey_config"])


@command("survey:v2:public:submit_recaptcha_v2")
def survey_v2_public_submit_recaptcha_v2(data: CommandData):
    recaptcha = check_survey_recaptcha(data.payload, True)
    if recaptcha is not None:
        return survey_v2_public_submit_recaptcha_v2_fail(data)

    guid = data.payload["guid"]

    gb_session().conn().query("""
    UPDATE survey_v2.survey_instance si
       SET si.recaptcha_v2_verified = 1
     WHERE si.guid = %s
    """, [guid])

    return submit_public_internal(data)


def submit_public_internal(data: CommandData):
    guid = data.payload["guid"]
    client_data = data.payload["data"]

    row = gb_session().conn().select_one_or_none("""
    SELECT w.state
      FROM survey_v2.survey_whitelist w
     WHERE w.gid = %s
    """, [guid])

    if row is None or row["state"] != 1:
        flask.abort(404)

    model = SurveyModelV2.load_instance(guid)
    if model is None:
        flask.abort(404)

    try:
        model.submit(client_data)
        export = model.export()
        check_for_end_type_in_export(model.guid(), export)
        return {
            "state": 1,
            "data": export
        }
    except Exception as e:
        conn = DbConnection()
        conn.query("""
        UPDATE survey_v2.survey_instance si
           SET si.error_text = %s
         WHERE si.guid = %s
        """, [str(e), guid])
        conn.commit()
        conn.close()
        return {
            "state": -1,
        }


@command("survey:v2:public:submit")
def survey_v2_public_submit(data: CommandData):

    if "guid" not in data.payload:
        flask.abort(404)

    guid = data.payload["guid"]

    row = gb_session().conn().select_one_or_none("""
    SELECT si.recaptcha_v2_verified
      FROM survey_v2.survey_instance si
     WHERE si.guid = %s
    """, [guid])

    if row is None:
        flask.abort(404)

    recaptcha_v2_verified = row["recaptcha_v2_verified"]
    if recaptcha_v2_verified != 1:
        recaptcha = check_survey_recaptcha(data.payload)
        if recaptcha is not None:
            return {
                "state": -1,
                "data": {
                    "recaptcha": recaptcha
                }
            }

    return submit_public_internal(data)


def check_survey_recaptcha(payload, recaptcha_v2=False):
    if rr.is_prod():

        if "guid" not in payload:
            flask.abort(404)

        guid = payload["guid"]

        if 'recaptcha_token' not in payload:
            log_survey_error(guid, "recaptcha_token_missing", "")
            return {
                "state": "recaptcha_token_missing",
                "error_message": "Recaptcha not initialized ! Reloading page..."
            }

        recaptcha_token = payload["recaptcha_token"]

        secret_key = PrivateDataModel.get_private_data()["google"]["recaptcha"]["secret_key"]
        if recaptcha_v2:
            secret_key = PrivateDataModel.get_private_data()["google"]["recaptcha_v2"]["secret_key"]

        recaptcha_result = requests.post("https://www.google.com/recaptcha/api/siteverify", data={
            "secret": secret_key,
            "response": recaptcha_token,
            "remoteip": flask.request.remote_addr
        })

        if recaptcha_result.status_code != 200:
            log_survey_error(guid, "recaptcha_status_code", str(recaptcha_result.status_code))
            return {
                "state": "recaptcha_status_code",
                "error_message": "Recaptcha not initialized ! Reloading page..."
            }

        content = json.loads(recaptcha_result.text)

        if "success" in content:
            if not content["success"]:
                log_survey_error(guid, "recaptcha_not_success", "")
                return {
                    "state": "recaptcha_not_success",
                    "error_message": "Recaptcha not initialized ! Reloading page..."
                }

        if "score" in content:
            if content["score"] < 0.5:
                log_survey_error(guid, "recaptcha_score", str(content['score']))
                return {
                    "state": "recaptcha_score",
                    "error_message": "Recaptcha not validated ! Reloading page..."
                }

    return None


@command("survey:v2:public:get_apps")
def survey_v2_public_get_apps(data: CommandData):
    guid = data.payload["guid"]

    # if not SurveyModelV2.is_valid(guid):
    #     flask.abort(404)

    apps = AppStoreSearch.search_app_by_name(gb_session().conn(), 0, data.payload["app_title"], SurveySearchAppQuotaContext(guid))

    return apps


@command("survey:v2:get")
def survey_v2_get(data: CommandData):
    guid = data.payload["guid"]
    model = SurveyModelV2.load_instance(guid)
    if model is None:
        return {
            "state": 2
        }
    return {
        "state": 1,
        "data": model.export()
    }


@command("survey:v2:set_lang")
def survey_v2_set_lang(data: CommandData):
    guid = data.payload["guid"]
    lang = data.payload["lang"]
    gb_session().conn().query("""
    UPDATE survey_v2.survey_instance s
       SET s.lang = %s
     WHERE s.guid = %s
    """, [lang, guid])
    return {
        "state": 1,
    }


@command("survey:v2:submit")
def survey_v2_submit(data: CommandData):

    model = SurveyModelV2.load_instance(data.payload["guid"])
    if model is None:
        flask.abort(404)
    model.submit(data.payload["data"])
    return {
        "state": 1,
        "data": model.export()
    }


def log_survey_error(guid: str, error_code: str, error_text: str):
    gb_session().conn().query("""
    UPDATE survey_v2.survey_instance
       SET error_code = %s,
           error_text = %s
     WHERE guid = %s
    """, [error_code, error_text, guid])


@command("survey:v2:get_texts")
def survey_v2_get_texts(data: CommandData):
    config = DmsCache.get_json(gb_session().conn(), guid=DmsConstants.survey_v2_config)
    param_texts = []
    if "param_texts" in config:
        param_texts = config["param_texts"]
    survey_texts = DmsCache.get_json(gb_session().conn(), guid=DmsConstants.survey_v2_texts)
    return {
        "translations": survey_texts["texts"],
        "params": param_texts
    }


@command("admin:survey:v2:get_configs", [is_admin])
def admin_survey_v2_get_configs():
    survey_config_rows = gb_session().conn().select_all("""
    SELECT * FROM survey_v2.survey_config
    """)
    survey_preview_rows = gb_session().conn().select_all("""
    SELECT si.guid as survey_instance,
           si.survey_config,
           sp.user_id
      FROM survey_v2.survey_preview sp,
           survey_v2.survey_instance si
     WHERE sp.survey_instance = si.guid
    """)
    return {
        "survey_configs": survey_config_rows,
        "survey_previews": survey_preview_rows
    }


@command("admin:survey:v2:create_config", [is_admin])
def admin_survey_v2_create_config():
    config_guid = GembaseUtils.get_guid()
    instance_guid = GembaseUtils.get_guid()
    gb_session().conn().insert("""
        INSERT INTO survey_v2.survey_config (guid, config, user_id) VALUES (%s, %s, %s)
        """, [config_guid, json.dumps(SurveyConfigModelV2.get_config_from_dms()), gb_session().user_id()])
    model = SurveyModelV2.create_instance(config_guid, instance_guid)
    gb_session().conn().query("""
    INSERT INTO survey_v2.survey_preview (user_id, survey_instance) VALUES (%s, %s)
    """, [gb_session().user_id(), model.guid()])
    return {
        "survey_instance": model.guid()
    }


@command("admin:survey:v2:reset_config", [is_admin])
def admin_survey_v2_reset_config(data: CommandData):
    survey_v2_config = PrivateDataModel.get_private_data()["google"]["google_docs"]["survey_v2_config"]
    DmsModel.save_json_to_dms(gb_session().conn(), external_api.sheet_to_dict(survey_v2_config["sheet_id"]), guid=survey_v2_config['dms_guid'])
    survey_v2_texts = PrivateDataModel.get_private_data()["google"]["google_docs"]["survey_v2_texts"]
    DmsModel.save_json_to_dms(gb_session().conn(), external_api.sheet_to_dict(survey_v2_texts["sheet_id"]), guid=survey_v2_texts['dms_guid'])
    guid = data.payload["guid"]
    config = SurveyConfigModelV2.get_config_from_dms()
    confing_json = json.dumps(config)
    gb_session().conn().query("""
    UPDATE survey_v2.survey_config SET config = %s WHERE guid = %s
    """, [confing_json, guid])
    row = gb_session().conn().select_one("""
    SELECT * FROM survey_v2.survey_config WHERE guid = %s
    """, [guid])
    return row


@command("admin:survey:v2:reset_survey", [is_admin])
def admin_survey_v2_reset_survey(data: CommandData):
    guid = data.payload["guid"]
    gb_session().conn().query("""
    UPDATE survey_v2.survey_instance 
       SET client_data = NULL, server_data = NULL, ext_data = NULL
     WHERE guid = %s
    """, [guid])


@command("admin:survey:v2:get_config", [is_admin])
def admin_survey_v2_get_config():
    config = SurveyConfigModelV2.get_config_from_dms()
    return config


@command("admin:survey:v2:set_page", [is_admin])
def admin_survey_v2_set_page(data: CommandData):
    model = SurveyModelV2.load_instance(data.payload["guid"])
    model.set_current_page(data.payload["page"])
    return {
        "state": 1,
        "data": model.export()
    }
