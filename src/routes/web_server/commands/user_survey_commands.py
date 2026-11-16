import json

import flask

from server_core.commands.command_data import CommandData
from server_core.commands.commands_handler import CommandsHandler
from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.dms_db_model import DmsCache
from src.db.survey.survey_db_model import SurveyDbModel
from src.db.survey.survey_study_preview_model import SurveyStudyPreviewModel
from src.env_data import ENV_DATA
from src.session.users_session import UsersSession



user_survey_commands_handler = CommandsHandler(
    session_class=UsersSession
)

@user_survey_commands_handler.register("user_survey:get_study_preview")
def user_survey__get_study_preview(session: UsersSession, data: CommandData):
    study_guid = data.payload["study_guid"]

    survey_data = {}
    if "survey_data" in data.payload:
        survey_data = data.payload["survey_data"]

    survey = SurveyStudyPreviewModel(
        conn=session.conn(),
        user_id=session.user_db.user_id,
        survey_guid=study_guid,
        survey_data=survey_data
    )
    
    return survey.get()

@user_survey_commands_handler.register("user_survey:submit_study_preview")
def user_survey__submit_study_preview(session: UsersSession, data: CommandData):
    study_guid = data.payload["study_guid"]
    survey_data = data.payload["survey_data"]
    submit_data = data.payload["submit_data"]
    
    survey = SurveyStudyPreviewModel(
        conn=session.conn(),
        user_id=session.user_db.user_id,
        survey_guid=study_guid,
        survey_data=survey_data
    )
        
    res = survey.submit(data=submit_data)
    return res


@user_survey_commands_handler.register("user_survey:get_survey")
def user_survey__get_survey(session: UsersSession, data: CommandData):
    guid = data.payload["guid"]
    model = SurveyDbModel.load_instance(conn=session.conn(), survey_instance_guid=guid)
    if model is None:
        return {
            "state": 2
        }
    return {
        "state": 1,
        "data": model.export()
    }

@user_survey_commands_handler.register("user_survey:set_lang")
def user_survey__set_lang(session: UsersSession, data: CommandData):
    guid = data.payload["guid"]
    lang = data.payload["lang"]
    session.conn().query("""
    UPDATE survey_v2.survey_instance s
       SET s.lang = %s
     WHERE s.guid = %s
    """, [lang, guid])
    return {
        "state": 1,
    }

@user_survey_commands_handler.register("user_survey:submit")
def user_survey__submit(session: UsersSession, data: CommandData):

    model = SurveyDbModel.load_instance(conn=session.conn(), survey_instance_guid=data.payload["guid"])
    if model is None:
        flask.abort(404)
    model.submit(data.payload["data"])
    return {
        "state": 1,
        "data": model.export()
    }

def log_survey_error(conn: DbConnection, guid: str, error_code: str, error_text: str):
    conn.query("""
    UPDATE survey_v2.survey_instance
       SET error_code = %s,
           error_text = %s
     WHERE guid = %s
    """, [error_code, error_text, guid])

@user_survey_commands_handler.register("user_survey:get_texts")
def user_survey__get_texts(session: UsersSession, data: CommandData):
    assert ENV_DATA.survey_config is not None
    assert ENV_DATA.survey_texts is not None
    
    config = DmsCache.get_json(session.conn(), guid=ENV_DATA.survey_config)
    param_texts = []
    if "param_texts" in config:
        param_texts = config["param_texts"]
    survey_texts = DmsCache.get_json(session.conn(), guid=ENV_DATA.survey_texts)
    return {
        "translations": survey_texts["texts"],
        "params": param_texts
    }

@user_survey_commands_handler.register("user_survey:get_configs")
def user_survey__get_configs(session: UsersSession):
    survey_config_rows = session.conn().select_all("""
    SELECT * FROM survey_v2.survey_config
    """)
    survey_preview_rows = session.conn().select_all("""
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

@user_survey_commands_handler.register("user_survey:create_config")
def user_survey__create_config(session: UsersSession):
    config_guid = Utils.get_guid()
    instance_guid = Utils.get_guid()
    session.conn().insert("""
        INSERT INTO survey_v2.survey_config (guid, config, user_id) VALUES (%s, %s, %s)
        """, [config_guid, json.dumps(SurveyDbModel.get_config_from_dms(conn=session.conn())), session.user_db.user_id])
    model = SurveyDbModel.create_instance(conn=session.conn(), survey_config=config_guid, guid=instance_guid)
    assert model is not None
    session.conn().query("""
    INSERT INTO survey_v2.survey_preview (user_id, survey_instance) VALUES (%s, %s)
    """, [session.user_db.user_id, model.survey_instance_guid])
    return {
        "survey_instance": model.survey_instance_guid
    }

@user_survey_commands_handler.register("user_survey:reset_survey")
def auser_survey__reset_survey(session: UsersSession, data: CommandData):
    guid = data.payload["guid"]
    session.conn().query("""
    UPDATE survey_v2.survey_instance 
       SET client_data = NULL, server_data = NULL, ext_data = NULL
     WHERE guid = %s
    """, [guid])

@user_survey_commands_handler.register("user_survey:get_config")
def user_survey_2_get_config(session: UsersSession):
    config = SurveyDbModel.get_config_from_dms(conn=session.conn())
    return config

@user_survey_commands_handler.register("user_survey:set_page")
def user_survey__set_page(session: UsersSession, data: CommandData):
    model = SurveyDbModel.load_instance(conn=session.conn(), survey_instance_guid=data.payload["guid"])
    assert model is not None
    model.set_current_page(data.payload["page"])
    return {
        "state": 1,
        "data": model.export()
    }

@user_survey_commands_handler.register("user_survey:get")
def user_survey__get(session: UsersSession, data: CommandData):
    model = SurveyDbModel.load_instance(conn=session.conn(), survey_instance_guid=data.payload["guid"])
    if model is None:
        return {
            "state": 2
        }
    return {
        "state": 1,
        "data": model.export()
    }
