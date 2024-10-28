from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from src.server.models.dms.dms_constants import DmsConstants
from src.server.models.dms.dms_model import DmsCache
from src.server.models.survey.v3.survey_study_preview_model import SurveyStudyPreviewModel
from src.session.session import gb_session


@command("survey:study_preview:get_texts")
def survey__study_preview__get_texts():
    config = DmsCache.get_json(gb_session().conn(), guid=DmsConstants.survey_v2_config)
    param_texts = []
    if "param_texts" in config:
        param_texts = config["param_texts"]
    survey_texts = DmsCache.get_json(gb_session().conn(), guid=DmsConstants.survey_v2_texts)
    return {
        "translations": survey_texts["texts"],
        "params": param_texts
    }


@command("survey:study_preview:get")
def survey__study_preview__get(data: CommandData):
    study_guid = data.payload["study_guid"]

    survey_data = None
    if "survey_data" in data.payload:
        survey_data = data.payload["survey_data"]

    survey = SurveyStudyPreviewModel(gb_session().conn(), study_guid, survey_data)
    return survey.get()


@command("survey:study_preview:submit")
def survey__study_preview__submit(data: CommandData):
    study_guid = data.payload["study_guid"]
    survey_data = data.payload["survey_data"]
    submit_data = data.payload["submit_data"]
    survey = SurveyStudyPreviewModel(gb_session().conn(), study_guid, survey_data)
    res = survey.submit(data=submit_data)
    return res
