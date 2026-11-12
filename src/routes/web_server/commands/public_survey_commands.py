import flask

from server_core.commands.command_data import CommandData
from server_core.commands.commands_handler import CommandsHandler
from src.db.games_search_db_model import GamesSearchDbModel
from src.db.quota.survey_quota_context import SurveySearchAppQuotaContext
from src.db.survey.survey_public_db_model import SurveyPublicDbModel
from src.models.web_search.demo_search import DemoSearch
from src.session.public_session import PublicSession



public_survey_commands_handler = CommandsHandler(
    session_class=PublicSession
)


@public_survey_commands_handler.register("survey:get")
def public_survey__get(session: PublicSession, data: CommandData):

    if "guid" not in data.payload:
        flask.abort(404)

    guid = data.payload["guid"]

    if "cid" not in data.payload:
        flask.abort(404)
    cid = data.payload["cid"]

    gender = None
    age = None
    year_of_birth = None
    if "s" in data.payload:
        gender = data.payload["s"]
    if "a" in data.payload:
        age = data.payload["a"]
    if "y" in data.payload:
        year_of_birth = data.payload["y"]

    return SurveyPublicDbModel(conn=session.conn()).get(
        guid=guid,
        cid=cid,
        gender=gender,
        age=age,
        year_of_birth=year_of_birth
    )


@public_survey_commands_handler.register("survey:submit")
def public_survey__submit(session: PublicSession, data: CommandData):

    if "guid" not in data.payload:
        flask.abort(404)

    guid = data.payload["guid"]

    return SurveyPublicDbModel(conn=session.conn()).submit(guid=guid, client_data=data.payload)


@public_survey_commands_handler.register("survey:get_apps")
def public_survey__get_apps(session: PublicSession, data: CommandData):
    guid = data.payload["guid"]

    apps = GamesSearchDbModel(conn=session.conn(), search_engine=DemoSearch()).search_app_by_name(user_id=0, app_name=data.payload["app_title"], quota_context=SurveySearchAppQuotaContext(guid))

    return apps


