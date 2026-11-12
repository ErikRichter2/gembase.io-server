from server_core.commands.command_data import CommandData, FilesCommandData
from server_core.commands.command_response_file import CommandResponseFile
from server_core.commands.commands_handler import CommandsHandler
from server_core.utils.utils import Utils
from src import session
from src.session.users_session import UsersSession

studies_commands_handler = CommandsHandler(
    session_class=UsersSession
)

@studies_commands_handler.register("studies:get_studies")
def studies__get_studies(session: UsersSession):
    return session.user_db.studies_db.get_studies()

@studies_commands_handler.register("studies:create_study")
def studies__create_study(session: UsersSession):
    return session.user_db.studies_db.create_study()

@studies_commands_handler.register("studies:get_studies_def")
def studies__get_studies_def(session: UsersSession):
    return session.user_db.studies_db.get_studies_def()

@studies_commands_handler.register("studies:save_study")
def studies__save_study(session: UsersSession, data: CommandData):
    return session.user_db.studies_db.get_study_model_from_command(
        data=data
    ).save_study(
        data=data.payload["data"]
    )

@studies_commands_handler.register("studies:launch_study")
def studies__launch_study(session: UsersSession, data: CommandData):
    return session.user_db.studies_db.get_study_model_from_command(
        data=data
    ).launch_study()

@studies_commands_handler.register("studies:copy_study")
def studies__copy_study(session: UsersSession, data: CommandData):
    return session.user_db.studies_db.get_study_model_from_command(
        data=data
    ).copy_study()

@studies_commands_handler.register("studies:delete_study")
def studies__delete_study(session: UsersSession, data: CommandData):
    return session.user_db.studies_db.get_study_model_from_command(
        data=data
    ).delete_study()

@studies_commands_handler.register("studies:generate_csv_for_internal_launch")
def studies__generate_csv_for_internal_launch(session: UsersSession, data: CommandData):
    return CommandResponseFile(
        filename="survey.csv",
        mime="text/csv",
        payload=session.user_db.studies_db.get_study_model_from_command(
            data=data
        ).generate_csv_for_internal_launch()
    )

@studies_commands_handler.register("studies:simulate_survey")
def studies__simulate_survey(session: UsersSession, data: CommandData):
    return session.user_db.studies_db.get_study_model_from_command(
        data=data
    ).simulate_survey()
