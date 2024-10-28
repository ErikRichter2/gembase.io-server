from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from gembase_server_core.commands.command_response_file import CommandResponseFile
from src.session.session import is_logged, is_admin, gb_session


@command("studies:get_studies", [is_logged])
def studies__get_studies():
    return gb_session().models().studies().get_studies()


@command("studies:create_study", [is_logged])
def studies__create_study():
    return gb_session().models().studies().create_study()


@command("studies:get_studies_def", [is_logged])
def studies__get_studies_def():
    return gb_session().models().studies().get_studies_def()


@command("studies:save_study", [is_logged])
def studies__save_study(data: CommandData):
    return gb_session().models().studies().get_study_model_from_command(
        data=data
    ).save_study(
        data=data.payload["data"]
    )


@command("studies:launch_study", [is_logged])
def studies__launch_study(data: CommandData):
    return gb_session().models().studies().get_study_model_from_command(
        data=data
    ).launch_study()


@command("studies:copy_study", [is_logged])
def studies__copy_study(data: CommandData):
    return gb_session().models().studies().get_study_model_from_command(
        data=data
    ).copy_study()


@command("studies:delete_study", [is_logged])
def studies__delete_study(data: CommandData):
    return gb_session().models().studies().get_study_model_from_command(
        data=data
    ).delete_study()


@command("studies:generate_csv_for_internal_launch", [is_logged])
def studies__generate_csv_for_internal_launch(data: CommandData):
    return CommandResponseFile(
        filename="survey.csv",
        mime="text/csv",
        payload=gb_session().models().studies().get_study_model_from_command(
            data=data
        ).generate_csv_for_internal_launch()
    )


@command("studies:simulate_survey", [is_admin])
def studies__simulate_survey(data: CommandData):
    return gb_session().models().studies().get_study_model_from_command(
        data=data
    ).simulate_survey()
