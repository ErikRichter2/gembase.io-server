from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from src.server.models.session.models.games_explorer_session_model import GamesExplorerSessionModel
from src.server.models.user.user_obfuscator import UserObfuscator
from src.session.session import is_logged, gb_session
from src.utils.gembase_utils import GembaseUtils


@command("portal:get_games_explorer_filter_apps", [is_logged])
def portal__get_games_explorer_filter_apps(data: CommandData):
    if not gb_session().models().games_explorer().scrap_apps_for_devs(
        filters=data.payload["filters"]
    ):
        return {
            "state": "scraping"
        }
    return gb_session().models().games_explorer().get_games_explorer_filter_apps(
        filters=data.payload["filters"]
    )


@command("portal:get_games_explorer_filters_def", [is_logged])
def portal__get_games_explorer_filters_def():
    return {
        "prices": GamesExplorerSessionModel.GAMES_EXPLORER_FILTER_PRICES,
        "stores": GamesExplorerSessionModel.GAMES_EXPLORER_FILTER_STORES
    }


@command("portal:get_games_explorer_compare_apps_data", [is_logged])
def portal__get_games_explorer_compare_apps_data(data: CommandData):
    return gb_session().models().games_explorer().get_games_explorer_compare_apps_data(
        app_ids_int=data.payload[UserObfuscator.APP_IDS_INT]
    )


@command("portal:get_games_explorer_kpi_hist_data", [is_logged])
def portal__get_games_explorer_kpi_hist_data(data: CommandData):
    assert GembaseUtils.is_string_enum(data.payload["kpi"], enum_vals=["size", "growth", "quality"])
    assert GembaseUtils.is_string_enum(data.payload["interval"], enum_vals=["6m", "12m", "all"])
    data = gb_session().models().apps().get_apps_history_kpis(
        app_ids_int=data.payload[UserObfuscator.APP_IDS_INT],
        kpi=data.payload["kpi"],
        interval=data.payload["interval"]
    )
    res = []
    for app_id_int in data:
        res = res + data[app_id_int]
    return res
