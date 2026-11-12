from server_core.commands.command_data import CommandData, FilesCommandData
from server_core.commands.commands_handler import CommandsHandler
from server_core.utils.utils import Utils
from src.db.calc.factory.calc_interface_gaps_opportunities import CalcInterfaceGapsOpportunities
from src.db.games_db_model import GamesDbModel
from src.db.games_search_db_model import GamesSearchDbModel
from src.db.surveys_db_model import SurveysDbModel
from src.db.user_portal_db_model import UserPortalDbModel
from src.models.scraper.scraper_utils import ScraperUtils
from src.models.user_obfuscator import UserObfuscator
from src.session.users_session import UsersSession


portal_commands_handler = CommandsHandler(
    session_class=UsersSession
)

@portal_commands_handler.register("portal:get_initial_data")
def portal__get_initial_data(session: UsersSession):
    return {
        "state": "ok",
        "app_details": session.user_db.portal_db.get_my_apps(load_app_details=True),
        "credits": session.user_db.get_credits(),
        "unlocked_modules": session.user_db.billing_db.get_unlocked_modules(),
        "def": session.user_db.platform_db.get_def(),
        "tutorial": session.user_db.tutorial_db.get_client_data()
    }

@portal_commands_handler.register("portal:get_developers_hints")
def portal__get_developers_hints(session: UsersSession, data: CommandData):
    return GamesSearchDbModel.create_from_user_db(user_db=session.user_db).get_developers_hints(
        user_id=session.user_db.user_id,
        title=data.ensure('title', Utils.is_string),
        include_concepts=data.try_ensure('include_concepts', Utils.is_bool, False)
    )

@portal_commands_handler.register("portal:search_google_play_apps")
def portal__search_google_play_apps(session: UsersSession, data: CommandData):
    return GamesSearchDbModel.create_from_user_db(user_db=session.user_db).search_app_by_title(
        user_id=session.user_db.user_id,
        title=data.ensure('app_title', Utils.is_string),
        search_in_concepts=data.try_ensure('search_in_concepts', Utils.is_bool, False)
    )

@portal_commands_handler.register("portal:get_developers_details")
def portal__get_developers_details(session: UsersSession, data: CommandData):
    return GamesDbModel(conn=session.conn()).get_devs_details(
        devs_ids_int=data.ensure(UserObfuscator.DEV_IDS_INT)
    )

@portal_commands_handler.register("portal:get_apps_details")
def portal__get_apps_details(session: UsersSession, data: CommandData):
    apps_ids_int: list[int] = data.ensure(UserObfuscator.APP_IDS_INT)
    games_db = GamesDbModel(conn=session.conn())

    if not games_db.scrap_apps_if_not_scraped(
        apps_ids_int=apps_ids_int
    ):
        return {
            "state": 2,
            "app_details": []
        }

    include_gallery = data.try_ensure('include_gallery', Utils.is_bool, False)

    app_details = session.user_db.portal_db.get_apps_details(
        apps_ids_int=apps_ids_int,
        include_gallery=include_gallery
    )

    return {
        "state": 1 if len(app_details) > 0 else -1,
        "app_details": app_details
    }

@portal_commands_handler.register("portal:scrap_app")
def portal__scrap_app(session: UsersSession, data: CommandData):
    return GamesDbModel(conn=session.conn()).scrap_app(
        app_id_in_store=data.ensure('app_id_in_store', Utils.is_string),
        store=data.ensure('store', Utils.is_int),
        force=data.try_ensure('force', Utils.is_bool, False)
    )

@portal_commands_handler.register("portal:scrap_dev")
def portal__scrap_dev(session: UsersSession, data: CommandData):
    return GamesDbModel(conn=session.conn()).scrap_dev(
        dev_id_in_store=data.ensure('dev_id_in_store', Utils.is_string),
        store=data.ensure('store', Utils.is_int),
        scrap_dev_apps=True
    )

@portal_commands_handler.register("portal:add_app_from_store_to_my_apps", is_not_demo=True)
def portal__add_app_from_store_to_my_apps(session: UsersSession, data: CommandData):
    return session.user_db.portal_db.add_app_from_store_to_my_apps(
        app_id_in_store=data.ensure('app_id_in_store', Utils.is_string),
        store=data.ensure('store', Utils.is_int)
    )

@portal_commands_handler.register("portal:create_concept_app_from_temp", is_not_demo=True)
def portal__create_concept_app_from_temp(session: UsersSession, data: CommandData):
    return session.user_db.games_db.create_concept_app_from_temp(
        app_detail_changes=data.ensure('app_detail_changes', UserPortalDbModel.is_app_detail_changes_input)
    )

@portal_commands_handler.register("portal:save_concept_app", is_not_demo=True)
def portal__save_concept_app(session: UsersSession, data: FilesCommandData):
    app_detail_changes = data.try_ensure('app_detail_changes', UserPortalDbModel.is_app_detail_changes_input)
    files = data.files
    return session.user_db.games_db.save_concept_app(
        app_id_int=data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int),
        app_detail_changes=app_detail_changes,
        files=files
    )

@portal_commands_handler.register("portal:save_concept_app_icon", is_not_demo=True)
def portal__save_concept_app_icon(session: UsersSession, data: FilesCommandData):
    app_id_int = data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int)

    if len(data.files) > 0:
        file = data.files[0]
        icon_bytes = ScraperUtils.get_app_icon_bytes_from_file(file=file)

        session.user_db.games_db.save_concept_app_icon(
            app_id_int=app_id_int,
            icon_bytes=icon_bytes
        )

    return session.user_db.games_db.get_app_detail(
        app_id_int=app_id_int,
        include_gallery=True
    )


@portal_commands_handler.register("portal:remove_app_from_my_apps", is_not_demo=True)
def portal__remove_app_from_my_apps(session: UsersSession, data: CommandData):
    app_id_int = data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int)
    session.user_db.portal_db.remove_app_from_my_apps(
        app_id_int=app_id_int
    )

@portal_commands_handler.register("portal:copy_app", is_not_demo=True)
def portal_apps_copy_app(session: UsersSession, data: CommandData):
    app_id_int = data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int)
    session.user_db.portal_db.create_concept_as_copy(
        app_id_int=app_id_int
    )

@portal_commands_handler.register("portal:get_tagged_app")
def portal_get_tagged_app(session: UsersSession, data: CommandData):
    app_id_int = data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int)
    return session.user_db.games_db.get_tags(
        app_id_int=app_id_int
    )

@portal_commands_handler.register("portal:tag_store_app_if_not_tagged")
def portal__tag_store_app_if_not_tagged(session: UsersSession, data: CommandData):
    app_id_int = data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int)
    tagging_context = data.ensure('tagging_context', Utils.is_string)
    admin_force = data.try_ensure('admin_force', Utils.is_bool, False)
    
    assert Utils.is_string_enum(
        tagging_context,
        enum_vals=GamesDbModel.TAGGING_CONTEXT_ENUM
    )

    if admin_force and not session.user_db.is_admin():
        raise Exception("User is not admin")

    return session.user_db.games_db.tag_store_app_if_not_tagged(
        app_id_int=app_id_int,
        tagging_context=tagging_context,
        admin_force=admin_force
    )

@portal_commands_handler.register("portal:tag_concept_app")
def portal__tag_concept_app(session: UsersSession, data: CommandData):
    app_id_int = data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int)
    tagging_context = data.ensure('tagging_context', Utils.is_string)

    assert Utils.is_string_enum(
        tagging_context,
        enum_vals=GamesDbModel.TAGGING_CONTEXT_ENUM
    )
    
    return session.user_db.games_db.tag_concept_app(
        app_id_int=app_id_int,
        tagging_context=tagging_context
    )


@portal_commands_handler.register("portal:get_tagging_state")
def portal__get_tagging_state(session: UsersSession, data: CommandData):
    app_id_int = data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int)
    return session.user_db.games_db.get_tagging_state(
        app_id_int=app_id_int
    )


@portal_commands_handler.register("portal:get_opportunity_detail")
def portal__get_opportunity_detail(session: UsersSession, data: CommandData):
    opportunity_guid = data.ensure('opportunity_guid', Utils.is_guid)
    survey_id=SurveysDbModel(conn=session.conn()).get_survey_meta_id(SurveysDbModel.SURVEY_CONTROL_GUID)
    
    if survey_id is None:
        raise Exception('Survey not found')
    
    return CalcInterfaceGapsOpportunities(
        conn=session.conn(),
        user_id=session.user_db.user_id,
        survey_id=survey_id
    ).generate_client_data(
        obfuscator=session.user_db.obfuscator,
        opportunity_guid=opportunity_guid
    )    

@portal_commands_handler.register("portal:save_tags_from_games_explorer", is_not_demo=True)
def portal__save_tags_from_games_explorer(session: UsersSession, data: CommandData):
    tags_per_app = {}
    for it in data.payload:
        if data.payload["tag_id"] == "__wip":
            continue

        app_id_int = data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int)
        tag_id_int = data.ensure(UserObfuscator.TAG_ID_INT, Utils.is_int)

        if app_id_int not in tags_per_app:
            tags_per_app[app_id_int] = {
                "add": [],
                "del": []
            }

        if it["b"]:
            tags_per_app[app_id_int]["add"].append({
                UserObfuscator.TAG_ID_INT: it[UserObfuscator.TAG_ID_INT],
                "tag_rank": it["tag_rank"]
            })
        else:
            tags_per_app[app_id_int]["del"].append(tag_id_int)

    for app_id_int in tags_per_app:

        assert session.user_db.games_db.is_concept(
            app_id_int=app_id_int,
            check_owner=session.user_db.user_id
        )

        current_tags = session.user_db.games_db.get_tags(
            app_id_int=app_id_int
        )["tags"]

        current_tags = [
            tag_detail for tag_detail in current_tags if tag_detail[UserObfuscator.TAG_ID_INT] not in tags_per_app[app_id_int]["del"]
        ]

        for tag_detail_add in tags_per_app[app_id_int]["add"]:
            found = False
            for tag_detail_current in current_tags:
                if tag_detail_current[UserObfuscator.TAG_ID_INT] == tag_detail_add[UserObfuscator.TAG_ID_INT]:
                    tag_detail_current["tag_rank"] = tag_detail_add["tag_rank"]
                    found = True
            if not found:
                current_tags.append(tag_detail_add)

        session.user_db.games_db.set_manual_tags(
            app_id_int=app_id_int,
            tags_details=current_tags
        )


@portal_commands_handler.register("portal:request_tags_override_by_user", is_not_demo=True)
def portal__request_tags_override_by_user(session: UsersSession, data: CommandData):
    app_id_int = data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int)
    
    session.user_db.games_db.create_user_override_request(
        app_id_int=app_id_int,
        tags_details=data.payload["tags_details"]
    )

@portal_commands_handler.register("portal:get_app_history_kpis")
def portal__get_app_history_kpis(session: UsersSession, data: CommandData):
    assert Utils.is_string_enum(data.payload["kpi"], enum_vals=["size", "growth", "quality"])
    assert Utils.is_string_enum(data.payload["interval"], enum_vals=["6m", "12m", "all"])
    
    app_id_int = data.ensure(UserObfuscator.APP_ID_INT, Utils.is_int)

    res = session.user_db.games_db.get_apps_history_kpis(
        app_ids_int=[app_id_int],
        kpi=data.payload["kpi"],
        interval=data.payload["interval"]
    )
    
    return res[app_id_int]

@portal_commands_handler.register("portal:get_games_explorer_filter_apps")
def portal__get_games_explorer_filter_apps(session: UsersSession, data: CommandData):
    if not session.user_db.games_db.scrap_apps_for_devs(
        filters=data.payload["filters"]
    ):
        return {
            "state": "scraping"
        }
    return session.user_db.games_db.get_games_explorer_filter_apps(
        filters=data.payload["filters"]
    )

@portal_commands_handler.register("portal:get_games_explorer_filters_def")
def portal__get_games_explorer_filters_def():
    return {
        "prices": GamesDbModel.GAMES_EXPLORER_FILTER_PRICES,
        "stores": GamesDbModel.GAMES_EXPLORER_FILTER_STORES
    }

@portal_commands_handler.register("portal:get_games_explorer_compare_apps_data")
def portal__get_games_explorer_compare_apps_data(session: UsersSession, data: CommandData):
    return session.user_db.games_db.get_games_explorer_compare_apps_data(
        app_ids_int=data.payload[UserObfuscator.APP_IDS_INT]
    )

@portal_commands_handler.register("portal:get_games_explorer_kpi_hist_data")
def portal__get_games_explorer_kpi_hist_data(session: UsersSession, data: CommandData):
    assert Utils.is_string_enum(data.payload["kpi"], enum_vals=["size", "growth", "quality"])
    assert Utils.is_string_enum(data.payload["interval"], enum_vals=["6m", "12m", "all"])
    
    kpi_data = session.user_db.games_db.get_apps_history_kpis(
        app_ids_int=data.payload[UserObfuscator.APP_IDS_INT],
        kpi=data.payload["kpi"],
        interval=data.payload["interval"]
    )
    
    res = []
    for app_id_int in kpi_data:
        res = res + kpi_data[app_id_int]
    return res

@portal_commands_handler.register("portal:get_player_explorer_data")
def portal__get_player_explorer_data(session: UsersSession, data: CommandData):
    input_filters = []
    if "filters" in data.payload:
        input_filters = data.payload["filters"]

    show = None
    if "show" in data.payload:
        show = data.payload["show"]

    return SurveysDbModel(conn=session.conn()).get_player_explorer_data(
        user_id=session.user_db.user_id,
        input_filters=input_filters,
        show=show
    )