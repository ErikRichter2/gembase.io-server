from server_core.commands.command_data import CommandData
from server_core.commands.commands_handler import CommandsHandler
from server_core.utils.utils import Utils
from src import session
from src.db.games_db_model import GamesDbModel
from src.db.user_app_search_db_model import UserAppSearchDbModel
from src.models.user_obfuscator import UserObfuscator
from src.session.users_session import UsersSession


portal_commands_handler = CommandsHandler(
    session_class=UsersSession
)


@portal_commands_handler.register("portal:get_initial_data")
def portal__get_initial_data(session: UsersSession):
    return {
        "state": "ok",
        "app_details": session.user_portal_db().get_my_apps(load_app_details=True),
        "credits": session.user_db().get_credits(),
        "unlocked_modules": session.user_billing_db().get_unlocked_modules(),
        "def": session.user_platform_db().get_def(session.user_billing_db()),
        "tutorial": session.user_tutorial_db().get_client_data()
    }

@portal_commands_handler.register("portal:get_developers_hints")
def portal__get_developers_hints(session: UsersSession, data: CommandData):
    return UserAppSearchDbModel(user_db=session.user_db(), search_model=session.user_db().get_search_model()).get_developers_hints(
        title=data.ensure('title', Utils.is_string),
        include_concepts=data.try_ensure('include_concepts', Utils.is_bool, False)
    )

@portal_commands_handler.register("portal:search_google_play_apps")
def portal__search_google_play_apps(session: UsersSession, data: CommandData):
    return UserAppSearchDbModel(user_db=session.user_db(), search_model=session.user_db().get_search_model()).search_app_by_title(
        title=data.ensure('app_title', Utils.is_string),
        search_in_concepts=data.try_ensure('search_in_concepts', Utils.is_bool, False)
    )

@portal_commands_handler.register("portal:search_google_play_apps")
def portal__get_developers_details(session: UsersSession, data: CommandData):
    return GamesDbModel(conn=session.conn()).get_devs_details(
        devs_ids_int=data.ensure(UserObfuscator.DEV_IDS_INT)
    )

@portal_commands_handler.register("portal:get_apps_details")
def portal__get_app_details(session: UsersSession, data: CommandData):
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

    app_details = session.user_portal_db().get_apps_details(
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


@command("portal:add_app_from_store_to_my_apps", [is_logged])
def portal__add_app_from_store_to_my_apps(data: CommandData):
    assert GembaseUtils.is_string(data.payload["app_id_in_store"], max_length=200)
    assert GembaseUtils.is_int(data.payload["store"])

    return gb_session().models().apps().add_app_from_store_to_my_apps(
        app_id_in_store=data.payload["app_id_in_store"],
        store=data.payload["store"]
    )


@command("portal:create_concept_app_from_temp", [is_logged])
def portal__create_concept_app_from_temp(data: CommandData):
    return gb_session().models().apps().create_concept_app_from_temp(
        app_detail_changes=data.payload["app_detail_changes"]
    )


@command("portal:save_concept_app", [is_logged])
def portal__save_concept_app(data: FilesCommandData):
    app_detail_changes = data.payload["app_detail_changes"] if "app_detail_changes" in data.payload else None
    files = data.files
    return gb_session().models().apps().save_concept_app(
        app_id_int=data.payload[UserObfuscator.APP_ID_INT],
        app_detail_changes=app_detail_changes,
        files=files
    )


@command("portal:save_concept_app_icon", [is_logged])
def portal__save_concept_app_icon(data: FilesCommandData):
    app_id_int = data.payload[UserObfuscator.APP_ID_INT]

    if len(data.files) > 0:
        file = data.files[0]
        icon_bytes = ScraperUtils.get_app_icon_bytes_from_file(file=file)

        gb_session().models().apps().save_concept_app_icon(
            app_id_int=app_id_int,
            icon_bytes=icon_bytes
        )

    return gb_session().models().apps().get_app_detail(
        app_id_int=app_id_int,
        include_gallery=True
    )


@command("portal:remove_app_from_my_apps", [is_logged])
def portal__remove_app_from_my_apps(data: CommandData):
    gb_session().models().apps().remove_app_from_my_apps(
        app_id_int=data.payload[UserObfuscator.APP_ID_INT]
    )


@command("portal:create_concept_as_copy", [is_logged])
def portal_apps_copy_app(data: CommandData):
    return gb_session().models().apps().create_concept_as_copy(
        app_id_int=data.payload[UserObfuscator.APP_ID_INT]
    )


@command("portal:get_tagged_app", [is_logged])
def portal_get_tagged_app(data: CommandData):
    return gb_session().models().tags().get_tags(
        app_id_int=data.payload[UserObfuscator.APP_ID_INT]
    )


@command("portal:tag_store_app_if_not_tagged", [is_logged])
def portal__tag_store_app_if_not_tagged(data: CommandData):
    assert GembaseUtils.is_string_enum(
        data.payload["tagging_context"],
        enum_vals=TagsSessionModel.TAGGING_CONTEXT_ENUM
    )

    admin_force = False
    if "admin_force" in data.payload:
        assert GembaseUtils.is_bool(data.payload["admin_force"])
        admin_force = data.payload["admin_force"]
    if admin_force and not gb_session().user().is_admin():
        raise Exception("admin")

    return gb_session().models().tags().tag_store_app_if_not_tagged(
        app_id_int=data.payload[UserObfuscator.APP_ID_INT],
        tagging_context=data.payload["tagging_context"],
        admin_force=admin_force
    )


@command("portal:tag_concept_app", [is_logged])
def portal__tag_concept_app(data: CommandData):
    assert GembaseUtils.is_string_enum(
        data.payload["tagging_context"],
        enum_vals=TagsSessionModel.TAGGING_CONTEXT_ENUM
    )
    return gb_session().models().tags().tag_concept_app(
        app_id_int=data.payload[UserObfuscator.APP_ID_INT],
        tagging_context=data.payload["tagging_context"]
    )


@command("portal:get_tagging_state", [is_logged])
def portal__get_tagging_state(data: CommandData):
    return gb_session().models().tags().get_tagging_state(
        app_id_int=data.payload[UserObfuscator.APP_ID_INT]
    )


@command("portal:get_opportunity_detail", [is_logged])
def portal__get_opportunity_detail(data: CommandData):
    assert GembaseUtils.is_guid(data.payload["opportunity_guid"])
    return gb_session().models().platform().get_opportunity_detail(
        opportunity_guid=data.payload["opportunity_guid"]
    )


@command("portal:save_tags_from_games_explorer", [is_logged])
def portal__save_tags_from_games_explorer(data: CommandData):
    tags_per_app = {}
    for it in data.payload:
        if data.payload["tag_id"] == "__wip":
            continue

        app_id_int = data.payload[UserObfuscator.APP_ID_INT]
        tag_id_int = data.payload[UserObfuscator.TAG_ID_INT]

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

        assert AppModel.is_concept(
            conn=gb_session().conn(),
            app_id_int=app_id_int,
            check_owner=gb_session().user().get_id()
        )

        current_tags = gb_session().models().tags().get_tags(
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

        gb_session().models().tags().set_manual_tags(
            app_id_int=app_id_int,
            tags_details=current_tags
        )


@command("portal:request_tags_override_by_user", [is_logged])
def portal__request_tags_override_by_user(data: CommandData):
    gb_session().models().tags().create_user_override_request(
        app_id_int=data.payload[UserObfuscator.APP_ID_INT],
        tags_details=data.payload["tags_details"]
    )


@command("portal:get_app_history_kpis", [is_logged])
def portal__get_app_history_kpis(data: CommandData):
    assert GembaseUtils.is_string_enum(data.payload["kpi"], enum_vals=["size", "growth", "quality"])
    assert GembaseUtils.is_string_enum(data.payload["interval"], enum_vals=["6m", "12m", "all"])

    return gb_session().models().apps().get_app_history_kpis(
        app_id_int=data.payload[UserObfuscator.APP_ID_INT],
        kpi=data.payload["kpi"],
        interval=data.payload["interval"]
    )
