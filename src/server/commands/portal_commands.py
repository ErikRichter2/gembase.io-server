from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from gembase_server_core.commands.files_command_data import FilesCommandData
from src.server.models.apps.app_model import AppModel
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.scraper.scraper_utils import ScraperUtils
from src.server.models.session.models.tags_session_model import TagsSessionModel
from src.server.models.user.user_obfuscator import UserObfuscator
from src.server.models.user.user_data import UserData
from src.server.models.user.user_model import UserModel
from src.session.session import gb_session, is_logged
from src.utils.gembase_utils import GembaseUtils


@command("portal:get", [is_logged])
def portal_get():

    if not gb_session().models().apps().scrap_apps_if_not_scraped(
        app_ids_int=gb_session().models().apps().get_my_apps()
    ):
        return {
            "state": "scraping"
        }

    return {
        "state": "ok",
        "app_details": gb_session().models().apps().get_my_apps(load_app_details=True),
        "credits": gb_session().user().get_credits(),
        "unlocked_modules": gb_session().models().billing().get_unlocked_modules(),
        "def": gb_session().models().platform().get_def(),
        "tutorial": gb_session().models().tutorial().get_client_data()
    }


@command("portal:update_user", [is_logged])
def portal__update_user(data: CommandData):
    assert GembaseUtils.is_string(data.payload["name"], max_length=200)
    assert GembaseUtils.is_int(data.payload["position_area"])
    assert GembaseUtils.is_int(data.payload["position_role"])

    gb_session().user().update(
        name=data.payload["name"],
        position_area=data.payload["position_area"],
        position_role=data.payload["position_role"]
    )


@command("portal:request_password_change", [is_logged])
def portal__change_password():
    gb_session().user().request_password_change()


@command("public:request_password_change")
def public__request_password_change(data: CommandData):
    assert GembaseUtils.is_email(data.payload["email"])

    user_id = UserData.get_user_id_from_email(
        conn=gb_session().conn(),
        email=data.payload["email"]
    )
    if user_id != 0:
        UserModel(
            conn=gb_session().conn(),
            user_id=user_id
        ).request_password_change()


@command("portal:get_developers_hints", [is_logged])
def portal_get_developers_hints(data: CommandData):
    assert GembaseUtils.is_string(data.payload["title"], max_length=200)

    include_concepts = True
    if "include_concepts" in data.payload and data.payload["include_concepts"] is not None:
        assert GembaseUtils.is_bool(data.payload["include_concepts"])
        include_concepts = data.payload["include_concepts"]

    return gb_session().models().search().get_developers_hints(
        title=data.payload['title'],
        include_concepts=include_concepts
    )


@command("portal:search_google_play_apps", [is_logged])
def portal_search_google_play_apps(data: CommandData):
    assert GembaseUtils.is_string(data.payload["app_title"], max_length=200)

    search_in_concepts = True
    if "search_in_concepts" in data.payload and data.payload["search_in_concepts"] is not None:
        assert GembaseUtils.is_bool(data.payload["search_in_concepts"])
        search_in_concepts = data.payload["search_in_concepts"]

    return gb_session().models().search().search_app_by_title(
        title=data.payload["app_title"],
        search_in_concepts=search_in_concepts
    )


@command("portal:get_developers_details", [is_logged])
def portal_get_developers_details(data: CommandData):
    return gb_session().models().apps().get_devs_details(
        dev_ids_int=data.payload[UserObfuscator.DEV_IDS_INT]
    )


@command("portal:get_app_details", [is_logged])
def portal_get_store_app_detail(data: CommandData):
    app_ids_int: list[int] | None = None

    if UserObfuscator.APP_ID_INT in data.payload:
        app_ids_int = [data.payload[UserObfuscator.APP_ID_INT]]
    elif UserObfuscator.APP_IDS_INT in data.payload:
        app_ids_int = data.payload[UserObfuscator.APP_IDS_INT]

    if not gb_session().models().apps().scrap_apps_if_not_scraped(
        app_ids_int=app_ids_int
    ):
        return {
            "state": 2,
            "app_details": []
        }

    include_gallery = False
    if "include_gallery" in data.payload:
        assert GembaseUtils.is_bool(data.payload["include_gallery"])
        include_gallery = data.payload["include_gallery"]

    app_details = gb_session().models().apps().get_apps_details(
        app_ids_int=app_ids_int,
        include_gallery=include_gallery
    )

    return {
        "state": 1 if len(app_details) > 0 else -1,
        "app_details": app_details
    }


@command("portal:scrap_app", [is_logged])
def portal_scrap_app(data: CommandData):
    assert GembaseUtils.is_string(data.payload["app_id_in_store"], max_length=200)
    assert GembaseUtils.is_int(data.payload["store"])

    force = False
    if "force" in data.payload:
        assert GembaseUtils.is_bool(data.payload["force"])
        force = data.payload["force"]

    return gb_session().models().scraper().scrap_app(
        app_id_in_store=data.payload["app_id_in_store"],
        store=data.payload["store"],
        force=force
    )


@command("portal:scrap_dev", [is_logged])
def portal_scrap_dev(data: CommandData):
    assert GembaseUtils.is_string(data.payload["dev_id_in_store"], max_length=200)
    assert GembaseUtils.is_int(data.payload["store"])

    return gb_session().models().scraper().scrap_dev(
        dev_id_in_store=data.payload["dev_id_in_store"],
        store=data.payload["store"],
        scrap_apps=True
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
    assert GembaseUtils.is_guid(data.get("uuid"))
    return gb_session().models().platform().get_opportunity_detail(
        uuid=data.get("uuid")
    )


@command("portal:save_tags_from_games_explorer", [is_logged])
def portal__save_tags_from_games_explorer(data: CommandData):
    tags_per_app = {}

    for it in data.payload:

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
    app_id_int = data.get(UserObfuscator.APP_ID_INT)

    primary_dev_id_int = AppModel.get_primary_dev_id_int(
        conn=gb_session().conn(),
        app_id_int=app_id_int
    )

    my_dev_id_int = gb_session().user().get_dev_id_int()

    return gb_session().models().tags().create_user_override_request(
        app_id_int=data.payload[UserObfuscator.APP_ID_INT],
        tags_details=data.payload["tags_details"],
        auto_confirm=primary_dev_id_int == my_dev_id_int
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
