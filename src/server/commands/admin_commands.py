import datetime
import json

from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from gembase_server_core.private_data.private_data_model import PrivateDataModel
from src import external_api
from src.server.models.apps.app_data_model import AppDataModel
from src.server.models.apps.app_model import AppModel
from src.server.models.dms.dms_constants import DmsConstants
from src.server.models.dms.dms_model import DmsModel, DmsCache
from src.server.models.platform_values.cache.platform_values_cache import PlatformValuesCache
from src.server.models.emails.email_templates.email_factory import EmailFactory
from src.server.models.emails.email_templates.follow_up_email import FollowUpEmail, FOLLOW_UP_EMAIL_ID
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.server.models.session.models.admin_session_model import AdminSessionModel
from src.server.models.user.user_emails import UserEmails
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.tags.tags_constants import TagsConstants
from src.server.models.user.user_data import UserData
from src.server.models.user.user_obfuscator import UserObfuscator
from src.session.session import gb_session, is_admin, is_user
from src.utils.gembase_utils import GembaseUtils


def is_admin_or_kyle():
    return is_admin() or is_user(36)


@command("admin:get_services", [is_admin])
def admin__get_services():
    rows = gb_session().conn().select_all("""
    SELECT s.pid, s.service_data, s.script, s.error, s.finished, s.error_message, s.status,
    UNIX_TIMESTAMP(s.heartbeat) as heartbeat,
    UNIX_TIMESTAMP(s.created) as created
      FROM app_temp_data.server_services s
    """)

    res = []
    for row in rows:
        row["service_data"] = json.loads(row["service_data"]) if row["service_data"] is not None else None
        res.append(row)

    return res


@command("admin:rebuild_platform_values", [is_admin])
def admin__rebuild_platform_values():
    ServiceWrapperModel.run(
        s="src/services/platform_values_rebuild_service.py"
    )


@command("admin:block_user", [is_admin])
def admin__block_user(data: CommandData):
    return gb_session().models().admin().block_user(
        email=data.get("email"),
        remove_block=data.get("remove_block", default_value=False)
    )


@command("admin:set_free_trial_date", [is_admin_or_kyle])
def admin__set_free_trial_date(data: CommandData):
    email = data.payload["email"]
    timestamp = data.payload["timestamp"]
    conn = gb_session().conn()

    conn.query("""
    UPDATE app.users SET free_trial_end_t = FROM_UNIXTIME(%s) WHERE email = %s
    """, [timestamp, email])

    conn.query("""
    UPDATE app.users_registration_requests SET free_trial_end_t = FROM_UNIXTIME(%s) WHERE email = %s
    """, [timestamp, email])


@command("admin:get_sent_emails", [is_admin_or_kyle])
def admin__get_sent_emails(data: CommandData):
    email = data.payload["email"]
    conn = gb_session().conn()

    rows = conn.select_all("""
    SELECT id, UNIX_TIMESTAMP(t) as t, subject, body, email_def, from_composer 
    FROM archive.users_sent_emails WHERE email = %s
    """, [email])

    return rows


@command("admin:whitelist_email_request", [is_admin_or_kyle])
def admin__whitelist_email_request(data: CommandData):
    email = data.payload["email"]
    conn = gb_session().conn()

    rows = conn.select_all("""
    SELECT IF(u.blocked IS NULL, 0, 1) as blocked
      FROM app.users u WHERE u.email = %s
    UNION
    SELECT IF(r.blocked IS NULL, 0, 1) as blocked
      FROM app.users_registration_requests r WHERE r.email = %s
    UNION
    SELECT 0 as blocked
      FROM app.organization_requests r WHERE r.email = %s
    """, [email, email, email])

    if len(rows) == 0:
        return "ok"

    for row in rows:
        if row["blocked"] == 1:
            return "blocked"

    return "email_whitelisted"


@command("admin:confirm_whitelist_email", [is_admin_or_kyle])
def admin__confirm_whitelist_email(data: CommandData):
    email = data.payload["email"]
    name = data.payload["name"]
    position_role = data.payload["position_role"]
    position_area = data.payload["position_area"]
    dev_id_in_store = data.payload["dev_id_in_store"]
    dev_store = data.payload["dev_store"]
    dev_concept_title = data.payload["dev_concept_title"]
    request_id = data.payload["request_id"] if "request_id" in data.payload else None
    dev_id_int = data.get(UserObfuscator.DEV_ID_INT)

    conn = gb_session().conn()

    row_request = conn.select_one_or_none("""
    SELECT request_guid, UNIX_TIMESTAMP(blocked) as blocked
     FROM app.registration_whitelist_pending WHERE request_guid = %s
    """, [request_id])

    if row_request is not None:
        if row_request["blocked"] is not None:
            return
        request_guid = row_request["request_guid"]
    else:
        request_guid = GembaseUtils.get_guid()

    conn.query("""
    DELETE FROM app.registration_whitelist_pending WHERE email = %s
    """, [email])

    if dev_id_int is None:

        if dev_concept_title != "":
            dev_id_int = AppModel.create_dev_concept(
                conn=conn,
                title=dev_concept_title
            )["dev_id_int"]
        elif dev_id_in_store != "":
            if dev_store == AppModel.STORE__CONCEPT:
                dev_id_int = AppDataModel.get_dev_id_int(conn=conn, dev_id=dev_id_in_store)
            else:
                scraped_dev = ScraperModel.scrap_dev(
                    conn=conn,
                    dev_id_in_store=dev_id_in_store,
                    store=dev_store,
                    scrap_dev_apps=True
                )
                if scraped_dev["state"] != 1:
                    raise Exception(f"Problem with developer scraping")

                dev_id_int = scraped_dev["dev_id_int"]

    assert dev_id_int is not None and dev_id_int != 0

    conn.query("""
    INSERT INTO app.users_registration_requests 
    (guid, email, name, position_area, position_role, dev_id_int) 
    VALUES (%s, %s, %s, %s, %s, %s)
    """, [request_guid, email, name, position_area, position_role, dev_id_int])


@command("admin:get_request_tags_override_by_user", [is_admin])
def admin__get_request_tags_override_by_user():
    return gb_session().models().admin().get_user_tags_override_client_data()


@command("admin:confirm_request_tags_override_by_user", [is_admin])
def admin__confirm_request_tags_override_by_user(data: CommandData):
    return gb_session().models().tags().confirm_user_tags_override_request(
        request_id=data.payload["request_id"],
        tags=data.payload["tags"]
    )


@command("admin:reject_request_tags_override_by_user", [is_admin])
def admin__reject_request_tags_override_by_user(data: CommandData):
    return gb_session().models().admin().reject_user_tags_override_request(
        request_id=data.payload["request_id"]
    )


@command("admin:update_email_templates_from_sheet", [is_admin_or_kyle])
def admin__update_email_templates_from_sheet():
    gb_session().models().admin().update_email_templates_from_sheet()


@command("admin:reset_email_draft", [is_admin_or_kyle])
def admin__reset_email_draft(data: CommandData):
    draft_id = data.payload["draft_id"]

    m = EmailFactory.load_draft(draft_id=draft_id, from_composer=True)
    m.delete_draft()
    m.create_draft()

    data = m.get_client_data(user=gb_session().user())

    return data


@command("admin:get_or_create_email_draft", [is_admin_or_kyle])
def admin__get_or_create_email_draft(data: CommandData):
    email = data.payload["email"]
    template_def = data.payload["template_def"]

    conn = gb_session().conn()

    email_id = UserData.get_or_create_email_id(conn=conn, email=email)

    row = conn.select_one_or_none("""
    SELECT id FROM app.users_email_draft WHERE template_def = %s
    AND email_id = %s
    """, [template_def, email_id])

    if row is not None:
        m = EmailFactory.load_draft(draft_id=row["id"], from_composer=True)
    else:
        m = EmailFactory.create(template_def=template_def, from_composer=True)
        m.create_draft(email=email)

    return m.get_client_data(user=gb_session().user())


@command("admin:save_email_draft", [is_admin_or_kyle])
def admin__save_email_draft(data: CommandData):
    draft_id = data.payload["draft_id"]
    m = EmailFactory.load_draft(draft_id=draft_id, from_composer=True)
    m.save_draft(instance_data=data.payload["instance_data"])


@command("admin:send_email_draft", [is_admin_or_kyle])
def admin__send_email_draft(data: CommandData):
    draft_id = data.payload["draft_id"]
    to_test_user = False
    if "to_test_user" in data.payload:
        to_test_user = data.payload["to_test_user"]
    to_current_user = False
    if "to_test_user" in data.payload:
        to_current_user = data.payload["to_current_user"]
    m = EmailFactory.load_draft(draft_id=draft_id, from_composer=True)
    return m.send(to_test_user=to_test_user, to_current_user=to_current_user)


@command("admin:get_followup_email_parameters", [is_admin_or_kyle])
def admin__get_followup_email_parameters(data: CommandData):
    draft_id = data.payload["draft_id"]

    audience_angle_id_int = None
    if UserObfuscator.AUDIENCE_ANGLE_ID_INT in data.payload:
        audience_angle_id_int = data.payload[UserObfuscator.AUDIENCE_ANGLE_ID_INT]
    app_id_int = None
    if UserObfuscator.APP_ID_INT in data.payload:
        app_id_int = data.payload[UserObfuscator.APP_ID_INT]

    m = FollowUpEmail(conn=gb_session().conn(), session_user=gb_session().user(), draft_id=draft_id, from_composer=True)
    data = m.set_parameters(
        audience_angle_id_int=audience_angle_id_int,
        app_id_int=app_id_int
    )

    return {
        "instance": m.get_client_data(user=gb_session().user()),
        "data": data
    }


@command("admin:get_email_templates", [is_admin_or_kyle])
def admin__get_email_templates():
    templates = EmailFactory.load_templates()
    themes = []
    for row in UserEmails.THEMES:
        themes.append({
            "theme": row["theme"],
            "wrapper": UserEmails.get_wrapper(theme=row["theme"], include_email_unsubscribe=True)
        })

    res = {
        "themes": themes,
        "templates": [x.get_template_def() for x in templates]
    }

    return res


@command("admin:users_set_text", [is_admin_or_kyle])
def admin__users_set_text(data: CommandData):
    return gb_session().models().admin().users_set_text(
        email=data.get("email"),
        customer_id=data.get("customer_id"),
        parameter=data.get("parameter"),
        value=data.get("value"),
        is_timestamp=data.get("is_timestamp")
    )


@command("admin:users_delete", [is_admin_or_kyle])
def admin__users_delete(data: CommandData):
    return gb_session().models().admin().users_delete(
        email=data.get("email"),
        customer_id=data.get("customer_id")
    )


@command("admin:users_add", [is_admin_or_kyle])
def admin__users_add():
    return gb_session().models().admin().users_add()


@command("admin:users_set_developer", [is_admin_or_kyle])
def admin__users_set_developer(data: CommandData):
    return gb_session().models().admin().users_set_developer(
        email=data.get("email"),
        dev_id_in_store=data.get("dev_id_in_store"),
        store=data.get("store"),
        customer_id=data.get("customer_id"),
        is_concept=data.get("is_concept"),
        concept_name=data.get("concept_name")
    )


@command("admin:set_my_developer", [is_admin_or_kyle])
def admin__set_my_developer(data: CommandData):
    return gb_session().models().admin().users_set_developer(
        email=gb_session().user().get_email(),
        dev_id_in_store=data.get("dev_id_in_store"),
        store=data.get("store"),
        remove_existing_apps=True
    )


@command("admin:send_all_emails", [is_admin])
def admin__send_all_emails(data: CommandData):
    org_name = gb_session().user().get_organization_title()
    email = gb_session().user().get_email()
    user_name = gb_session().user().get_name()
    to_email = data.payload["to_email"]

    UserEmails.not_whitelisted_email_from_user(
        subscribe_guid="test",
        to_user_email=to_email,
        request_guid="0",
        from_user_email=email,
        from_user_name=user_name
    )

    UserEmails.password_change(
        subscribe_guid="test",
        user_name=user_name,
        to_address=to_email,
        guid="0"
    )

    UserEmails.not_whitelisted_email(
        subscribe_guid="test",
        to_user_email=to_email,
        request_guid="0"
    )

    UserEmails.organization_invite_mail_for_new_user(
        subscribe_guid="test",
        request_guid="0",
        to_address=to_email,
        organization_admin_email=email,
        locked=False,
        organization_name="Organization" if org_name is None else org_name
    )

    UserEmails.already_registered_email(
        subscribe_guid="test",
        to_email=to_email
    )

    UserEmails.organization_invite_mail_for_existing_user(
        subscribe_guid="test",
        locked=False,
        organization_admin_email=email,
        request_guid="0",
        user_email=to_email,
        user_name=user_name
    )

    UserEmails.invite_mail_for_user_without_organization(
        subscribe_guid="test",
        to_email=to_email,
        user_name=user_name,
        request_guid="0",
        locked=False
    )


@command("admin:fake_login", [is_admin])
def admin__fake_login(data: CommandData):
    user_id = UserData.get_user_id_from_email(conn=gb_session().conn(), email=data.payload["email"])
    if user_id != 0:
        gb_session().conn().query("""
        UPDATE app.users u SET u.fake_login = %s WHERE u.id = %s
        """, [user_id, gb_session().user_id()])
        return {
            "state": "ok"
        }
    return {
        "state": "not_found"
    }


@command("admin:get_data_per_emails", [is_admin_or_kyle])
def admin__get_data_per_emails():

    return []

    data = external_api.read_sheet("1VpZ24bAZ84IJI-0nsX0VxcZOk0KB4qD_s8u95f8t3rw", "Clients", True)
    followup_override = {}
    for row in data:
        if "Followup" in row and row["Followup"] is not None and row["Followup"] != "":
            date_element = datetime.datetime.strptime(row["Followup"], "%Y-%m-%d")
            timestamp = date_element.timestamp()
            followup_override[row["Email"]] = timestamp

    conn = gb_session().conn()

    rows_sent_emails = conn.select_all("""
    SELECT DISTINCT e.email_def, m.email, 
    FIRST_VALUE(UNIX_TIMESTAMP(e.t)) over (PARTITION BY e.email, e.email_def ORDER BY e.t DESC) as t,
    FIRST_VALUE(UNIX_TIMESTAMP(e.opened_t)) over (PARTITION BY e.email, e.email_def ORDER BY e.t DESC) as opened_t
      FROM archive.users_sent_emails e,
           app.map_user_email m
     WHERE e.email_def IS NOT NULL
       AND m.email = e.email
    """)

    rows_subscription = conn.select_all("""
    SELECT m.email, u.subscribed
      FROM app.users_email_subscription u,
           app.map_user_email m
     WHERE u.email_id = m.id
    """)

    res_map = {}

    for row in rows_sent_emails:
        email = row["email"]
        if email not in res_map:
            res_map[email] = {
                "sent_emails": []
            }
        res_map[email]["sent_emails"].append(row)
    for email in followup_override:
        if email not in res_map:
            res_map[email] = {
                "sent_emails": []
            }
        found = False
        for email_data in res_map[email]["sent_emails"]:
            if email_data["email_def"] == FOLLOW_UP_EMAIL_ID:
                found = True
                email_data["t"] = followup_override[email]
        if not found:
            res_map[email]["sent_emails"].append({
                "email_def": FOLLOW_UP_EMAIL_ID,
                "email": email,
                "t": followup_override[email]
            })
    for row in rows_subscription:
        email = row["email"]
        if email not in res_map:
            res_map[email] = {}
        res_map[email]["subscription"] = row

    return [{
        "email": k,
        "data": res_map[k]
    } for k in res_map]


@command("admin:get_users", [is_admin_or_kyle])
def admin__get_users():
    return gb_session().models().admin().get_users()


@command("admin:save_tags_from_games_explorer", [is_admin])
def admin__save_tags_from_games_explorer(data: CommandData):
    conn = gb_session().conn()
    apps_ids_int = []
    for it in data.payload:
        if it["app_id_int"] not in apps_ids_int:
            apps_ids_int.append(it["app_id_int"])
        conn.query("""
        DELETE FROM tagged_data.tags t
        WHERE t.app_id_int = %s
          AND t.tag_id_int = %s
        """, [it["app_id_int"], it["tag_id_int"]])
        if it["b"]:
            conn.query("""
            INSERT INTO tagged_data.tags (app_id_int, prompt_row_id, tag_id_int, tag_rank)
            VALUES (%s, 0, %s, %s)
            """, [it["app_id_int"], it["tag_id_int"], it["tag_rank"]])

    if len(apps_ids_int) > 0:
        for app_id_int in apps_ids_int:
            hist_id = conn.insert("""
                    INSERT INTO tagged_data.platform_tagged_history (app_id_int, context)
                    VALUES (%s, %s)
                    """, [app_id_int, "admin_set_tags"])
            conn.query("""
                    INSERT INTO tagged_data.tags_history (hist_id, prompt_row_id, app_id_int, tag_id_int, 
                    tag_rank, added_from_store, removed_from_store, is_tag_rank_override, tag_rank_override)
                    SELECT %s as hist_id, t.prompt_row_id, t.app_id_int, t.tag_id_int, t.tag_rank,
                    t.added_from_store, t.removed_from_store, t.is_tag_rank_override, t.tag_rank_override
                      FROM tagged_data.tags t
                     WHERE t.app_id_int = %s
                    """, [hist_id, app_id_int])
            conn.query("""
            DELETE FROM tagged_data.platform_tagged t
            WHERE t.app_id_int = %s
            """, [app_id_int])
            prompts_b = conn.select_one("""
            SELECT b.b FROM tagged_data.active_prompts_b b
            """)["b"]
            conn.query("""
            INSERT INTO tagged_data.platform_tagged (app_id_int, prompts_b, manual) 
            VALUES (%s, %s, 1)
            """, [app_id_int, prompts_b])

            PlatformValuesCache.start_service_for_single_app(app_id_int=app_id_int)


@command("admin:save_tags_for_existing_app", [is_admin])
def admin__save_tags_for_existing_app(data: CommandData):
    AppModel.set_tags(
        conn=gb_session().conn(),
        user_id=gb_session().user_id(),
        app_id_int=data.get("app_id_int"),
        tags_details=data.get("tags_details"),
        context="admin_set_tags"
    )


@command("admin:get_sheets", [is_admin])
def admin_get_sheets():
    res = []
    sheets = PrivateDataModel.get_private_data()["google"]["google_docs"]
    for k in sheets:
        res.append({
            'name': k,
            'sheet_id': sheets[k]['sheet_id'],
            'dms_guid': sheets[k]['dms_guid']
        })
    return res


@command("admin:update_dms_from_sheet", [is_admin])
def admin_update_dms_from_sheet(data: CommandData):
    return AdminSessionModel.update_def_sheets(
        conn=gb_session().conn(),
        sheet_name=data.get("name")
    )
