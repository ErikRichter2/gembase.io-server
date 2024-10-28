from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from src.external_api.gmail import CC_ADMIN_EMAIL
from src.server.models.apps.app_model import AppModel
from src.server.models.emails.email_templates.invite_by_user_email import InviteByUserEmail
from src.server.models.user.user_emails import UserEmails
from src.server.models.user.user_registration_helper import UserRegistrationHelper
from src.server.models.user.organization_helper import OrganizationHelper
from src.server.models.user.user_data import UserData
from src.server.models.user.user_model import UserModel
from src.session.session import gb_session, is_logged
from src.session.session_helper import GbSessionHelper
from src.utils.gembase_utils import GembaseUtils


@command("registration:get_change_password_data")
def registration__get_change_password_data(data: CommandData):
    assert GembaseUtils.is_guid(data.payload["request_guid"])

    conn = gb_session().conn()
    row = conn.select_one_or_none("""
    SELECT u.email
      FROM app_temp_data.users_password_reset_pending up,
           app.users u
     WHERE up.guid = %s
       AND up.id = u.id
       AND u.blocked IS NULL
    """, [data.payload["request_guid"]])

    if row is not None:
        return {
            "email": row["email"]
        }


@command("registration:get_email_subscription_state")
def registration__get_email_subscription_state(data: CommandData):
    assert GembaseUtils.is_guid(data.payload["request_guid"])

    conn = gb_session().conn()
    row = conn.select_one_or_none("""
    SELECT u.subscribed
      FROM app.users_email_subscription u
     WHERE u.request_guid = %s
    """, [data.payload["request_guid"]])

    subscribed = 1
    if row is not None:
        subscribed = row["subscribed"]

    return subscribed


@command("registration:set_email_subscription_state")
def registration__set_email_subscription_state(data: CommandData):
    assert GembaseUtils.is_guid(data.payload["request_guid"])
    assert GembaseUtils.is_bool(data.payload["subscribed"])

    request_guid = data.payload["request_guid"]
    subscribed = data.payload["subscribed"]

    conn = gb_session().conn()

    row = conn.select_one_or_none("""
    SELECT 1
      FROM app.users_email_subscription
     WHERE request_guid = %s
    """, [request_guid])

    if row is not None:
        conn.query("""
        UPDATE app.users_email_subscription SET subscribed = %s
         WHERE request_guid = %s
        """, [subscribed, request_guid])
    else:
        row = conn.select_one_or_none("""
            SELECT email
              FROM app.registration_whitelist_pending
             WHERE request_guid = %s
            """, [request_guid])

        if row is not None:

            email_id = UserData.get_or_create_email_id(
                conn=conn,
                email=row["email"]
            )

            row = conn.select_one_or_none("""
            SELECT 1
              FROM app.users_email_subscription
             WHERE email_id = %s
            """, [email_id])

            if row is not None:
                conn.query("""
                UPDATE app.users_email_subscription SET subscribed = %s WHERE email_id = %s
                """, [subscribed, email_id])
            else:
                conn.query("""
                INSERT INTO app.users_email_subscription (email_id, request_guid, subscribed)
                VALUES (%s, %s, %s)
                """, [email_id, request_guid, subscribed])


@command("registration:change_password")
def registration__change_password(data: CommandData):
    assert GembaseUtils.is_guid(data.payload["request_guid"])
    assert GembaseUtils.is_string(data.payload["password"])
    assert GembaseUtils.is_string(data.payload["recaptcha_token"])

    GbSessionHelper.validate_recaptcha(data.payload['recaptcha_token'])

    conn = gb_session().conn()
    row = conn.select_one("""
    SELECT u.id
      FROM app_temp_data.users_password_reset_pending u
     WHERE u.guid = %s
    """, [data.payload["request_guid"]])

    conn.query("""
    DELETE FROM app_temp_data.users_password_reset_pending u
     WHERE u.id = %s
    """, [row["id"]])

    UserModel(
        conn=conn,
        user_id=row["id"]
    ).set_password(
        password=data.payload["password"]
    )

    gb_session().logout()

    return {
        "state": "ok"
    }


@command("registration:invite_by_user", [is_logged])
def registration__invite_by_user(data: CommandData):
    email = data.payload["email"]
    assert GembaseUtils.is_email(email)

    conn = gb_session().conn()

    invite_by_user_email = InviteByUserEmail(
        conn=conn,
        session_user=gb_session().user()
    )
    invite_by_user_email.set_email(email=email)
    invite_by_user_email.send()


@command("registration:by_email")
def registration__by_email(data: CommandData):
    assert GembaseUtils.is_email(data.payload["email"])

    user_id = gb_session().logged_user_id()
    conn = gb_session().conn()

    if user_id is None:
        assert GembaseUtils.is_string(data.payload["recaptcha_token"])
        GbSessionHelper.validate_recaptcha(data.payload['recaptcha_token'])

    email = data.payload["email"]

    user = None

    if user_id is not None:

        user = gb_session().user()

        if user.get_email() == email:
            return

        # if this user is organization admin and this email domain is allowed, create
        # organization request

        if user.get_organization().is_organization_admin():
            if OrganizationHelper.is_email_allowed(
                    conn=conn,
                    organization_id=user.get_organization().get_id(),
                    email=email
            ):
                OrganizationHelper.send_organization_invite(
                    conn=conn,
                    from_user_email=user.get_email(),
                    request_guid=OrganizationHelper.add_request(
                        conn=conn,
                        organization_id=user.get_organization().get_id(),
                        email=email,
                        from_user=user
                    )
                )

                return

    UserRegistrationHelper.send_registration_email(
        conn=conn,
        email=email,
        from_user=user
    )


@command("registration:get_registration_confirm_def")
def registration__get_registration_confirm_def(data: CommandData):

    assert GembaseUtils.is_guid(data.payload["request_guid"])

    guid = data.payload["request_guid"]
    conn = gb_session().conn()
    test = False
    if "test" in data.payload:
        test = data.payload["test"]

    row = conn.select_one_or_none("""
    SELECT r.organization_id, 
           o.dev_id_int,
           r.email,
           r.temp_user,
           r.name,
           r.position_area,
           r.position_role
      FROM app.organization_requests r,
           app.organization o
     WHERE r.request_guid = %s
       AND r.organization_id = o.id
    """, [guid])

    if row is not None:

        user_id = UserData.get_user_id_from_email(
            conn=conn,
            email=row["email"]
        )

        if user_id == 0:

            dev_data = AppModel.get_devs_details(
                conn=conn,
                devs_ids_int=[row["dev_id_int"]]
            )[row["dev_id_int"]]

            rows_position_role = conn.select_all("SELECT id, value FROM app.def_user_position_role")
            rows_position_area = conn.select_all("SELECT id, value FROM app.def_user_position_area")

            return {
                "state": "register",
                "position_role_def": rows_position_role,
                "position_area_def": rows_position_area,
                "dev_title": dev_data["title"],
                "user_data": {
                    "email": row["email"],
                    "name": row["name"],
                    "position_role": row["position_role"],
                    "position_area": row["position_area"]
                }
            }

        else:

            UserModel(
                conn=conn,
                user_id=user_id
            ).set_organization_from_request(
                request_guid=guid
            )

            return {
                "state": "platform"
            }

    else:

        if "email" in data.payload and data.payload["email"] is not None:
            user_id = UserData.get_user_id_from_email(conn=conn, email=data.payload["email"])
            if user_id != 0:
                return {
                    "state": "platform"
                }

        row = conn.select_one_or_none("""
        SELECT r.dev_id_int,
               r.email,
               r.temp_user,
               r.name,
               r.position_role,
               r.position_area
          FROM app.users_registration_requests r
         WHERE r.guid = %s
           AND r.blocked IS NULL
        """, [guid])

        if row is not None:

            if not test:
                conn.query("""
                UPDATE app.users_registration_requests 
                SET responded_t = NOW()
                WHERE guid = %s
                """, [guid])

            user_id = UserData.get_user_id_from_email(
                conn=conn,
                email=row["email"]
            )

            if user_id == 0:

                dev_data = AppModel.get_devs_details(
                    conn=conn,
                    devs_ids_int=[row["dev_id_int"]]
                )[row["dev_id_int"]]

                rows_position_role = conn.select_all("SELECT id, value FROM app.def_user_position_role")
                rows_position_area = conn.select_all("SELECT id, value FROM app.def_user_position_area")

                return {
                    "state": "register",
                    "position_role_def": rows_position_role,
                    "position_area_def": rows_position_area,
                    "dev_title": dev_data["title"],
                    "user_data": {
                        "email": row["email"],
                        "name": row["name"],
                        "position_role": row["position_role"],
                        "position_area": row["position_area"]
                    }
                }

    row = conn.select_one_or_none("""
    SELECT email
      FROM app.registration_whitelist_pending
      WHERE request_guid = %s
      AND blocked IS NULL
    """, [guid])

    if row is not None:

        email = row["email"]

        UserData.get_or_create_email_id(
            conn=conn,
            email=email
        )

        row_confirmed = conn.select_one_or_none("""
        SELECT confirmed FROM app.registration_whitelist_pending
        WHERE request_guid = %s
        """, [guid])

        if row_confirmed is not None and row_confirmed["confirmed"] == 0:
            UserEmails.admin__whitelist_requested(
                request_guid=guid,
                request_email=email,
                to_email=CC_ADMIN_EMAIL
            )

        if not test:
            conn.query("""
            UPDATE app.registration_whitelist_pending
               SET confirmed = 1, responded_t = NOW()
             WHERE request_guid = %s
            """, [guid])

    return {
        "state": "whitelist_pending"
    }


@command("registration:confirm_registration_request")
def registration__confirm_registration_request(data: CommandData):

    assert GembaseUtils.is_guid(data.payload["request_guid"])

    guid = data.payload["request_guid"]
    conn = gb_session().conn()

    row = conn.select_one_or_none("""
    SELECT r.organization_id, 
           o.dev_id_int,
           r.email,
           r.temp_user,
           UNIX_TIMESTAMP(r.sent_request_t) as sent_request_t
      FROM app.organization_requests r,
           app.organization o
     WHERE r.request_guid = %s
       AND r.organization_id = o.id
    """, [guid])

    if row is not None:

        email = row["email"]

        user_id = UserData.get_user_id_from_email(
            conn=conn,
            email=email
        )

        if user_id == 0:

            assert GembaseUtils.is_string(data.payload["password"])
            assert GembaseUtils.is_int(data.payload["position_role"])
            assert GembaseUtils.is_int(data.payload["position_area"])
            assert GembaseUtils.is_string(data.payload["name"])

            user_id = UserData.create_user(
                conn=conn,
                email=email,
                password_raw=data.payload["password"],
                role=0,
                name=data.payload["name"],
                position_area=data.payload["position_area"],
                position_role=data.payload["position_role"],
                sent_request_t=row["sent_request_t"]
            )

            UserModel(
                conn=conn,
                user_id=user_id
            ).set_organization_from_request(
                request_guid=guid
            )

            login_data = gb_session().login(
                email=email,
                password=data.payload["password"]
            )

            return {
                "state": "created",
                "login_data": login_data
            }

        else:

            UserModel(
                conn=gb_session().conn(),
                user_id=user_id
            ).get_organization(
            ).set_organization(
                organization_id=row["organization_id"]
            )

            return {
                "state": "updated",
            }

    else:

        row = conn.select_one_or_none("""
        SELECT r.dev_id_int,
               r.email,
               r.temp_user,
               r.credits,
               UNIX_TIMESTAMP(r.sent_request_t) as sent_request_t,
               r.organization_domain,
               r.organization_role,
               UNIX_TIMESTAMP(r.free_trial_end_t) as free_trial_end_t
          FROM app.users_registration_requests r
         WHERE r.guid = %s
           AND r.blocked IS NULL
        """, [guid])

        if row is not None:
            email = row["email"]

            user_id = UserData.get_user_id_from_email(
                conn=conn,
                email=email
            )

            if user_id == 0:

                assert GembaseUtils.is_string(data.payload["password"])
                assert GembaseUtils.is_int(data.payload["position_role"])
                assert GembaseUtils.is_int(data.payload["position_area"])
                assert GembaseUtils.is_string(data.payload["name"])

                user_id = UserData.create_user(
                    conn=conn,
                    email=email,
                    password_raw=data.payload["password"],
                    role=0,
                    name=data.payload["name"],
                    position_area=data.payload["position_area"],
                    position_role=data.payload["position_role"],
                    initial_credits=row["credits"],
                    sent_request_t=row["sent_request_t"],
                    free_trial_end_t=row["free_trial_end_t"]
                )

                user_model = UserModel(
                    conn=conn,
                    user_id=user_id
                )

                user_model.set_my_dev_and_apps(
                    dev_id_int=row["dev_id_int"],
                    remove_existing_apps=True
                )

                if row["organization_domain"] is not None:

                    organization_id = OrganizationHelper.get_organization_id_by_domain(
                        conn=conn,
                        email_domain=row["organization_domain"]
                    )

                    if organization_id is None:
                        organization_id = OrganizationHelper.create(
                            conn=conn,
                            dev_id_int=user_model.get_dev_id_int(),
                            credits_value=25,
                            email_domain=row["organization_domain"],
                            prime_number=user_model.get_user_prime_number(),
                            users=[]
                        )

                    OrganizationHelper.add_user(
                        conn=conn,
                        organization_id=organization_id,
                        user_id=user_model.get_id(),
                        role=row["organization_role"]
                    )

                login_data = gb_session().login(
                    email=email,
                    password=data.payload["password"]
                )

                return {
                    "state": "created",
                    "login_data": login_data
                }

    return {
        "state": "error"
    }


@command("registration:confirm_tos")
def registration__confirm_tos():
    gb_session().user().confirm_tos()
    return {
        "state": "ok"
    }
