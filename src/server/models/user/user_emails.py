import datetime

from src.external_api.gmail import GbEmailService
from src.utils.gembase_utils import GembaseUtils


class UserEmails:

    __COLOR_EMAIL_LINK = "#005c3b"
    __COLOR_EMAIL_BG = "#f5f7fb"
    __COLOR_EMAIL_TEXT = "#000000"
    __COLOR_EMAIL_FOOTER = "#545d84"
    __COLOR_EMAIL_UNSUBSCRIBE = "#98a4d7"
    __COLOR_EMAIL_TEXT_BG = "#ffffff"

    MAIN_LINK_STYLE = f"""
    style="text-decoration: underline; color: {__COLOR_EMAIL_LINK};"
    """

    __UNSUBSCRIBE_ELEMENT = f"""
    <tr>
        <td>
            <a href="[gb__email_unsubscribe_url]" style="color: {__COLOR_EMAIL_UNSUBSCRIBE}; font-size: 0.8rem; text-decoration: underline;">Unsubscribe</a>
        </td>
    </tr>
    """

    __UNSUBSCRIBE_ELEMENT_v2 = f"""
                        <a href="[gb__email_unsubscribe_url]" style="color: {__COLOR_EMAIL_UNSUBSCRIBE}; font-size: 0.8rem; text-decoration: underline;">Unsubscribe</a>

        """

    __HTML_WRAPPER_HEAD = """
    <head>
        <meta http-equiv="Content-Type" content="text/html charset=UTF-8" />
    </head>
    """

    __HTML_WRAPPER_BODY = f"""
    <table style="width: 100%;">
                        <tr height="30">
                            <td colspan="3">
                            </td>
                        </tr>
                        <tr style="width: 100%;">
                            <td align="center">
                                <table style="background-color: {__COLOR_EMAIL_TEXT_BG}; width: 60%; min-width: 400px;">
                                    <tr height="40" style="height: 40px;">
                                        <td colspan="3">
                                        </td>
                                    </tr>
                                    <tr>
                                        <td width="30" style="width: 30px; white-space: nowrap;">
                                        <div style="width: 30px; white-space: nowrap;"></div>
                                        </td>
                                        <td style="width: 100%;">
                                            <table style="width: 100%;">
                                                <tr>
                                                </tr>
                                                <tr style="width: 100%;">
                                                    <td style="width: 100%;">
                                                        <div>[gb__title]</div>
                                                        [gb__content]
                                                        <p>
                                                            [gb__footer]
                                                        </p>
                                                        <p style="width: 100%;">
                                                            <table style="width: 100%;">
                                                                <tr style="width: 100%;">
                                                                    <td style="width: 100%;" colspan="2">
                                                                        <div style="background-color: {__COLOR_EMAIL_FOOTER}; width: 100%; height: 1px;"></div>
                                                                    </td>
                                                                </tr>
                                                                <tr height="5" style="height: 5px;">
                                                                    <td colspan="2">
                                                                    </td>
                                                                </tr>
                                                                <tr style="width: 100%;">
                                                                    <td style="width: 100%;">
                                                                                <div style="color: {__COLOR_EMAIL_FOOTER}; font-size: 0.8rem;">
                                                                                    © [gb__year] Gembase.io, s.r.o., Nove Zahrady I/9, 82105, Bratislava, Slovakia
                                                                                    <br>
                                                                                    [gb__email_unsubscribe_element]
                                                                                </div>
                                                                            </td>
                                                                            <td align="right" style="text-align: right;">
                                                                                <img style="height: 30px; width: auto;" src="https://server.gembase.io:5000/email-logo/[gb__email_logo_guid]">
                                                                            </td>
                                                                </tr>
                                                            </table>
                                                        </p>
                                                    </td>
                                                </tr>
                                            </table>
                                        </td>
                                        <td width="30" style="width: 30px; white-space: nowrap;">
                                        <div style="width: 30px; white-space: nowrap;"></div>
                                        </td>
                                    </tr>
                                    <tr height="15" style="height: 15px;">
                                        <td colspan="3">
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                        <tr height="30">
                            <td colspan="3">
                            </td>
                        </tr>
                    </table>
    """

    HTML_WRAPPER = f"""
    <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
    <html>
        {__HTML_WRAPPER_HEAD}
        <body style="color: {__COLOR_EMAIL_TEXT}; background-color: {__COLOR_EMAIL_BG}; font-size: 1.1rem;">
            {__HTML_WRAPPER_BODY}
        </body>
    </html>
    """

    HTML_WRAPPER_THEME_PERSONALIZED = f"""
        <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
        <html>
            <body>
                <p>
                    [gb__title]
                </p>
                [gb__content]
                <p>
                    [gb__footer]
                </p>
            </body>
        </html>
        """

    @staticmethod
    def set_email_logo_guid(wrapper: str, guid: str):
        wrapper = wrapper.replace("[gb__email_logo_guid]", guid)
        return wrapper

    @staticmethod
    def get_email_unsubscribe_url(guid: str):
        return f"{GembaseUtils.client_url_root()}/unsubscribe-email/?request={guid}"

    @staticmethod
    def set_email_unsubscribe_guid(wrapper: str, guid: str):
        wrapper = wrapper.replace("[gb__email_unsubscribe_element]", UserEmails.__UNSUBSCRIBE_ELEMENT_v2)
        wrapper = wrapper.replace("[gb__email_unsubscribe_url]", UserEmails.get_email_unsubscribe_url(guid=guid))
        return wrapper

    THEMES = [
        {
            "theme": "default",
            "wrapper": HTML_WRAPPER
        },
        {
            "theme": "personalized",
            "wrapper": HTML_WRAPPER_THEME_PERSONALIZED
        }
    ]

    @staticmethod
    def get_wrapper_by_theme(theme: str) -> str:
        for row in UserEmails.THEMES:
            if row["theme"] == theme:
                return row["wrapper"]
        for row in UserEmails.THEMES:
            if row["theme"] == "default":
                return row["wrapper"]
        raise Exception("Default theme not found")

    @staticmethod
    def get_wrapper(theme: str, include_email_unsubscribe=False, email_logo_guid: str | None = None):
        wrapper = UserEmails.get_wrapper_by_theme(theme)
        wrapper = wrapper.replace("[gb__year]", str(datetime.date.today().year))
        if include_email_unsubscribe:
            wrapper = wrapper.replace("[gb__email_unsubscribe_element]", UserEmails.__UNSUBSCRIBE_ELEMENT_v2)
        else:
            wrapper = wrapper.replace("[gb__email_unsubscribe_element]", "")

        if email_logo_guid is None or email_logo_guid == "":
            wrapper = UserEmails.set_email_logo_guid(wrapper, "")
        else:
            wrapper = UserEmails.set_email_logo_guid(wrapper, email_logo_guid)

        return wrapper

    @staticmethod
    def get_homepage_url():
        return f"""<a href="{GembaseUtils.client_url_root()}" {UserEmails.MAIN_LINK_STYLE}>Gembase.io</a>"""

    @staticmethod
    def get_email_html(email: str):
        return f"""<a href="mailto:{email}" target="_blank" style="color: {UserEmails.__COLOR_EMAIL_TEXT}; text-decoration: none !important; text-decoration:none;">{email}</a>"""

    @staticmethod
    def __get_url_html(url: str, name: str):
        return f"""<a href="{url}" target="_blank" style="color: {UserEmails.__COLOR_EMAIL_TEXT}; text-decoration: none !important; text-decoration:none;">{name}</a>"""

    @staticmethod
    def create_email_template(subscribe_guid: str | None, title: str, content: str, footer: str | None = None):
        if footer is None:
            footer = """
            Best regards,<br>
            Team Gembase.io
            """
        res = UserEmails.HTML_WRAPPER
        res = res.replace("[gb__title]", title)
        res = res.replace("[gb__content]", content)
        res = res.replace("[gb__footer]", footer)
        res = res.replace("[gb__year]", str(datetime.date.today().year))
        res = UserEmails.set_email_logo_guid(res, "")

        if subscribe_guid is None:
            res = res.replace("[gb__email_unsubscribe_element]", "")
        else:
            res = UserEmails.set_email_unsubscribe_guid(wrapper=res, guid=subscribe_guid)

        return res

    @staticmethod
    def password_change(subscribe_guid: str, to_address: str, guid: str, user_name: str):
        url = f"{GembaseUtils.client_url_root()}/change-password/?request={guid}"

        email_subject = "Gembase.io password change request"
        email_body = UserEmails.create_email_template(
            subscribe_guid=subscribe_guid,
            title=f"Hi {user_name},",
            content=f"""
                                <p>
                                    Someone has requested to change your account password for {UserEmails.__get_url_html(GembaseUtils.client_url_root(), 'Gembase.io')}.
                                </p>
                                <p>
                                    Please <a href="{url}" {UserEmails.MAIN_LINK_STYLE}>follow this link to set a new password</a>.
                                </p>
                                <p>
                                    If you haven't requested the change, you may want to inform your administrator.
                                </p>
                                """
        )

        GbEmailService.send_mail(
            subject=email_subject,
            body=email_body,
            is_html=True,
            to_address=[to_address]
        )

        return {
            "email": to_address,
            "subject": email_subject,
            "body": email_body
        }

    @staticmethod
    def organization_invite_mail_for_new_user(
            subscribe_guid: str,
            locked: bool, to_address: str, request_guid: str, organization_name: str,
            organization_admin_email: str):
        url = f"{GembaseUtils.client_url_root()}/confirm-organization-invite/?request={request_guid}"

        email_subject = "You became an authorized user of gembase.io!"
        email_body = UserEmails.create_email_template(
            subscribe_guid=subscribe_guid,
            title=f"Dear {organization_name} team member,",
            content=f"""
                        <p>
                            Congratulations - you can now use {UserEmails.__get_url_html(GembaseUtils.client_url_root(), 'gembase.io')}. 
                        </p>
                        <p>
                            Please use <a href="{url}" {UserEmails.MAIN_LINK_STYLE}>
                            this link to confirm your account</a> and finish registration.
                        </p>
                        <p>
                            Have a great time using the platform!
                        </p>
                        <p>
                            This invitation has been sent by {UserEmails.get_email_html(organization_admin_email)}.
                        </p>
                        """
        )

        GbEmailService.send_mail(
            subject=email_subject,
            body=email_body,
            is_html=True,
            to_address=[to_address],
            locked=locked,
            cc_admin=True
        )

        return {
            "email": to_address,
            "subject": email_subject,
            "body": email_body
        }

    @staticmethod
    def organization_invite_mail_for_existing_user(
            subscribe_guid: str,
            request_guid: str,
            user_name: str,
            organization_admin_email: str,
            user_email: str,
            locked: bool
    ):
        url = f"{GembaseUtils.client_url_root()}/confirm-organization-invite/?request={request_guid}"

        email_subject = "Your gembase.io account has been activated!"
        email_body = UserEmails.create_email_template(
            subscribe_guid=subscribe_guid,
            title=f"Hey {user_name},",
            content=f"""
                        <p>
                            Congratulations - you can now use {UserEmails.__get_url_html(GembaseUtils.client_url_root(), 'gembase.io')}. 
                        </p>
                        <p>
                            You can log in <a href="{url}" {UserEmails.MAIN_LINK_STYLE}>using this link</a>.
                        </p>
                        <p>
                            Have a great time using the platform!
                        </p>
                        <p>
                            This invitation has been sent by {UserEmails.get_email_html(organization_admin_email)}.
                        </p>
                        """
        )

        GbEmailService.send_mail(
            subject=email_subject,
            body=email_body,
            is_html=True,
            to_address=[user_email],
            locked=locked,
            cc_admin=True
        )

        return {
            "email": user_email,
            "subject": email_subject,
            "body": email_body
        }

    @staticmethod
    def not_whitelisted_email_from_user(
            subscribe_guid: str,
            request_guid: str,
            from_user_name: str,
            from_user_email: str,
            to_user_email: str
    ):
        url = f"{GembaseUtils.client_url_root()}/confirm-registration-by-email/?request={request_guid}"
        email_subject = "You are invited to access Gembase.io"
        email_body = UserEmails.create_email_template(
            subscribe_guid=subscribe_guid,
            title="Hi,",
            content=f"""
                            <p>
                                You have been invited to access {UserEmails.__get_url_html(GembaseUtils.client_url_root(), 'gembase.io')} by {from_user_name} ({UserEmails.get_email_html(from_user_email)}).
                            </p>
                            <p>
                                Use <a href="{GembaseUtils.client_url_root()}" {UserEmails.MAIN_LINK_STYLE}>{UserEmails.__get_url_html(GembaseUtils.client_url_root(), 'Gembase.io')}</a> to boost your game's market potential, discover unmade concepts and deeply understand all market actors.
                            </p>
                            <p>
                                Please <a href="{url}" {UserEmails.MAIN_LINK_STYLE}>follow this link to confirm your email</a> and continue registration.
                            </p>
                            <p>
                                Have a great time using the platform!
                            </p>
                            """
        )
        GbEmailService.send_mail(
            subject=email_subject,
            body=email_body,
            is_html=True,
            to_address=[to_user_email]
        )

        return {
            "email": to_user_email,
            "subject": email_subject,
            "body": email_body
        }

    @staticmethod
    def not_whitelisted_email(
            subscribe_guid: str,
            request_guid: str,
            to_user_email: str
    ):
        url = f"{GembaseUtils.client_url_root()}/confirm-registration-by-email/?request={request_guid}"
        email_subject = "You are invited to access Gembase.io"
        email_body = UserEmails.create_email_template(
            subscribe_guid=subscribe_guid,
            title="Hi,",
            content=f"""
                            <p>
                                Thank you for your interest in {UserEmails.__get_url_html(GembaseUtils.client_url_root(), 'Gembase.io')}.
                            </p>
                            <p>
                                Please <a href="{url}" {UserEmails.MAIN_LINK_STYLE}>follow this link to confirm your email</a> and continue registration.
                            </p>
                            <p>
                                Use Gembase.io to boost your game's market potential, discover unmade concepts and deeply understand all market actors.
                            </p>
                            <p>
                                Have a great time using the platform!
                            </p>
                            """
        )
        GbEmailService.send_mail(
            subject=email_subject,
            body=email_body,
            is_html=True,
            to_address=[to_user_email]
        )

        return {
            "email": to_user_email,
            "subject": email_subject,
            "body": email_body
        }

    @staticmethod
    def already_registered_email(
            subscribe_guid: str,
            to_email: str
    ):
        url = f"{GembaseUtils.client_url_root()}/password-reset"
        email_subject = "Request for Gembase.io access"
        email_body = UserEmails.create_email_template(
            subscribe_guid=subscribe_guid,
            title="Hi,",
            content=f"""
                            <p>
                                Someone requested an access to {UserEmails.__get_url_html(GembaseUtils.client_url_root(), 'Gembase.io')} to be created for this email address, even though an account for this email already exists.
                            </p>
                            <p>
                                You can log in again if you remember your password, or <a href="{url}" {UserEmails.MAIN_LINK_STYLE}>follow this link to set a new password</a>.
                            </p>
                            <p>
                                If you haven't requested the change, you may want to inform your administrator.
                            </p>
                            """
        )

        GbEmailService.send_mail(
            subject=email_subject,
            body=email_body,
            is_html=True,
            to_address=[to_email]
        )

        return {
            "email": to_email,
            "subject": email_subject,
            "body": email_body
        }

    @staticmethod
    def invite_mail_for_user_without_organization(
            subscribe_guid: str,
            request_guid: str,
            user_name: str,
            to_email: str,
            locked: bool
    ):
        url = f"{GembaseUtils.client_url_root()}/confirm-registration-by-email/?request={request_guid}"
        title = "Hi"
        if user_name is not None:
            arr = user_name.split(" ")
            title = f"Hi {arr[0]}"

        email_subject = "You have been whitelisted to access Gembase.io!"
        email_body = UserEmails.create_email_template(
            subscribe_guid=subscribe_guid,
            title=title,
            content=f"""
                    <p>
                        We are happy to invite you to use {UserEmails.__get_url_html(GembaseUtils.client_url_root(), 'Gembase.io')}.
                    </p>
                    <p>
                        Please use <a href="{url}" {UserEmails.MAIN_LINK_STYLE}>this link to finish your registration</a>.
                    </p>
                    """
        )

        GbEmailService.send_mail(
            subject=email_subject,
            body=email_body,
            is_html=True,
            to_address=[to_email],
            locked=locked,
            cc_admin=True
        )

        return {
            "email": to_email,
            "subject": email_subject,
            "body": email_body
        }

    @staticmethod
    def admin__whitelist_requested(
            request_guid: str,
            request_email: str,
            to_email: str
    ):
        url = f"{GembaseUtils.client_url_root()}/admin/whitelist-request/?email={request_email}&request={request_guid}"

        email_subject = f"Whitelist requested by {request_email}"
        email_body = UserEmails.create_email_template(
            subscribe_guid=None,
            title="",
            content=f"""
                        <p>
                            {request_email} has requested access to Gembase.io, 
                            please click <a href="{url}" {UserEmails.MAIN_LINK_STYLE}>this link to whitelist the user.</a>
                        </p>
                        """
        )

        GbEmailService.send_mail(
            subject=email_subject,
            body=email_body,
            is_html=True,
            to_address=[to_email]
        )

        return {
            "email": to_email,
            "subject": email_subject,
            "body": email_body
        }
