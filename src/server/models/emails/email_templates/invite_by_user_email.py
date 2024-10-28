from src.server.models.emails.email_templates.base_email import BaseEmail
from src.server.models.user.user_constants import uc
from src.server.models.user.user_data import UserData
from src.server.models.user.user_emails import UserEmails
from src.server.models.user.user_model import UserModel
from src.server.models.user.user_registration_helper import UserRegistrationHelper


class InviteByUserEmail(BaseEmail):

    def get_template_def(self):
        return "invite_by_user"

    def get_content_parameters(self):
        return {
            "[FROM_USER_NAME]": None,
            "[FROM_USER_EMAIL]": None,
            "[MAIN_LINK_START]": None,
            "[MAIN_LINK_END]": None
        }

    def __set_params(self):
        user = self.session_user

        if self.from_composer:
            user = UserModel(conn=self.conn, user_id=uc.get_user_id_for_guid(conn=self.conn, guid=uc.ADMIN_USER_GUID))

        self.instance_data.content_parameters["[MAIN_LINK_START]"] = self.get_registration_url()
        self.instance_data.content_parameters["[MAIN_LINK_END]"] = "</a>"
        self.instance_data.content_parameters["[FROM_USER_NAME]"] = user.get_name()
        self.instance_data.content_parameters["[FROM_USER_EMAIL]"] = UserEmails.get_email_html(
            user.get_email()
        )

    def after_draft_loaded(self):
        self.__set_params()

    def set_email(self, email: str):
        super().set_email(email=email)
        self.__set_params()

    def check_from_composer(self):

        user_id = UserData.get_user_id_from_email(conn=self.conn, email=self.email)

        if user_id != 0:
            is_whitelisted = True
        else:
            is_whitelisted = UserRegistrationHelper.get_whitelist_request_guid(
                conn=self.conn,
                email=self.email
            ) is not None

        if not is_whitelisted:
            return {
                "error": "not_whitelisted"
            }

        return None
