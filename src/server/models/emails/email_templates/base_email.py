import json

from gembase_server_core.db.db_connection import DbConnection
from src.external_api.gmail import GbEmailService
from src.server.models.emails.emails_helper import EmailsHelper
from src.server.models.emails.email_templates.email_instance_data import EmailInstanceData
from src.server.models.user.user_data import UserData
from src.server.models.user.user_emails import UserEmails
from src.server.models.user.user_model import UserModel
from src.server.models.user.user_registration_helper import UserRegistrationHelper
from src.utils.gembase_utils import GembaseUtils


class BaseEmail:

    client_url_root = GembaseUtils.client_url_root()

    def __init__(
            self,
            conn: DbConnection,
            session_user: UserModel,
            draft_id: int | None = None,
            from_composer=False
    ):
        self.conn = conn
        self.session_user = session_user
        self.draft_id = draft_id
        self.email = None
        self.instance_data: EmailInstanceData = EmailInstanceData()
        self.load_template()
        self.from_composer=from_composer
        if self.draft_id is not None:
            self.load_draft()

    def load_template(self):
        row = self.conn.select_one_or_none("""
                SELECT subject, title, body, footer FROM app.def_email_templates WHERE template_def = %s
                """, [self.get_template_def()])

        self.instance_data = EmailInstanceData()

        if row is not None:
            self.instance_data.subject = row["subject"]

            footer = row["footer"]
            if footer is None or footer.strip() == "":
                footer = """
                    Best regards,<br>
                    Team Gembase.io
                    """

            self.instance_data.template_parameters["gb__homepage"] = UserEmails.get_homepage_url()
            self.instance_data.template_parameters["gb__title"] = row["title"]
            self.instance_data.template_parameters["gb__content"] = row["body"]
            self.instance_data.template_parameters["gb__footer"] = footer
            self.instance_data.from_address = self.get_from_address()
            self.instance_data.content_parameters = self.get_content_parameters()

    def delete_draft(self):
        if self.draft_id is not None:
            self.conn.query("""
                DELETE FROM app.users_email_draft WHERE id = %s
                """, [self.draft_id])
            self.load_template()

    def save_draft(self, instance_data):
        self.instance_data = EmailInstanceData.from_json(instance_data=instance_data)

        content_parameters_final = None
        if self.instance_data.content_parameters is not None:
            content_parameters_final = {}
            for k in self.instance_data.content_parameters:
                if not self.is_static_content_parameter(k):
                    content_parameters_final[k] = self.instance_data.content_parameters[k]

        content_parameters_final = None if content_parameters_final is None else json.dumps(content_parameters_final)
        draft_parameters = None if self.instance_data.draft_parameters is None else json.dumps(self.instance_data.draft_parameters)

        self.conn.query("""
            UPDATE app.users_email_draft
            SET template_parameters = %s, content_parameters = %s, t = NOW(), draft_parameters = %s,
            subject = %s, from_address = %s
            WHERE id = %s
            """, [json.dumps(self.instance_data.template_parameters), content_parameters_final, draft_parameters,
                  self.instance_data.subject, self.instance_data.from_address, self.draft_id])

    def get_creds(self):
        return None

    def modify_template_parameter_before_send(self, p: str, is_test_email=False) -> str:
        return self.instance_data.template_parameters[p]

    def modify_content_parameter_before_send(self, p: str, val: str, is_test_email=False) -> str:
        if p == "[MAIN_LINK_START]":
            is_registered = False
            if self.email is not None:
                user_id = UserData.get_user_id_from_email(conn=self.conn, email=self.email)
                if user_id != 0:
                    is_registered = True
            url = self.get_registration_url(is_test=is_test_email, is_registered=is_registered)
            return url

        return val

    def check_from_composer(self):
        return None

    def send(self, to_test_user=False, to_current_user=False):

        if self.from_composer:
            check = self.check_from_composer()
            if check is not None:
                return check

        email_logo_guid = GembaseUtils.get_guid()

        subject = self.instance_data.subject
        theme = UserEmails.THEMES[0]["theme"]
        if self.instance_data.draft_parameters is not None and "theme" in self.instance_data.draft_parameters:
            theme = self.instance_data.draft_parameters["theme"]
        wrapper = UserEmails.get_wrapper(theme=theme, include_email_unsubscribe=True, email_logo_guid=email_logo_guid)

        for k in self.instance_data.template_parameters:
            if not self.is_static_content_parameter(k):
                s = self.modify_template_parameter_before_send(k, is_test_email=to_test_user or to_current_user)
                wrapper = wrapper.replace(f"[{k}]", s)
                subject = subject.replace(f"[{k}]", s)

        unsubscribe_request_guid = UserData.get_or_create_unsubscribe_guid(
            conn=self.conn,
            email=self.email
        )

        wrapper = UserEmails.set_email_unsubscribe_guid(
            wrapper=wrapper,
            guid=unsubscribe_request_guid
        )

        wrapper = wrapper.replace("[HOMEPAGE]", UserEmails.get_homepage_url())

        for k in self.instance_data.content_parameters:
            if not self.is_static_content_parameter(k):
                val = self.instance_data.content_parameters[k]
                val = self.modify_content_parameter_before_send(p=k, val=val, is_test_email=to_test_user or to_current_user)
                wrapper = wrapper.replace(k, val)
                subject = subject.replace(k, val)

        to_address = self.email
        if to_test_user:
            to_address = "xxx@xxx.xxx"
        if to_current_user:
            to_address = self.session_user.get_email()

        GbEmailService.send_mail(
            from_address=self.instance_data.from_address,
            subject=subject,
            body=wrapper,
            is_html=True,
            to_address=[to_address],
            subject_creds=self.get_creds()
        )

        EmailsHelper.archive_email(
            conn=self.conn,
            guid=email_logo_guid,
            email_def=self.get_template_def(),
            data={
                "email": to_address,
                "subject": subject,
                "body": wrapper
            },
            from_composer=self.from_composer
        )

        return None

    def create_draft(self, email: str | None = None):

        if self.email is None and email is None:
            raise Exception(f"Missing email_id when creating draft")

        if email is not None and email != self.email:
            self.set_email(email=email)

        email_id = UserData.get_or_create_email_id(
            conn=self.conn,
            email=self.email
        )

        self.draft_id = self.conn.insert("""
        INSERT INTO app.users_email_draft (email_id, template_def, subject, from_address, template_parameters, 
        content_parameters)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, [email_id, self.get_template_def(), self.instance_data.subject,
              self.instance_data.from_address,
              json.dumps(self.instance_data.template_parameters),
              json.dumps(self.instance_data.content_parameters)])
        self.load_draft()

    def load_draft(self):
        row = self.conn.select_one_or_none(f"""
                SELECT u.id, u.email_id, u.template_parameters, u.content_parameters, u.draft_parameters, 
                UNIX_TIMESTAMP(u.t) as t, m.email, u.template_def, u.subject, u.from_address
                  FROM app.users_email_draft u,
                       app.map_user_email m
                 WHERE m.id = u.email_id
                   AND u.id = %s
                """, [self.draft_id])

        self.instance_data = EmailInstanceData()
        self.instance_data.subject = row["subject"]
        self.instance_data.from_address = row["from_address"]

        if row["template_parameters"] is not None:
            self.instance_data.template_parameters = json.loads(row["template_parameters"])
        if row["content_parameters"] is not None:
            self.instance_data.content_parameters = json.loads(row["content_parameters"])
            if "[HOMEPAGE]" in self.instance_data.content_parameters:
                del self.instance_data.content_parameters["[HOMEPAGE]"]
        if row["draft_parameters"] is not None:
            self.instance_data.draft_parameters = json.loads(row["draft_parameters"])

        email = UserData.get_email_from_id(
            conn=self.conn,
            email_id=row["email_id"]
        )

        if self.email != email:
            self.set_email(email=email)

        self.after_draft_loaded()

    def after_draft_loaded(self):
        pass

    def get_template_def(self):
        return ""

    def get_from_address(self):
        return None

    def get_content_parameters(self):
        return {}

    def get_static_content_parameters(self):
        return ["[HOMEPAGE]"]

    def is_static_content_parameter(self, p: str):
        return p in self.get_static_content_parameters()

    def set_email(self, email: str):
        self.email = email
        unsubscribe_request_guid = UserData.get_or_create_unsubscribe_guid(
            conn=self.conn,
            email=self.email
        )
        self.instance_data.content_parameters["[gb__email_unsubscribe_url]"] = UserEmails.get_email_unsubscribe_url(
            guid=unsubscribe_request_guid)

    def get_client_data(self, user: UserModel):
        instance_data = self.instance_data.get_client_data(user=user)
        for k in instance_data["template_parameters"]:
            if not self.is_static_content_parameter(k):
                instance_data["template_parameters"][k] = self.modify_template_parameter_before_send(k)
        for k in instance_data["content_parameters"]:
            if not self.is_static_content_parameter(k):
                val = instance_data["content_parameters"][k]
                instance_data["content_parameters"][k] = self.modify_content_parameter_before_send(
                    p=k, val=val, is_test_email=self.from_composer)

        return {
            "def_id": self.get_template_def(),
            "instance_data": instance_data,
            "draft_id": self.draft_id
        }

    def get_registration_url(self, is_test=False, is_registered=False) -> str:
        request_guid = UserRegistrationHelper.get_invite_request_guid(
            conn=self.conn,
            email=self.email
        )

        client_url_root = GembaseUtils.client_url_root()
        registration_url = f"{client_url_root}/confirm-registration-by-email/?request={request_guid}"

        if is_test:
            registration_url += "&test"
        if is_registered and self.email is not None:
            registration_url += f"&email={self.email}"

        return f"""<a href="{registration_url}" {UserEmails.MAIN_LINK_STYLE}>"""
