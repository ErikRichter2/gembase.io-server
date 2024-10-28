from src.server.models.emails.email_templates.base_email import BaseEmail
from src.server.models.emails.email_templates.follow_up_email import FollowUpEmail
from src.server.models.emails.email_templates.invite_by_user_email import InviteByUserEmail
from src.session.session import gb_session


class EmailFactory:

    @staticmethod
    def load_draft(draft_id: int, from_composer=False):
        row = gb_session().conn().select_one_or_none("""
            SELECT template_def FROM app.users_email_draft where id = %s
            """, [draft_id])
        if row is not None:
            return EmailFactory.create(template_def=row["template_def"], draft_id=draft_id, from_composer=from_composer)
        return None

    @staticmethod
    def load_templates():
        rows = gb_session().conn().select_all("""
        SELECT template_def FROM app.def_email_templates
        """)
        res = []
        for row in rows:
            res.append(EmailFactory.create(template_def=row["template_def"]))
        return res

    @staticmethod
    def create(template_def: str, draft_id: int | None = None, from_composer=False) -> BaseEmail | None:
        if template_def == "follow_up":
            return FollowUpEmail(conn=gb_session().conn(), draft_id=draft_id, session_user=gb_session().user(), from_composer=from_composer)
        if template_def == "invite_by_user":
            return InviteByUserEmail(conn=gb_session().conn(), draft_id=draft_id, session_user=gb_session().user(), from_composer=from_composer)
        return None
