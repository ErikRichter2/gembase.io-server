from gembase_server_core.db.db_connection import DbConnection
from src.server.models.logs.logs_model import LogsModel
from src.utils.gembase_utils import GembaseUtils


class EmailsHelper:

    @staticmethod
    def archive_email(
            conn: DbConnection,
            data, guid: str | None = None,
            email_def: str | None = None,
            from_composer=False
    ):
        try:
            if guid is None:
                guid = GembaseUtils.get_guid()
            conn.query("""
                INSERT INTO archive.users_sent_emails (guid, email, subject, body, email_def, from_composer) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """, [guid, data["email"], data["subject"], data["body"], email_def, from_composer])
        except Exception:
            LogsModel.server_error_log(
                user_id=0,
                title="archive_email",
                stacktrace="",
                exception=True
            )
