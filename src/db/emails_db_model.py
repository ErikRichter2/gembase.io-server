from server_core.db.db_connection import DbConnection
from server_core.logs.logs_db_model import LogsDbModel
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel


class EmailsDbModel(BaseDbModel):
    
    def __init__(self, conn: DbConnection):
        super(EmailsDbModel, self).__init__(conn=conn)
        
    def archive_email(self, data, guid: str | None = None, email_def: str | None = None):
        try:
            if guid is None:
                guid = Utils.get_guid()
            self.conn().query("""
                INSERT INTO archive.users_sent_emails (guid, email, subject, body, email_def) 
                VALUES (%s, %s, %s, %s, %s)
                """, [guid, data["email"], data["subject"], data["body"], email_def])
        except Exception:
            LogsDbModel.server_error_log(
                user_id=0,
                title="archive_email",
                stacktrace="",
                exception=True
            )
