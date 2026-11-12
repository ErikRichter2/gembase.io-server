import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback

from server_core.logs.logs_db_model import LogsDbModel
from src.env_data import APP_ENV_PROD, ENV_DATA

SMTP_SERVER = "mail.aialgo.sk"
SMTP_PORT = 465
SMTP_USER: str | None = ENV_DATA.email_sender_address
SMTP_PASSWORD = ENV_DATA.email_sender_password

DEFAULT_FROM = 'noreply@aialgo.sk'

class EmailSender:

    def send(
        self, 
        recipient: str, 
        subject: str, 
        body: str,
        is_html=False,
        cc_admin=False,
        locked=False
    ):
        if SMTP_USER is None or SMTP_PASSWORD is None:
            if ENV_DATA.app_env == APP_ENV_PROD:
                raise Exception('SMTP config not set !')
            return
        
        if ENV_DATA.app_env != APP_ENV_PROD:
            recipient = 'tech@aialgo.sk'
        
        msg = MIMEMultipart()
        msg["From"] = DEFAULT_FROM
        msg["To"] = recipient
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        
        try:
            server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT)
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(msg["From"], msg["To"], msg.as_string())
            server.quit()
            return {
                'from': msg["From"],
                'to': msg["To"],
                'subject': msg["Subject"],
                'body': msg.as_string()
            }
        except Exception as e:
            LogsDbModel.server_error_log(
                user_id=0,
                title=f'EmailSender: {str(e)}',
                stacktrace=traceback.format_exc()
            )
            print(f"Error: {e}")
            return None
