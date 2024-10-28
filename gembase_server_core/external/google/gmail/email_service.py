import base64

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from email.mime.text import MIMEText

from gembase_server_core.external.google.gmail.email_data import EmailData

SCOPES = ['https://mail.google.com/']


class EmailService:

    def __init__(self, service_account_credentials: dict, default_subject_credentials: str):
        self.__service_account_credentials = service_account_credentials
        self.__default_subject_credentials = default_subject_credentials

    def send(self, email_data: EmailData, subject_credentials: str | None = None):
        creds = service_account.Credentials.from_service_account_info(
            self.__service_account_credentials
        )
        creds = creds.with_scopes(SCOPES)

        if subject_credentials is not None:
            creds = creds.with_subject(subject_credentials)
        else:
            creds = creds.with_subject(self.__default_subject_credentials)

        try:
            service = build('gmail', 'v1', credentials=creds)

            if email_data.is_html:
                message = MIMEText(email_data.body, "html")
            else:
                message = MIMEText(email_data.body)

            message['From'] = email_data.fromAddress
            message['Subject'] = email_data.subject
            message['To'] = email_data.toAddress

            # encoded message
            encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

            create_message = {
                'raw': encoded_message
            }
            # pylint: disable=E1101
            send_message = (service.users().messages().send
                            (userId="me",
                             body=create_message).execute())
            print(F'Message Id: {send_message["id"]}')
        except HttpError as error:
            print(F'An error occurred: {error}')
            send_message = None
        return send_message
