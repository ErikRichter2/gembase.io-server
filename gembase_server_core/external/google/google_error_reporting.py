from google.cloud import error_reporting
from google.oauth2 import service_account

from gembase_server_core.environment.runtime_constants import rr


class GoogleErrorReporting:

    __credentials = None

    @staticmethod
    def set_credentials(service_account_json):
        GoogleErrorReporting.__credentials = service_account.Credentials.from_service_account_info(service_account_json)

    @staticmethod
    def log(message: str, user_id: int, exception=False):
        if not rr.is_prod():
            return

        try:
            client = error_reporting.Client(credentials=GoogleErrorReporting.__credentials)
            if exception:
                client.report_exception(user=str(user_id))
            else:
                client.report(message=message, user=str(user_id))
        except Exception:
            pass
