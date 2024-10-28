from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.external.google.google_error_reporting import GoogleErrorReporting
from gembase_server_core.private_data.private_data_model import PrivateDataModel


class LogsModel:

    @staticmethod
    def init():
        GoogleErrorReporting.set_credentials(
            PrivateDataModel.get_private_data()["google"]["service_account"]
        )

    @staticmethod
    def debug_log(
            title: str,
            log: str = "log"
    ):
        conn = DbConnection()

        try:
            conn.query("""
                    INSERT INTO logs.logs (user_id, title, stacktrace, type, platform)
                    VALUES (%s, %s, %s, 'debug', 'server')
                    """, [0, title, log])
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()

    @staticmethod
    def server_error_log(
            user_id: int,
            title: str,
            stacktrace: str,
            exception=False
    ):
        GoogleErrorReporting.log(
            message=f"[SERVER_ERROR] {title}",
            user_id=user_id,
            exception=exception
        )

        conn = DbConnection()

        try:
            conn.query("""
            INSERT INTO logs.logs (user_id, title, stacktrace, type, platform)
            VALUES (%s, %s, %s, 'error', 'server')
            """, [user_id, title, stacktrace])
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()

    @staticmethod
    def client_error_log(
            user_id: int,
            message: str
    ):
        GoogleErrorReporting.log(
            message=f"[CLIENT_ERROR] {message}",
            user_id=user_id
        )

        # conn = DbConnection()
        #
        # try:
        #     conn.query("""
        #         INSERT INTO logs.logs (user_id, title, stacktrace, type, platform)
        #         VALUES (%s, %s, %s, 'error', 'client')
        #         """, [user_id, title, stacktrace])
        #     conn.commit()
        # except Exception:
        #     pass
        # finally:
        #     conn.close()
