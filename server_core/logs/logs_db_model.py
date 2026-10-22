from server_core.db.db_connection import DbConnection


class LogsDbModel:
    
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
        pass
