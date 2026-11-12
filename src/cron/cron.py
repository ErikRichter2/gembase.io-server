from datetime import datetime

from server_core.db.db_connection import DbConnection

class Cron:

    def __init__(self):
        self.conn = DbConnection()

    def run(self):

        try:
            self.__run()
        except Exception as err:
            print(err)
            self.conn.close()
            raise err
        finally:
            self.conn.close()

    def __run(self):
        pass

    def __can_run(self, cron_name: str, last_run_datetime: datetime | None, force=False) -> bool:
        return False

def default_method(*args, **kwargs):
    Cron().run()
