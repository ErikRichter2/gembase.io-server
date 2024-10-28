import os

from datetime import datetime

from src.external_api.gmail import GbEmailService
from src.server.models.apps.app_model import AppModel
from src.server.models.billing.billing_cron import BillingCron
from src.server.models.platform_values.platform_values_cron import PlatformValuesCron
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.utils.gembase_utils import GembaseUtils


class Cron:

    def __init__(self):
        self.conn = ServiceWrapperModel.create_conn()

    def run(self):

        try:
            self.__run()
        except Exception as err:
            print(err)
            ServiceWrapperModel.close_conn(conn_id=self.conn.connection_id(), conn=self.conn)
            raise err
        finally:
            ServiceWrapperModel.close_conn(conn_id=self.conn.connection_id(), conn=self.conn)

    def __run(self):

        ServiceWrapperModel.kill_without_heartbeat()
        ServiceWrapperModel.run(d=ServiceWrapperModel.SERVICE_GPT_TAGGER, t=True)
        ServiceWrapperModel.run(d=ServiceWrapperModel.SERVICE_SCRAPER)
        self.conn.commit()

        rows_cron = self.conn.select_all("""
        SELECT c.id, c.cron_name, c.state, c.priority, c.last_run
          FROM app.def_cron c
         WHERE c.state is NULL 
            OR c.state != 'err'
         ORDER BY c.priority
        """)

        for row in rows_cron:
            cron_name = row["cron_name"]
            last_run = row["last_run"]
            force = row["state"] == "force"
            if self.__can_run(cron_name=cron_name, last_run_datetime=last_run, force=force):
                try:
                    self.__run_cron_name(cron_name)
                except Exception as err:
                    self.conn.query("""
                    UPDATE app.def_cron
                       SET log = %s,
                           state = 'err',
                           last_run = NOW()
                     WHERE cron_name = %s
                    """, [str(err), cron_name])
                    self.conn.commit()
                    GbEmailService.send_system_alert(
                        subject=f"Gembase.io - CRON {cron_name} ERROR",
                        body=f"Cron: {cron_name}\n\nError: {err}"
                    )
                    raise err

    def __can_run(self, cron_name: str, last_run_datetime: datetime | None, force=False) -> bool:

        if force:
            return True

        current_datetime = datetime.fromtimestamp(GembaseUtils.timestamp_int())
        current_date = current_datetime.date()

        if cron_name == "h":
            if last_run_datetime is None:
                return True
            return (last_run_datetime.hour < current_datetime.hour and last_run_datetime.date() == current_date) or last_run_datetime.date() < current_date

        if cron_name == "d":
            if last_run_datetime is None:
                return True
            return last_run_datetime.date() < current_date

        if cron_name == "w":
            if current_date.weekday() == 0:
                if last_run_datetime is None:
                    return True
                return last_run_datetime.date() < current_date

        if cron_name == "ww":
            if current_date.day == 1 or current_date.day == 15:
                if last_run_datetime is None:
                    return True
                return last_run_datetime.date() < current_date

        if cron_name == "m":
            if current_date.day == 1:
                if last_run_datetime is None:
                    return True
                if last_run_datetime.date() < current_date:
                    return True

        return False

    def __run_cron_name(self, cron_name: str):
        self.conn.query("""
        UPDATE app.def_cron
          SET state = 'working'
        WHERE cron_name = %s
        """, [cron_name])
        self.conn.commit()

        if cron_name == "h":
            self.h()
        elif cron_name == "d":
            self.d()
        elif cron_name == "w":
            self.w()
        elif cron_name == "ww":
            self.ww()
        elif cron_name == "m":
            self.m()

        self.conn.query("""
        UPDATE app.def_cron
          SET state = 'done',
              last_run = NOW(),
              log = NULL
        WHERE cron_name = %s
        """, [cron_name])
        self.conn.commit()

    def h(self):
        # check disk free size
        statvfs = os.statvfs('/')
        free_size = statvfs.f_frsize * statvfs.f_bavail
        if free_size <= 5 * 1000 * 1000 * 1000:
            GbEmailService.send_system_alert(
                subject=f"Gembase.io - LOW AVAILABLE DISK SPACE",
                body=f"Available: {free_size}"
            )

        self.conn.query("""
        TRUNCATE TABLE app_temp_data.users_commands_calls
        """)
        self.conn.commit()

        BillingCron.process(conn=self.conn)

        self.conn.commit()

    def d(self):
        self.cleanup_db()
        self.update_loyalty_installs_where_null()
        PlatformValuesCron.d(conn=self.conn)

    def w(self):
        pass

    def ww(self):
        pass

    def m(self):
        AppModel.update_loyalty_installs_bulk(conn=self.conn)
        self.conn.commit()

    def update_loyalty_installs_where_null(self):
        conn = self.conn
        AppModel.update_loyalty_installs_bulk(conn=conn, only_where_null=True)
        conn.commit()

    def cleanup_db(self):
        conn = self.conn

        conn.query("""
            DELETE FROM app_temp_data.users_login_tokens
             WHERE DATE_ADD(created, INTERVAL expire_days DAY) < NOW() 
               AND expire_days > 0
            """)
        conn.commit()

        conn.query("""
            DELETE FROM archive.archive_data d
             WHERE DATE_ADD(d.t, INTERVAL d.expire_days DAY) < NOW()
               AND d.expire_days > 0
            """)
        conn.commit()

        conn.query("""
            DELETE FROM app_temp_data.server_services d
             WHERE DATE_ADD(d.heartbeat, INTERVAL 2 DAY) < NOW()
               AND d.finished = 1
            """)
        conn.commit()

        conn.query("""
        PURGE BINARY LOGS BEFORE DATE_SUB(NOW(), INTERVAL 3 DAY)
        """)
        conn.commit()


def default_method(*args, **kwargs):
    Cron().run()
