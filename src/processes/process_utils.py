import os
import signal
from typing import Optional

from server_core.db.db_connection import DbConnection


class ProcessUtils:

    @staticmethod
    def process_running_instances(conn: DbConnection, process_name: str) -> int:
        return conn.select_one("""
            SELECT count(1) as cnt 
              FROM gb_server_processes.processes
             WHERE state != 'finished'
               AND heartbeat_t >= DATE_SUB(NOW(), INTERVAL 15 SECOND)
               AND process_name = %s
            """, [process_name])["cnt"]

    @staticmethod
    def kill(pid: Optional[int] = None, error_msg: Optional[str] = None):

        if pid is None:
            pid = os.getpid()

        conn = DbConnection()

        if error_msg is None:
            error_msg = "kill"

        conn.query("""
        UPDATE gb_server_processes.processes
           SET state = 'finished',
               is_error = 1,
               error_msg = %s
         WHERE pid = %s
        """, [error_msg, pid])
        conn.commit()
        conn.close()

        try:
            if os.name == 'nt':
                os.kill(pid, signal.SIGTERM)
            else:
                os.kill(pid, signal.SIGKILL)
        except Exception as err:
            pass
