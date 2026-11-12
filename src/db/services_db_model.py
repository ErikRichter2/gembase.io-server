from multiprocessing import shared_memory
import os
import signal
import subprocess

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel
from src.env_data import ENV_DATA
from src.processes.process_args import ProcessArgs


class ServicesDbModel(BaseDbModel):    

    SERVICE_SCRAPER = 1
    SERVICE_TRENDING_GAMES_SCRAPER = 2
    SERVICE_GPT_TAGGER = 3
    SERVICE_PORTAL_LANDING = 4
    SERVICE_PLATFORM_VALUES_SINGLE_APP = 5
    
    def __init__(self, conn: DbConnection, demo=False):
        super(ServicesDbModel, self).__init__(conn=conn)
        self.demo = demo
        
    def run(
        self,
        process_args: ProcessArgs
    ):

        command_line_args = process_args.to_command_line_args()
        root = Utils.get_project_root_path()

        if os.name == 'nt':
            stdout = None
            stderr = None
            preexec_fn = None
            py_path = os.path.join(root, ".venv", "Scripts", "python.exe")
            script_path = os.path.join(root, "run_process.py")
            data = [py_path, script_path] + command_line_args
        else:
            stdout = open('/dev/null', 'w')
            stderr = open('logfile.log', 'a')
            preexec_fn = os.setpgrp
            data = ["nohup", "bash", os.path.join(root, "run_process.sh")] + command_line_args

        print(f"[debug] service: subprocess open {' '.join(data)}")
        subprocess.Popen(
            data,
            stdout=stdout,
            stderr=stderr,
            preexec_fn=preexec_fn)
    
    @staticmethod
    def create_conn() -> DbConnection:
        conn = DbConnection()
        conn.query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        wrapper_conn = DbConnection()
        wrapper_conn.query("""
        INSERT INTO gb_app_temp_data.server_services_conn (pid, conn_id)
        VALUES (%s, %s)
        """, [os.getpid(), conn.connection_id()])
        wrapper_conn.commit()
        wrapper_conn.close()
        return conn

    @staticmethod
    def close_conn(conn_id: int, conn: DbConnection | None = None):
        wrapper_conn = DbConnection()
        wrapper_conn.query("""
        DELETE FROM gb_app_temp_data.server_services_conn
         WHERE conn_id = %s
        """, [conn_id])
        wrapper_conn.commit()

        if conn is not None:
            conn.close()
        else:
            wrapper_conn.query_safe("KILL %s", [conn_id])

        wrapper_conn.close()

    def close_all_conns(self, pid: int | None = None, wrapper_conn: DbConnection | None = None):
        if pid is None:
            pid = os.getpid()

        close_conn = False
        if wrapper_conn is None:
            wrapper_conn = DbConnection()
            close_conn = True

        rows = wrapper_conn.select_all("""
        SELECT conn_id
          FROM gb_app_temp_data.server_services_conn
         WHERE pid = %s
        """, [pid])
        for row in rows:
            wrapper_conn.query_safe("KILL %s", [row["conn_id"]])
        wrapper_conn.query("""
        DELETE FROM gb_app_temp_data.server_services_conn
         WHERE pid = %s
        """, [pid])
        wrapper_conn.commit()

        if close_conn:
            wrapper_conn.close()

    def kill(self, pid: int | None = None, wrapper_conn: DbConnection | None = None):

        if self.demo:
            return

        if pid is None:
            pid = os.getpid()

        close_conn = False
        if wrapper_conn is None:
            wrapper_conn = DbConnection()
            close_conn = True

        self.close_all_conns(pid=pid, wrapper_conn=wrapper_conn)

        wrapper_conn.query("""
        UPDATE gb_app_temp_data.server_services
           SET finished = 1,
               error = 1,
               status = 'killed'
         WHERE pid = %s
        """, [pid])

        wrapper_conn.commit()

        if close_conn:
            wrapper_conn.close()

        try:
            if os.name == 'nt':
                os.kill(pid, signal.SIGTERM)
            else:
                os.kill(pid, signal.SIGKILL)
        except Exception as err:
            pass

    def get_heartbeat(self, pid: int | None = None):
        if pid is None:
            pid = os.getpid()

        conn = DbConnection()
        row_hb = conn.select_one_or_none("""
        SELECT s.heartbeat
          FROM gb_app_temp_data.server_services s
         WHERE s.pid = %s
        """, [pid])
        conn.close()

        heartbeat = None
        if row_hb is not None:
            heartbeat = row_hb["heartbeat"]
        return heartbeat

    def kill_without_heartbeat(self):
        if self.demo:
            return
        
        wrapper_conn = DbConnection()

        rows = wrapper_conn.select_all("""
        SELECT s.id, s.pid
          FROM gb_app_temp_data.server_services s
         WHERE DATE_ADD(s.heartbeat, INTERVAL 1 MINUTE) <= NOW()
           AND s.finished = 0
        """)

        for row in rows:
            self.kill(row["pid"], wrapper_conn=wrapper_conn)

        wrapper_conn.close()

    def is_running(self, d: int | None = None, s: str | None = None):
        if d is not None:
            return self.conn.select_one("""
            SELECT count(1) as cnt 
              FROM gb_app_temp_data.server_services 
            WHERE def_id = %s 
              AND finished = 0
              AND DATE_ADD(heartbeat, INTERVAL 1 MINUTE) > NOW()
            """, [d])["cnt"] > 0
        else:
            return self.conn.select_one("""
            SELECT count(1) as cnt 
              FROM gb_app_temp_data.server_services 
            WHERE script = %s 
              AND finished = 0
              AND DATE_ADD(heartbeat, INTERVAL 1 MINUTE) > NOW()
            """, [s])["cnt"] > 0

    def kill_by_def_id(self, def_id: int):
        if self.demo:
            return
        
        conn = DbConnection()

        rows = conn.select_all("""
        SELECT s.pid
          FROM gb_app_temp_data.server_services s
         WHERE s.def_id = %s
           AND finished = 0
        """, [def_id])

        for row in rows:
            self.kill(row["pid"], wrapper_conn=conn)

        conn.close()

    def update_service_shared_mem(self, mem_id: str):
        if self.demo:
            return
        
        try:
            shm = shared_memory.SharedMemory(
                name=mem_id
            )
            buffer = shm.buf
            if buffer is not None:
                buffer[0] = 1
        except Exception as err:
            pass

