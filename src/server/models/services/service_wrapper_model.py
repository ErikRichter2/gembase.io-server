import json
import os
import signal
import subprocess

from gembase_server_core.db.db_connection import DbConnection
from multiprocessing import shared_memory

from gembase_server_core.environment.runtime_constants import rr
from gembase_server_core.utils.gb_utils import GbUtils


class ServiceWrapperModel:

    SERVICE_SCRAPER = 1
    SERVICE_TRENDING_GAMES_SCRAPER = 2
    SERVICE_GPT_TAGGER = 3
    SERVICE_PORTAL_LANDING = 4
    SERVICE_PLATFORM_VALUES_CALC = 5

    @staticmethod
    def create_conn() -> DbConnection:
        conn = DbConnection()
        conn.set_read_uncommitted()
        wrapper_conn = DbConnection()
        wrapper_conn.query("""
        INSERT INTO app_temp_data.server_services_conn (pid, conn_id)
        VALUES (%s, %s)
        """, [os.getpid(), conn.connection_id()])
        wrapper_conn.commit()
        wrapper_conn.close()
        return conn

    @staticmethod
    def close_conn(conn_id: int, conn: DbConnection | None = None):
        wrapper_conn = DbConnection()
        wrapper_conn.query("""
        DELETE FROM app_temp_data.server_services_conn
         WHERE conn_id = %s
        """, [conn_id])
        wrapper_conn.commit()

        if conn is not None:
            conn.close()
        else:
            wrapper_conn.query_safe("KILL %s", [conn_id])

        wrapper_conn.close()

    @staticmethod
    def close_all_conns(pid: int | None = None, wrapper_conn: DbConnection | None = None):
        if pid is None:
            pid = os.getpid()

        close_conn = False
        if wrapper_conn is None:
            wrapper_conn = DbConnection()
            close_conn = True

        rows = wrapper_conn.select_all("""
        SELECT conn_id
          FROM app_temp_data.server_services_conn
         WHERE pid = %s
        """, [pid])
        for row in rows:
            wrapper_conn.query_safe("KILL %s", [row["conn_id"]])
        wrapper_conn.query("""
        DELETE FROM app_temp_data.server_services_conn
         WHERE pid = %s
        """, [pid])
        wrapper_conn.commit()

        if close_conn:
            wrapper_conn.close()

    @staticmethod
    def kill(pid: int | None = None, wrapper_conn: DbConnection | None = None):

        if pid is None:
            pid = os.getpid()

        close_conn = False
        if wrapper_conn is None:
            wrapper_conn = DbConnection()
            close_conn = True

        ServiceWrapperModel.close_all_conns(pid=pid, wrapper_conn=wrapper_conn)

        wrapper_conn.query("""
        UPDATE app_temp_data.server_services
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

    @staticmethod
    def get_heartbeat(pid: int | None = None):
        if pid is None:
            pid = os.getpid()

        conn = DbConnection()
        row_hb = conn.select_one_or_none("""
        SELECT s.heartbeat
          FROM app_temp_data.server_services s
         WHERE s.pid = %s
        """, [pid])
        conn.close()

        heartbeat = None
        if row_hb is not None:
            heartbeat = row_hb["heartbeat"]
        return heartbeat

    @staticmethod
    def kill_without_heartbeat():
        wrapper_conn = DbConnection()

        rows = wrapper_conn.select_all("""
        SELECT s.id, s.pid
          FROM app_temp_data.server_services s
         WHERE DATE_ADD(s.heartbeat, INTERVAL 1 MINUTE) <= NOW()
           AND s.finished = 0
        """)

        for row in rows:
            ServiceWrapperModel.kill(row["pid"], wrapper_conn=wrapper_conn)

        wrapper_conn.close()

    @staticmethod
    def run(
        u: int | None = None,
        g: str | None = None,
        d: int | None = None,
        s: str | None = None,
        e: str | None = None,
        m: str | None = None,
        c: str | None = None,
        a=False,
        x=False,
        z=False,
        t=False
    ):
        """
        :param u: user id
        :param g: guid (auto generated if None)
        :param d: service def id
        :param s: service script path
        :param e: environment (dev, prod)
        :param m: method to run (default is default_method())
        :param c: custom data
        :param a: do not delete service record from db after successful run
        :param x: run service even if service with same id / script is already running
        :param b: set debug env
        :param z: update shared memory if already running
        :param t: check timeout
        :return: nothing
        """
        params = []

        if u is not None:
            params.append("-u")
            params.append(str(u))
        if g is not None:
            params.append("-g")
            params.append(g)
        if d is not None:
            params.append("-d")
            params.append(str(d))
        if s is not None:
            params.append("-s")
            params.append(s)
        if e is not None:
            params.append("-e")
            params.append(e)
        if m is not None:
            params.append("-m")
            params.append(m)
        if c is not None:
            params.append("-c")
            params.append(c)
        if a:
            params.append("-a")
        if x:
            params.append("-x")
        if z:
            params.append("-z")
        if rr.is_debug():
            params.append("-b")
        if t:
            params.append("-t")

        root = GbUtils.get_project_root_path()

        if os.name == 'nt':
            stdout = None
            stderr = None
            preexec_fn = None
            data = [os.path.join(root, "conda", "python.exe"), os.path.join(root, "service_wrapper.py")] + params
        else:
            stdout = open('/dev/null', 'w')
            stderr = open('logfile.log', 'a')
            preexec_fn = os.setpgrp
            data = ["nohup", "bash", os.path.join(root, "service_wrapper.sh")] + params

        print(f"[debug] service: subprocess open {' '.join(data)}")
        subprocess.Popen(
            data,
            stdout=stdout,
            stderr=stderr,
            preexec_fn=preexec_fn)

    @staticmethod
    def is_running(conn: DbConnection, d: int | None = None, s: str | None = None):
        if d is not None:
            return conn.select_one("""
            SELECT count(1) as cnt 
              FROM app_temp_data.server_services 
            WHERE def_id = %s 
              AND finished = 0
              AND DATE_ADD(heartbeat, INTERVAL 1 MINUTE) > NOW()
            """, [d])["cnt"] > 0
        else:
            return conn.select_one("""
            SELECT count(1) as cnt 
              FROM app_temp_data.server_services 
            WHERE script = %s 
              AND finished = 0
              AND DATE_ADD(heartbeat, INTERVAL 1 MINUTE) > NOW()
            """, [s])["cnt"] > 0

    @staticmethod
    def kill_by_def_id(def_id: int):
        conn = DbConnection()

        rows = conn.select_all("""
        SELECT s.pid
          FROM app_temp_data.server_services s
         WHERE s.def_id = %s
           AND finished = 0
        """, [def_id])

        for row in rows:
            ServiceWrapperModel.kill(row["pid"], wrapper_conn=conn)

        conn.close()

    @staticmethod
    def update_service_shared_mem(mem_id: str):
        try:
            shm = shared_memory.SharedMemory(
                name=mem_id
            )
            buffer = shm.buf
            if buffer is not None:
                buffer[0] = 1
        except Exception as err:
            pass

    @staticmethod
    def set_service_data(pid: int, data):
        conn = DbConnection()

        conn.query("""
        UPDATE app_temp_data.server_services
        SET service_data = %s
        WHERE pid = %s
        """, [json.dumps(data), pid])
        conn.commit()

        conn.close()
