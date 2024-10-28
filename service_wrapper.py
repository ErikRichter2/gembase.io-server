import os
import sys
import time
import traceback
import uuid
from os.path import dirname
from pathlib import Path
from sys import path
from threading import Thread

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.environment.runtime_constants import RuntimeConstants, rr
from gembase_server_core.utils.gb_utils import GbUtils
from src.server.models.logs.logs_model import LogsModel
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.services.service_data import ServiceData
from src.utils.gembase_utils import GembaseUtils

services_path = os.path.join(GbUtils.get_project_root_path(), 'src', 'services')


def kill_current_process_if_not_exists(conn: DbConnection):
    pid = os.getpid()
    try:
        row = conn.select_one_or_none("""
        SELECT s.id, s.force_stop, s.finished
          FROM app_temp_data.server_services  s
         WHERE s.pid = %s
        """, [pid])
        if row is None:
            conn.close()
            ServiceWrapperModel.kill(pid=pid)
            return True
        if row["force_stop"] == 1:
            conn.query("""
            UPDATE app_temp_data.server_services 
               SET finished = 1, 
                   error = 1, 
                   error_message = 'force stop'
            WHERE pid = %s
            """, [pid])
            conn.commit()
            conn.close()
            ServiceWrapperModel.kill(pid=pid)
            return True
        if row['finished'] == 1:
            return True
    except Exception as err:
        conn.close()
        DbConnection.s_query("""
        UPDATE app_temp_data.server_services s
           SET s.error_message = %s
         WHERE s.pid = %s
        """, [str(err), pid])
        ServiceWrapperModel.kill(pid=pid)
        return True

    return False


def update_service_heartbeat(sid: int, check_timeout=False):
    heartbeat_conn = DbConnection()
    while True:
        if kill_current_process_if_not_exists(heartbeat_conn):
            break

        if check_timeout:
            row = heartbeat_conn.select_one_or_none("""
            SELECT UNIX_TIMESTAMP(heartbeat_child) as heartbeat_child
              FROM app_temp_data.server_services 
              WHERE id = %s
            """, [sid])
            if row is not None and row["heartbeat_child"] is not None:
                t = GembaseUtils.timestamp()
                diff = t - row["heartbeat_child"]
                if diff > 5 * 60:
                    heartbeat_conn.close()
                    ServiceWrapperModel.kill(pid=os.getpid())

        heartbeat_conn.query("""
        UPDATE app_temp_data.server_services 
           SET heartbeat = NOW() 
         WHERE id = %s
        """, [sid])
        heartbeat_conn.commit()
        time.sleep(5)


def process_script(
    service_data: ServiceData,
    user_id: int,
    s: str | None = None,
    m: str | None = None,
    c: str | None = None,
    a=False
):
    script_conn = None
    try:
        print(f"Script START: {s}")
        exec(open(s).read(), globals())
        if m is not None:
            exec(m, globals())
        else:
            globals()['service_data'] = service_data
            exec(f"default_method(service_data=service_data, sid={service_data.service_id}, input_data={c})", globals())
        script_conn = DbConnection()
        if not a:
            script_conn.query("""
            DELETE FROM app_temp_data.server_services 
             WHERE id = %s
            """, [service_data.service_id])
        else:
            script_conn.query("""
            UPDATE app_temp_data.server_services 
               SET finished = 1 
             WHERE id = %s
            """, [service_data.service_id])
        script_conn.commit(True)
    except Exception as e:
        if script_conn is not None:
            script_conn.close()
        DbConnection.s_query("""
        UPDATE app_temp_data.server_services 
           SET finished = 1, 
               error = 1, 
               error_message = %s 
         WHERE id = %s
        """, [str(e), service_data.service_id])
        LogsModel.server_error_log(
            user_id=user_id,
            title=str(e),
            stacktrace=traceback.format_exc()
        )


def start(
    u: int | None = None,
    g: str | None = None,
    d: int | None = None,
    s: str | None = None,
    e: str | None = None,
    m: str | None = None,
    c: str | None = None,
    a=False,
    x=False,
    b=False,
    z=False,
    t=False
):
    pid = os.getpid()

    conn = DbConnection()
    conn.query("LOCK TABLE app_temp_data.server_services WRITE, app.def_server_services READ")

    if not x:
        if ServiceWrapperModel.is_running(conn=conn, d=d, s=s):
            print(f"Service {d}/{s} already running !")

            if z:
                ServiceWrapperModel.update_service_shared_mem(mem_id=str(pid))
                print(f"Service {d}/{s} memory updated !")

            conn.unlock_tables(commit=False)
            conn.close()
            return

    if g is None:
        g = str(str(uuid.uuid4()))

    user_id = 0
    if u is not None:
        user_id = u

    if e is not None:
        rr.ENV = e

    if b:
        RuntimeConstants.IS_DEBUG = True

    if d is not None:
        def_row = conn.select_one("""
        SELECT command
          FROM app.def_server_services
         WHERE id = %s
        """, [d])
        s = os.path.join(services_path, def_row["command"])
        sid = conn.insert("""
        INSERT INTO app_temp_data.server_services(pid, def_id, script, user_id, status, guid) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        """, [pid, d, s, user_id, "working", g])
    elif s is not None:
        sid = conn.insert("""
        INSERT INTO app_temp_data.server_services(pid, def_id, script, user_id, status, guid) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        """, [pid, 0, s, user_id, "working", g])
    else:
        raise Exception(f"Unknown arguments config")

    log = f"Service {sid} -> (u:{user_id}, g:{g}, d:{d}, s:{s}, e:{e}, m:{m}, c:{c}, a:{a}, x:{x})"

    conn.query("""
    UPDATE app_temp_data.server_services
       SET payload = %s
     WHERE id = %s
    """, [log, sid])

    conn.unlock_tables()
    conn.close()

    print(log)

    service_data = ServiceData(sid, g)

    threads = [
        Thread(target=process_script, args=(service_data, user_id, s, m, c, a)),
        Thread(target=update_service_heartbeat, args=(sid, t))
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    ServiceWrapperModel.close_all_conns()

    print(f"Service {sid} FINISHED")

    sys.exit(0)


if __name__ == '__main__':
    path.insert(0, dirname(__file__))
    root_path = str(Path(__file__).absolute())
    path.append(root_path)
    path.append(services_path)

    kwargs = {}

    for i in range(len(sys.argv)):
        if sys.argv[i] == "-u":
            kwargs['u'] = int(sys.argv[i + 1])
        if sys.argv[i] == "-g":
            kwargs['g'] = sys.argv[i + 1]
        if sys.argv[i] == "-d":
            kwargs['d'] = int(sys.argv[i + 1])
        if sys.argv[i] == "-s":
            kwargs['s'] = sys.argv[i + 1]
        if sys.argv[i] == "-e":
            kwargs['e'] = sys.argv[i + 1]
        if sys.argv[i] == "-m":
            kwargs['m'] = sys.argv[i + 1]
        if sys.argv[i] == "-c":
            kwargs['c'] = sys.argv[i + 1]
        if sys.argv[i] == "-x":
            kwargs['x'] = True
        if sys.argv[i] == "-a":
            kwargs['a'] = True
        if sys.argv[i] == "-b":
            kwargs['b'] = True
        if sys.argv[i] == "-z":
            kwargs['z'] = True
        if sys.argv[i] == "-t":
            kwargs['t'] = True

    start(**kwargs)
