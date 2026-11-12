import json
import os
import sys
import time
import traceback
from os.path import dirname
from pathlib import Path
from sys import path
from threading import Thread

import psutil

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from server_core.logs.logs_db_model import LogsDbModel
from src.processes.process_args import ProcessArgs
from src.processes.process_utils import ProcessUtils


def kill_current_process_if_not_exists(conn: DbConnection):
    pid = os.getpid()
    try:
        row = conn.select_one_or_none("""
        SELECT state, force_kill
          FROM gb_server_processes.processes
         WHERE pid = %s
        """, [pid])
        if row is None:
            conn.close()
            ProcessUtils.kill()
            return True
        if row["force_kill"] == 1:
            conn.commit()
            conn.close()
            ProcessUtils.kill(error_msg="force_kill")
            return True
        if row["state"] == "finished":
            return True
    except Exception as err:
        conn.close()
        LogsDbModel.server_error_log(
            user_id=0,
            title=str(err),
            stacktrace=traceback.format_exc(),
            exception=True
        )
        ProcessUtils.kill(error_msg=str(err))
        return True

    return False


def heartbeat(process_data):
    pid = os.getpid()
    heartbeat_conn = DbConnection()

    while True:
        if "process_stopped" in process_data:
            heartbeat_conn.close()
            break

        if kill_current_process_if_not_exists(heartbeat_conn):
            heartbeat_conn.close()
            break

        free_size = 0
        
        try:
            statvfs = os.statvfs('/')
            free_size = statvfs.f_frsize * statvfs.f_bavail
        except Exception:
            pass
        
        hdd = ""
        if free_size >= 1024:
            free_size = round(free_size / 1024, 2)
            hdd = f"{free_size}kB"
        if free_size >= 1024:
            free_size = round(free_size / 1024, 2)
            hdd = f"{free_size}MB"
        if free_size >= 1024:
            free_size = round(free_size / 1024, 2)
            hdd = f"{free_size}GB"

        os_info = {
            "cpu": psutil.cpu_percent(),
            "hdd": hdd,
            "mem": round(psutil.virtual_memory().available * 100 / psutil.virtual_memory().total)
        }

        heartbeat_conn.query("""
        UPDATE gb_server_processes.processes
           SET heartbeat_t = NOW(),
               os_info = %s
         WHERE pid = %s
        """, [json.dumps(os_info), pid])
        heartbeat_conn.commit()
        time.sleep(5)


def process_script(process_args: ProcessArgs):
    script_conn = DbConnection()
    s = os.path.join(Utils.get_project_root_path(), process_args.get_script(conn=script_conn))
    script_conn.close()
    pid = os.getpid()
    try:
        content = open(s).read()
        content += '\n' + 'process_main(process_args)'
        exec(content, {'process_args': process_args})
        script_conn = DbConnection()
        if process_args.keep_record:
            script_conn.query("""
            UPDATE gb_server_processes.processes
            SET state = 'finished'
            WHERE pid = %s
            """, [pid])        
        else:
            script_conn.query("""
            DELETE FROM gb_server_processes.processes
            WHERE pid = %s
            """, [pid])        
        script_conn.commit(True)
    except Exception as e:
        if script_conn is not None:
            script_conn.close()
        DbConnection.s_query("""
        UPDATE gb_server_processes.processes
           SET state = 'finished',
               is_error = 1,
               error_msg = %s
         WHERE pid = %s
        """, [str(e), pid])
        LogsDbModel.server_error_log(
            user_id=0,
            title=f'{process_args.get_name()}: {str(e)}',
            stacktrace=traceback.format_exc(),
            exception=True
        )


def start(process_args: ProcessArgs):
    pid = os.getpid()

    conn = DbConnection()
    conn.query("LOCK TABLE gb_server_processes.processes WRITE")

    if not process_args.allow_multiple:
        cnt = ProcessUtils.process_running_instances(conn=conn, process_name=process_args.get_name())
        if cnt > 0:
            conn.unlock_tables()
            conn.close()
            return

    conn.query("""
    INSERT INTO gb_server_processes.processes (pid, state, process_name, input_args) VALUES (%s, 'working', %s, %s)
    """, [pid, process_args.get_name(), json.dumps(process_args.to_command_line_args())])
    conn.unlock_tables()
    conn.close()

    process_data = {}

    heartbeat_thread = Thread(target=heartbeat, args=(process_data,))
    heartbeat_thread.start()

    process_script(process_args=process_args)
    process_data["process_stopped"] = True

    heartbeat_thread.join()
    sys.exit(0)


if __name__ == '__main__':
    path.insert(0, dirname(__file__))
    root_path = str(Path(__file__).absolute())
    path.append(root_path)

    process_args = ProcessArgs.from_command_line_args()
    
    try:
        start(process_args=process_args)
    except Exception as e:
        LogsDbModel.server_error_log(
            user_id=0,
            title="run_process",
            stacktrace=traceback.format_exc()
        )
        raise e
