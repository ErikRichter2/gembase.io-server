import json
import time
import traceback
from threading import Thread

from mysql.connector import OperationalError

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.environment.runtime_constants import rr
from src.server.models.logs.logs_model import LogsModel
from src.server.models.platform_values.cache.platform_values_cache import PlatformValuesCache
from src.server.models.platform_values.models.factory.platform_values_calc_model_factory import \
    PlatformValuesCalcModelFactory
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.utils.gembase_utils import GembaseUtils
from multiprocessing import shared_memory


threads = []
idle_time = 0
calc_version: int | None = None


def platform_values_calc_service():

    GembaseUtils.log_service("Platform calc service START")
    shm_name = f"service_platform_values__{rr.ENV}"

    try:
        shm = shared_memory.SharedMemory(
            create=True,
            name=shm_name,
            size=1
        )
    except FileExistsError:
        shm = shared_memory.SharedMemory(
            create=False,
            name=shm_name,
            size=1)
    except Exception as err:
        raise err

    buffer = shm.buf

    process_conn = DbConnection()
    platform_values_cache = PlatformValuesCache(conn=process_conn)

    def __close_shm():
        process_conn.close()
        __kill_all_threads()
        if shm is not None:
            shm.close()
            shm.unlink()

    first_loop = True
    delta_time = 0

    while True:

        do_loop = False or first_loop
        first_loop = False

        if buffer[0] == -1:
            break

        if buffer[0] == 1:
            buffer[0] = 0
            do_loop = True

        if delta_time > 10:
            do_loop = True

        if do_loop:
            try:
                delta_time = 0
                __process_app_recalc(
                    conn=process_conn,
                    platform_values_cache=platform_values_cache
                )
                res_queue = __process_queue(conn=process_conn)
                __remove_finished_threads()
                if not res_queue:
                    break
            except Exception as err:
                __close_shm()
                raise err

        delta_time += 0.1
        time.sleep(0.1)

        if rr.is_debug():
            global idle_time
            if len(threads) == 0:
                idle_time += 0.1
                if idle_time >= 15:
                    break
            else:
                idle_time = 0

    __close_shm()
    GembaseUtils.log_service("Platform calc service END")


def __kill_all_threads():
    for thread in threads:
        thread.kill()
    threads.clear()


def __remove_finished_threads():
    finished_threads = []
    for thread in threads:
        if not thread.is_alive():
            finished_threads.append(thread)
    for thread in finished_threads:
        thread.kill()
        if thread in threads:
            threads.remove(thread)


def __process_app_recalc(conn: DbConnection, platform_values_cache: PlatformValuesCache):

    while True:

        row_app_id_int = conn.select_one_or_none("""
        SELECT id, app_id_int
          FROM platform_values.requests_app_recalc
         LIMIT 1
        """)

        if row_app_id_int is None:
            break

        platform_values_cache.rebuild_for_single_app(
            app_id_int=row_app_id_int["app_id_int"]
        )

        conn.query("""
        DELETE FROM platform_values.requests_app_recalc
        WHERE id = %s
        """, [row_app_id_int["id"]])

        conn.commit()


def __process_queue(conn: DbConnection):

    calc_version_in_db = PlatformValuesHelper.get_calc_version(conn=conn)

    global calc_version
    if calc_version is None:
        calc_version = calc_version_in_db

    if calc_version < calc_version_in_db:
        return False

    rows_queue = conn.select_all("""
    SELECT q.platform_id, q.user_id, q.calc, q.hash_key, q.input_data, q.survey_id
      FROM platform_values.requests_queue q
      WHERE NOT EXISTS (
     SELECT 1
       FROM platform_values.requests r
      WHERE r.state != 'error'
        AND r.state != 'killed'
        AND r.user_id = q.user_id
        AND r.survey_id = q.survey_id
        AND r.hash_key = q.hash_key
        AND r.version = %s
        AND r.calc = q.calc)
    """, [PlatformValuesHelper.get_calc_version(conn=conn)])
    conn.query("""
    DELETE FROM platform_values.requests_queue q
    """)
    conn.commit()

    queue_threads = []
    threads_to_kill = []
    for row in rows_queue:
        for thread in threads:
            if thread.can_be_killed_by(user_id=row["user_id"], calc=row["calc"]):
                threads_to_kill.append(thread)

        cnt = conn.select_one("""
        SELECT count(1) as cnt
       FROM platform_values.requests r
      WHERE r.state != 'error'
        AND r.state != 'killed'
        AND r.user_id = %s
        AND r.survey_id = %s
        AND r.hash_key = %s
        AND r.calc = %s
        AND r.version = %s
        """, [row["user_id"], row["survey_id"], row["hash_key"], row["calc"], PlatformValuesHelper.get_calc_version(conn=conn)])["cnt"]

        if cnt == 0:
            conn.query("""
            INSERT INTO platform_values.requests 
            (platform_id, user_id, hash_key, calc, state, input_data, survey_id, conn_id, version)
            VALUES
            (%s, %s, %s, %s, 'working', %s, %s, 0, %s) 
            """, [row["platform_id"], row["user_id"], row["hash_key"], row["calc"], row["input_data"],
                  row["survey_id"], PlatformValuesHelper.get_calc_version(conn=conn)])
            conn.commit()

            queue_thread = PlatformValuesServiceThread(
                platform_id=row["platform_id"],
                user_id=row["user_id"],
                calc=row["calc"]
            )
            queue_threads.append(queue_thread)

    for thread_to_kill in threads_to_kill:
        thread_to_kill.kill(delete_request=True)

    for queue_thread in queue_threads:
        queue_thread.start()

    return True


def __run_thread_internal(platform_id: int, user_id: int):
    conn = ServiceWrapperModel.create_conn()

    conn.query("""
    UPDATE platform_values.requests
       SET conn_id = %s
     WHERE platform_id = %s
    """, [conn.connection_id(), platform_id])
    conn.commit()

    def update_progress_data(progress_data: {}):
        conn.query("""
        UPDATE platform_values.requests 
           SET progress_data = %s
         WHERE platform_id = %s
        """, [json.dumps(progress_data), platform_id])
        conn.commit()

    def handle_exception(err):
        DbConnection.s_query("""
                    UPDATE platform_values.requests s
                       SET s.t_end = NOW(),
                           s.t_heartbeat = NOW(),
                           s.state = 'error'
                     WHERE s.platform_id = %s
                    """, [platform_id])
        LogsModel.server_error_log(
            user_id=user_id,
            title=str(err),
            stacktrace=traceback.format_exc()
        )

    try:
        PlatformValuesCalcModelFactory.calc(
            conn=conn,
            platform_id=platform_id,
            update_progress_data=update_progress_data
        )

        conn.query("""
            UPDATE platform_values.requests s
               SET s.t_end = NOW(),
                   s.t_heartbeat = NOW(),
                   s.state = 'done'
             WHERE s.platform_id = %s
            """, [platform_id])
        conn.commit()
        ServiceWrapperModel.close_conn(conn_id=conn.connection_id(), conn=conn)
    except OperationalError as err:
        conn.close()
        if err.errno == 2013:
            pass
        else:
            handle_exception(err)
    except Exception as err:
        conn.close()
        handle_exception(err)


def run_thread(platform_id: int, user_id: int):
    __run_thread_internal(platform_id=platform_id, user_id=user_id)


def default_method(*args, **kwargs):
    platform_values_calc_service()


class PlatformValuesServiceThread:

    def __init__(self, platform_id: int, user_id: int, calc: str):
        self.platform_id = platform_id
        self.user_id = user_id
        self.calc = calc
        self.thread: Thread | None = None

    def can_be_killed_by(self, user_id: int, calc: str):
        if self.user_id == user_id:
            if self.calc == calc:
                if calc != PlatformValuesHelper.CALC_AUDIENCES_ANGLES and calc != PlatformValuesHelper.CALC_COMPETITORS_FOR_AUDIENCE_ANGLE:
                    return True

        return False

    def kill(self, delete_request=False):
        conn = DbConnection()
        request_row = conn.select_one_or_none("""
        SELECT r.conn_id
          FROM platform_values.requests r
         WHERE r.platform_id = %s
        """, [self.platform_id])
        if delete_request:
            conn.query("""
            DELETE FROM platform_values.requests WHERE platform_id = %s
            """, [self.platform_id])
            conn.commit()
        conn.close()
        if request_row is not None:
            conn_id = request_row["conn_id"]
            ServiceWrapperModel.close_conn(conn_id=conn_id)
        if self in threads:
            threads.remove(self)

    def start(self):
        self.thread = Thread(
            target=run_thread,
            args=(self.platform_id, self.user_id)
        )
        if self not in threads:
            threads.append(self)
        self.thread.start()

    def is_alive(self):
        return self.thread is not None and self.thread.is_alive()
