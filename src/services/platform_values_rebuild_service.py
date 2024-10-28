import json
import os

from src.server.models.platform_values.platform_values_cron import PlatformValuesCron
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.server.models.session.models.admin_session_model import AdminSessionModel


def __process():

    conn = ServiceWrapperModel.create_conn()

    def __update_progress(progress_data):
        if progress_data is not None:
            progress_data = json.dumps(progress_data)
        conn.query("""
        UPDATE app_temp_data.server_services
        SET service_data = %s
        WHERE pid = %s
        """, [progress_data, os.getpid()])
        conn.commit()

    __update_progress({
        "step_index": 0,
        "step_desc": "Updating product sheet"
    })

    AdminSessionModel.update_def_sheets(
        conn=conn,
        sheet_name="platform"
    )

    __update_progress({
        "step_index": 0,
        "step_desc": "Updating platform values sheet"
    })

    AdminSessionModel.update_def_sheets(
        conn=conn,
        sheet_name="platform_values"
    )

    PlatformValuesCron.d(conn=conn, update_progress=__update_progress)

    ServiceWrapperModel.kill_by_def_id(def_id=ServiceWrapperModel.SERVICE_PLATFORM_VALUES_CALC)

    conn.query("""
    DELETE FROM platform_values.requests
    """)
    conn.commit()

    ServiceWrapperModel.close_conn(
        conn_id=conn.connection_id(),
        conn=conn
    )


def default_method(*args, **kwargs):
    __process()
