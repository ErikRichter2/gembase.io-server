import json

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.environment.runtime_constants import rr
from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.server.models.user.user_data import UserData
from src.server.models.user.user_obfuscator import UserObfuscator
from src.utils.gembase_utils import GembaseUtils


class PlatformValuesCalcBaseModel:
    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        self.conn = conn
        self.hash_key = 0
        self.input_data = None
        self.user_id = user_id
        self.calc = None
        self.survey_id = survey_id
        self.obfuscator: UserObfuscator | None = None

    def set_obfuscator(
            self,
            obfuscator: UserObfuscator
    ):
        self.obfuscator = obfuscator

    def set_input_client_data(
            self,
            input_data: {},
            obfuscator: UserObfuscator
    ):
        self.obfuscator = obfuscator
        if not self.modify_input_data(input_data=input_data):
            raise Exception(f"Invalid input data")
        input_data, hash_data = self.create_input_data_and_hash_data_from_request_payload(input_data)
        self.input_data = input_data
        if hash_data is None:
            hash_data = input_data
        self.hash_key = GembaseUtils.hash(json.dumps(hash_data, sort_keys=True))
        if BillingUtils.is_module_locked(conn=self.conn, user_id=self.user_id, module_id=self.get_module_required_unlocked()):
            self.user_id = UserData.demo_batch_user_id(conn=self.conn)
        return self

    def get_module_required_unlocked(self):
        return BillingUtils.BILLING_MODULE_AUDITOR

    def set_input_server_data(
            self,
            input_data: {},
            hash_key: int
    ):
        self.input_data = input_data
        self.hash_key = hash_key
        return self

    def modify_input_data(
            self,
            input_data: {}
    ) -> bool:
        return True

    def create_input_data_and_hash_data_from_request_payload(
            self,
            request_payload: {}
    ):
        return None, None

    def get_cached_result(
            self,
            obfuscator: UserObfuscator,
            is_admin=False
    ):
        row_cached = self.conn.select_one_or_none("""
           SELECT s.platform_id, s.survey_id, s.state, s.input_data
             FROM platform_values.requests s
            WHERE s.hash_key = %s
              AND s.user_id = %s
              AND s.calc = %s
              AND s.state != 'error'
              AND s.state != 'killed'
              AND s.version = %s
            ORDER BY s.t_end DESC
           """, [self.hash_key, self.user_id, self.calc, PlatformValuesHelper.get_calc_version(conn=self.conn)])

        if row_cached is not None:
            return {
                "metadata": {
                    "state": row_cached["state"],
                    "platform_id": row_cached["platform_id"],
                    "hash_key": self.hash_key,
                },
                "payload": {
                    "result_data": self.generate_client_data(
                        platform_id=row_cached["platform_id"],
                        obfuscator=obfuscator,
                        is_admin=is_admin
                    )
                }
            }

        return None

    @staticmethod
    def create_queue(
            user_id: int,
            calc: str,
            hash_key: int,
            survey_id: int,
            input_data: str
    ):
        conn = DbConnection()
        platform_id = conn.select_one("""
        SELECT platform.next_platform_id() as next_id
        """)["next_id"]
        conn.query("""
        DELETE FROM platform_values.requests_queue
         WHERE user_id = %s AND calc = %s AND hash_key = %s AND survey_id = %s
        """, [user_id, calc, hash_key, survey_id])
        conn.query("""
        INSERT INTO platform_values.requests_queue
        (platform_id, user_id, calc, hash_key, survey_id, input_data)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, [platform_id, user_id, calc, hash_key, survey_id, input_data])

        conn.commit()
        conn.close()

        if rr.is_debug() and rr.ENV != rr.ENV_DEV:
            pass
        else:
            ServiceWrapperModel.run(
                d=ServiceWrapperModel.SERVICE_PLATFORM_VALUES_CALC
            )
            ServiceWrapperModel.update_service_shared_mem(f"service_platform_values__{rr.ENV}")

        return {
            "state": "queue",
            "hash_key": hash_key
        }

    def add_to_queue(self):
        return PlatformValuesCalcBaseModel.create_queue(
            user_id=self.user_id,
            calc=self.calc,
            hash_key=self.hash_key,
            survey_id=self.survey_id,
            input_data=json.dumps(self.input_data)
        )

    def do_calc(
            self,
            platform_id: int,
            update_progress_data=None
    ):
        pass

    def generate_client_data(
            self,
            platform_id: int,
            obfuscator: UserObfuscator,
            is_admin=False
    ):
        pass
