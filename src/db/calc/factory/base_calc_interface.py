import json
from typing import Optional

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.services_db_model import ServicesDbModel
from src.env_data import ENV_DATA
from src.models.user_obfuscator import UserObfuscator
from src.processes.process_args import ProcessArgs

class BaseCalcInterface(BaseDbModel):
    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        super(BaseCalcInterface, self).__init__(conn=conn)
        
        self.hash_key = 0
        self.input_data: dict = {}
        self.user_id = user_id
        self.calc = None
        self.survey_id = survey_id
        self.obfuscator: UserObfuscator | None = None
        self.calc_utils = CalcUtils(conn=conn)

    def set_obfuscator(
            self,
            obfuscator: UserObfuscator
    ):
        self.obfuscator = obfuscator

    def set_input_client_data(
            self,
            input_data: dict,
            obfuscator: UserObfuscator
    ):
        self.obfuscator = obfuscator
        if not self.modify_input_data(input_data=input_data):
            raise Exception(f"Invalid input data")
        input_data, hash_data = self.create_input_data_and_hash_data_from_request_payload(input_data)
        self.input_data = input_data
        if hash_data is None:
            hash_data = input_data
        self.hash_key = Utils.hash(json.dumps(hash_data, sort_keys=True))
        return self

    def set_input_server_data(
            self,
            input_data: dict,
            hash_key: int
    ):
        self.input_data = input_data
        self.hash_key = hash_key
        return self

    def modify_input_data(
            self,
            input_data: dict
    ) -> bool:
        return True

    def create_input_data_and_hash_data_from_request_payload(
            self,
            request_payload: dict
    ) -> tuple[dict, Optional[dict]]:
        return {}, None

    def get_cached_result(
            self,
            obfuscator: UserObfuscator
    ):
        row_cached = self.conn.select_one_or_none("""
           SELECT s.platform_id, s.survey_id, s.state, s.input_data
             FROM gb_calc.requests s
            WHERE s.hash_key = %s
              AND s.user_id = %s
              AND s.calc = %s
              AND s.state != 'error'
              AND s.state != 'killed'
              AND s.version = %s
            ORDER BY s.t_end DESC
           """, [self.hash_key, self.user_id, self.calc, self.calc_utils.get_calc_version()])

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
                        obfuscator=obfuscator
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
        SELECT gb_platform.next_platform_id() as next_id
        """)["next_id"]
        conn.query("""
        DELETE FROM gb_calc.requests_queue
         WHERE user_id = %s AND calc = %s AND hash_key = %s AND survey_id = %s
        """, [user_id, calc, hash_key, survey_id])
        conn.query("""
        INSERT INTO gb_calc.requests_queue
        (platform_id, user_id, calc, hash_key, survey_id, input_data)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, [platform_id, user_id, calc, hash_key, survey_id, input_data])

        conn.commit()
        
        services = ServicesDbModel(conn=conn)
        services.run(ProcessArgs(
            user_id=user_id,
            service_script_id=ServicesDbModel.SERVICE_PLATFORM_VALUES_SINGLE_APP
        ))
        services.update_service_shared_mem(f"service_platform_values__{ENV_DATA.app_env}")
        
        conn.close()

        return {
            "state": "queue",
            "hash_key": hash_key
        }

    def add_to_queue(self):
        if self.calc is None:
            raise Exception('calc not defined')
        
        return self.create_queue(
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
            obfuscator: UserObfuscator
    ) -> dict:
        raise Exception('Not implemented')
