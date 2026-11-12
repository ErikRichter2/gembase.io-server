from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.calc.calc_audience_angles_db_model import CalcAudienceAnglesDbModel
from src.db.calc.calc_audience_gaps_db_model import CalcAudienceGapsDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.calc.factory.base_calc_interface import BaseCalcInterface
from src.db.user_db_model import UserDbModel
from src.models.user_obfuscator import UserObfuscator

class CalcInterfaceAudienceGaps(BaseCalcInterface):

    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        super(CalcInterfaceAudienceGaps, self).__init__(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id
        )
        self.calc = CalcUtils.CALC_GAPS_AUDIENCES_TS

    def create_input_data_and_hash_data_from_request_payload(
            self,
            request_payload: dict
    ):
        input_data = {}

        input_data["tag_details"] = request_payload["tag_details"]
        input_data["tag_details"].sort(key=lambda x: x[UserObfuscator.TAG_ID_INT])

        input_data[UserObfuscator.DEV_ID_INT] = UserDbModel(
            conn=self.conn,
            user_id=self.user_id
        ).games_db.get_dev_id_int()

        hash_data = Utils.json_copy(input_data)

        return input_data, hash_data

    def modify_input_data(
            self,
            input_data: dict
    ) -> bool:
        return True

    def do_calc(
            self,
            platform_id: int,
            update_progress_data=None
    ):
        CalcAudienceGapsDbModel(conn=self.conn).calc(
            platform_id=platform_id,
            survey_id=self.survey_id,
            dev_id_int=self.input_data[UserObfuscator.DEV_ID_INT],
            selected_tags_details=self.input_data["tag_details"],
            update_progress_data=update_progress_data
        )

    def generate_client_data(
            self,
            platform_id: int,
            obfuscator: UserObfuscator
    ):
        return CalcAudienceGapsDbModel(conn=self.conn).generate_client_data(
            platform_id=platform_id,
        )
