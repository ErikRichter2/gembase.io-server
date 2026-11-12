from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.calc.calc_audience_angles_db_model import CalcAudienceAnglesDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.calc.factory.base_calc_interface import BaseCalcInterface
from src.db.user_db_model import UserDbModel
from src.models.user_obfuscator import UserObfuscator

class CalcInterfaceAudienceAngles(BaseCalcInterface):

    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        super(CalcInterfaceAudienceAngles, self).__init__(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id
        )
        self.calc = CalcUtils.CALC_AUDIENCES_ANGLES

    def create_input_data_and_hash_data_from_request_payload(
            self,
            request_payload: dict
    ):
        input_data = {}

        if len(request_payload["tag_details"]) > 0:
            input_data["tag_details"] = request_payload["tag_details"]
            input_data["tag_details"].sort(key=lambda x: x[UserObfuscator.TAG_ID_INT])

        Utils.set_if_not_none(
            d=input_data,
            key="include_angle",
            val=Utils.try_get_from_dict(
                request_payload, "include_angle", UserObfuscator.AUDIENCE_ANGLE_ID_INT
            )
        )

        Utils.set_if_not_none(
            d=input_data,
            key="exclusive_angle",
            val=Utils.try_get_from_dict(
                request_payload, "exclusive_angle", UserObfuscator.AUDIENCE_ANGLE_ID_INT
            )
        )

        dev_id_int = UserDbModel(conn=self.conn, user_id=self.user_id).games_db.get_dev_id_int()
        if UserObfuscator.DEV_ID_INT in request_payload and request_payload[UserObfuscator.DEV_ID_INT] is not None:
            dev_id_int = request_payload[UserObfuscator.DEV_ID_INT]

        input_data[UserObfuscator.DEV_ID_INT] = dev_id_int

        input_hash = Utils.json_copy(input_data)

        return input_data, input_hash

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
        include_angle = None
        if "include_angle" in self.input_data:
            include_angle = self.input_data["include_angle"]

        exclusive_angle = None
        if "exclusive_angle" in self.input_data:
            exclusive_angle = self.input_data["exclusive_angle"]

        # audience angles
        CalcAudienceAnglesDbModel(conn=self.conn).calc(
            survey_id=self.survey_id,
            platform_id=platform_id,
            dev_id_int=self.input_data["dev_id_int"],
            tag_details=self.input_data["tag_details"],
            include_angle=include_angle,
            exclusive_angle=exclusive_angle
        )

    def generate_client_data(
            self,
            platform_id: int,
            obfuscator: UserObfuscator
    ):
        return CalcAudienceAnglesDbModel(conn=self.conn).generate_client_data(
                platform_id=platform_id
            )
