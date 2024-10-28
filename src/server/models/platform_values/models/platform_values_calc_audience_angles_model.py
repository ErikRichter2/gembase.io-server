from gembase_server_core.db.db_connection import DbConnection
from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.platform_values.calc.platform_values_audience_angle_calc import PlatformValuesAudienceAngleCalc
from src.server.models.platform_values.models.base.platform_values_calc_base_model import PlatformValuesCalcBaseModel
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.user.user_model import UserModel
from src.server.models.user.user_obfuscator import UserObfuscator
from src.utils.gembase_utils import GembaseUtils


class PlatformValuesCalcAudienceAnglesModel(PlatformValuesCalcBaseModel):

    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        super(PlatformValuesCalcAudienceAnglesModel, self).__init__(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id
        )
        self.calc = PlatformValuesHelper.CALC_AUDIENCES_ANGLES

    def create_input_data_and_hash_data_from_request_payload(
            self,
            request_payload: {}
    ):
        input_data = {}

        if "tag_details" in request_payload and len(request_payload["tag_details"]) > 0:
            input_data["tag_details"] = request_payload["tag_details"]
            input_data["tag_details"].sort(key=lambda x: x[UserObfuscator.TAG_ID_INT])

        GembaseUtils.set_if_not_none(
            d=input_data,
            key="include_angle",
            val=GembaseUtils.try_get_from_dict(
                request_payload, "include_angle", UserObfuscator.AUDIENCE_ANGLE_ID_INT
            )
        )

        GembaseUtils.set_if_not_none(
            d=input_data,
            key="exclusive_angle",
            val=GembaseUtils.try_get_from_dict(
                request_payload, "exclusive_angle", UserObfuscator.AUDIENCE_ANGLE_ID_INT
            )
        )

        dev_id_int = UserModel(conn=self.conn, user_id=self.user_id).get_dev_id_int()
        if UserObfuscator.DEV_ID_INT in request_payload and request_payload[UserObfuscator.DEV_ID_INT] is not None:
            dev_id_int = request_payload[UserObfuscator.DEV_ID_INT]

        input_data[UserObfuscator.DEV_ID_INT] = dev_id_int

        input_hash = GembaseUtils.json_copy(input_data)

        return input_data, input_hash

    def modify_input_data(
            self,
            input_data: {}
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
        PlatformValuesAudienceAngleCalc.calc(
            conn=self.conn,
            survey_id=self.survey_id,
            platform_id=platform_id,
            dev_id_int=self.input_data["dev_id_int"],
            tag_details=self.input_data["tag_details"] if "tag_details" in self.input_data else [],
            include_angle=include_angle,
            exclusive_angle=exclusive_angle
        )

    def generate_client_data(
            self,
            platform_id: int,
            obfuscator: UserObfuscator,
            is_admin=False
    ):
        return PlatformValuesAudienceAngleCalc.generate_client_data(
                conn=self.conn,
                platform_id=platform_id,
                is_admin=is_admin
            )
