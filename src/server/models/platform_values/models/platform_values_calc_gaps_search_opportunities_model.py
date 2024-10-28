from gembase_server_core.db.db_connection import DbConnection
from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.platform_values.calc.platform_values_gaps_v2 import PlatformValuesGapsV2Calc
from src.server.models.platform_values.models.base.platform_values_calc_base_model import PlatformValuesCalcBaseModel
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.user.user_model import UserModel
from src.server.models.user.user_obfuscator import UserObfuscator
from src.utils.gembase_utils import GembaseUtils


class PlatformValuesCalcGapsSearchOpportunitiesModel(PlatformValuesCalcBaseModel):

    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        super(PlatformValuesCalcGapsSearchOpportunitiesModel, self).__init__(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id
        )
        self.calc = PlatformValuesHelper.CALC_GAPS_SEARCH_OPPORTUNITIES

    def get_module_required_unlocked(self):
        return BillingUtils.BILLING_MODULE_IDEAS

    def create_input_data_and_hash_data_from_request_payload(
            self,
            request_payload: {}
    ):
        input_data = {}

        if "tag_details" in request_payload and len(request_payload["tag_details"]) > 0:
            input_data["tag_details"] = request_payload["tag_details"]
            arr_unique = []
            unique_ids = []
            for it in input_data["tag_details"]:
                if it[UserObfuscator.TAG_ID_INT] in unique_ids:
                    continue
                unique_ids.append(it[UserObfuscator.TAG_ID_INT])
                arr_unique.append(it)

            input_data["tag_details"].sort(key=lambda x: x[UserObfuscator.TAG_ID_INT])

        input_data[UserObfuscator.DEV_ID_INT] = UserModel(
            conn=self.conn,
            user_id=self.user_id
        ).get_dev_id_int()

        hash_data = GembaseUtils.json_copy(input_data)

        return input_data, hash_data

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
        PlatformValuesGapsV2Calc.calc(
            conn=self.conn,
            platform_id=platform_id,
            survey_id=self.survey_id,
            dev_id_int=self.input_data[UserObfuscator.DEV_ID_INT],
            selected_tags_details=self.input_data["tag_details"] if "tag_details" in self.input_data else [],
            update_progress_data=update_progress_data
        )

    def generate_client_data(
            self,
            obfuscator: UserObfuscator,
            platform_id: int,
            is_admin=False
    ):
        is_locked = BillingUtils.is_module_locked(
            conn=self.conn,
            user_id=self.user_id,
            module_id=BillingUtils.BILLING_MODULE_IDEAS
        )

        return PlatformValuesGapsV2Calc.generate_client_data(
            conn=self.conn,
            platform_id=platform_id,
            is_admin=is_admin,
            is_locked=is_locked
        )
