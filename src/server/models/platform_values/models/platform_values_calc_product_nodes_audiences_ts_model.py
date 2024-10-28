import json

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.platform_values.calc.platform_values_product_nodes_audience_ts_calc_v2 import \
    PlatformValuesProductNodesAudienceTsCalcV2
from src.server.models.platform_values.models.base.platform_values_calc_base_model import PlatformValuesCalcBaseModel
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.user.user_model import UserModel
from src.server.models.user.user_obfuscator import UserObfuscator
from src.utils.gembase_utils import GembaseUtils


class PlatformValuesCalcProductNodesAudiencesTsModel(PlatformValuesCalcBaseModel):

    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        super(PlatformValuesCalcProductNodesAudiencesTsModel, self).__init__(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id
        )
        self.calc = PlatformValuesHelper.CALC_PRODUCT_NODES_AUDIENCES_TS

    def create_input_data_and_hash_data_from_request_payload(
            self,
            request_payload: {}
    ):
        input_data = {}

        if "tag_details" in request_payload and len(request_payload["tag_details"]) > 0:
            input_data["tag_details"] = request_payload["tag_details"]
            input_data["tag_details"].sort(key=lambda x: x[UserObfuscator.TAG_ID_INT])

        if "exclude_apps_from_competitors" in request_payload:
            exclude_apps_from_competitors = request_payload["exclude_apps_from_competitors"][UserObfuscator.APP_IDS_INT]
            if len(exclude_apps_from_competitors) > 0:
                exclude_apps_from_competitors.sort()
                input_data["exclude_apps_from_competitors"] = exclude_apps_from_competitors

        dev_id_int = UserModel(
            conn=self.conn,
            user_id=self.user_id
        ).get_dev_id_int()

        if UserObfuscator.DEV_ID_INT in request_payload and request_payload[UserObfuscator.DEV_ID_INT] is not None:
            dev_id_int = request_payload[UserObfuscator.DEV_ID_INT]

        input_data[UserObfuscator.DEV_ID_INT] = dev_id_int

        growth = None
        tier = None

        found_app = False
        if UserObfuscator.APP_ID_INT in request_payload and request_payload[UserObfuscator.APP_ID_INT] is not None:
            app_id_int = request_payload[UserObfuscator.APP_ID_INT]
            row_app = self.conn.select_one_or_none("""
                SELECT growth, tier
                  FROM platform.platform_values_apps a 
                 WHERE a.app_id_int = %s
                """, [app_id_int])
            if row_app is not None:
                growth = row_app["growth"]
                tier = row_app["tier"]
                found_app = True

        if not found_app:
            growth = GembaseUtils.try_get_from_dict(
                request_payload,
                "growth"
            )
            tier = GembaseUtils.try_get_from_dict(
                request_payload,
                "tier"
            )

        if tier is not None and tier != 0:
            input_data["tier"] = tier
        if growth is not None and growth != 0:
            input_data["growth"] = growth

        hash_data = json.loads(json.dumps(input_data))

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
        PlatformValuesProductNodesAudienceTsCalcV2.calc(
            conn=self.conn,
            platform_id=platform_id,
            survey_id=self.survey_id,
            dev_id_int=self.input_data[UserObfuscator.DEV_ID_INT],
            my_tier=GembaseUtils.try_get_from_dict(self.input_data, "tier"),
            my_growth=GembaseUtils.try_get_from_dict(self.input_data, "growth"),
            selected_tags_details=self.input_data["tag_details"] if "tag_details" in self.input_data else [],
            exclude_apps_from_competitors=self.input_data["exclude_apps_from_competitors"] if "exclude_apps_from_competitors" in self.input_data else [],
            update_progress_data=update_progress_data
        )

    def generate_client_data(
            self,
            platform_id: int,
            obfuscator: UserObfuscator,
            is_admin=False
    ):
        return PlatformValuesProductNodesAudienceTsCalcV2.generate_client_data(
            conn=self.conn,
            platform_id=platform_id,
            is_admin=is_admin
        )
