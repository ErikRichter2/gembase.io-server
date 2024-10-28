from gembase_server_core.db.db_connection import DbConnection
from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.platform_values.calc.platform_values_competitors_calc import PlatformValuesCompetitorsCalc
from src.server.models.platform_values.models.base.platform_values_calc_base_model import PlatformValuesCalcBaseModel
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.user.user_model import UserModel
from src.server.models.user.user_obfuscator import UserObfuscator
from src.utils.gembase_utils import GembaseUtils


class PlatformValuesCalcCompetitorsForAudienceAngleModel(PlatformValuesCalcBaseModel):

    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        super(PlatformValuesCalcCompetitorsForAudienceAngleModel, self).__init__(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id
        )
        self.calc = PlatformValuesHelper.CALC_COMPETITORS_FOR_AUDIENCE_ANGLE

    def create_input_data_and_hash_data_from_request_payload(
            self,
            request_payload: {}
    ):
        input_data = {}

        if "tag_details" in request_payload:
            tag_details = request_payload["tag_details"]
            if tag_details is not None and len(tag_details) > 0:
                tag_details.sort(key=lambda x: x[UserObfuscator.TAG_ID_INT])
                input_data["tag_details"] = tag_details

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

        app_found = False

        tier = None
        growth = None

        if UserObfuscator.APP_ID_INT in request_payload and request_payload[UserObfuscator.APP_ID_INT] is not None:
            app_id_int = request_payload[UserObfuscator.APP_ID_INT]
            row_app = self.conn.select_one_or_none("""
            SELECT growth, tier
              FROM platform.platform_values_apps a 
             WHERE a.app_id_int = %s
            """, [app_id_int])
            if row_app is not None:
                app_found = True
                growth = row_app["growth"]
                tier = row_app["tier"]

        if not app_found:
            if "growth" in request_payload and request_payload["growth"] is not None:
                growth = request_payload["growth"]
            if "tier" in request_payload and request_payload["tier"] is not None:
                tier = request_payload["tier"]

        if tier is not None and tier != 0:
            input_data["tier"] = tier
        if growth is not None and growth != 0:
            input_data["growth"] = growth

        input_data["audience_angle_row_id"] = request_payload["audience_angle_row_id"]

        if "advanced_filter" in request_payload and request_payload["advanced_filter"] is not None:
            advanced_filter = request_payload["advanced_filter"]
            if "weights" in advanced_filter:
                input_data["tags_weights"] = advanced_filter["weights"]

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
        PlatformValuesCompetitorsCalc.find_competitors_for_audience_angle(
            conn=self.conn,
            platform_id=platform_id,
            survey_id=self.survey_id,
            my_tier=self.input_data["tier"] if "tier" in self.input_data else None,
            my_growth=self.input_data["growth"] if "growth" in self.input_data else None,
            my_tags_details=self.input_data["tag_details"] if "tag_details" in self.input_data else [],
            exclude_apps_from_competitors=self.input_data["exclude_apps_from_competitors"] if "exclude_apps_from_competitors" in self.input_data else [],
            audience_angle_row_id=self.input_data["audience_angle_row_id"],
            dev_id_int=self.input_data[UserObfuscator.DEV_ID_INT],
            tags_weights=self.input_data["tags_weights"] if "tags_weights" in self.input_data else None
        )

    def generate_client_data(
            self,
            platform_id: int,
            obfuscator: UserObfuscator,
            is_admin=False
    ):
        res = PlatformValuesCompetitorsCalc.generate_client_data(
            conn=self.conn,
            platform_id=platform_id,
            is_admin=is_admin
        )

        if len(res["data"]) == 1:
            res["data"][0]["competitor_apps_details"] = res["app_details"]
            return res["data"][0]

        return {
            "ts_items": [],
            "platform_id": platform_id,
            "competitors_count": 0,
            "competitors_pool_cnt": 0,
            "ts": 0
        }
