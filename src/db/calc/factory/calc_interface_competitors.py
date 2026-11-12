from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.calc.calc_audience_angles_db_model import CalcAudienceAnglesDbModel
from src.db.calc.calc_competitors_db_model import CalcCompetitorsDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.calc.factory.base_calc_interface import BaseCalcInterface
from src.db.user_db_model import UserDbModel
from src.models.user_obfuscator import UserObfuscator

class CalcInterfaceCompetitors(BaseCalcInterface):

    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        super(CalcInterfaceCompetitors, self).__init__(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id
        )
        self.calc = CalcUtils.CALC_COMPETITORS_FOR_AUDIENCE_ANGLE

    def create_input_data_and_hash_data_from_request_payload(
            self,
            request_payload: dict
    ):
        input_data = {}

        input_data["tag_details"] = request_payload["tag_details"]
        input_data["tag_details"].sort(key=lambda x: x[UserObfuscator.TAG_ID_INT])

        if Utils.set_if_not_none(
            d=input_data,
            key="exclude_apps_from_competitors",
            val=Utils.try_get_from_dict(
                request_payload, "exclude_apps_from_competitors", UserObfuscator.APP_IDS_INT
            )
        ):
            input_data["exclude_apps_from_competitors"].sort()

        dev_id_int = UserDbModel(
            conn=self.conn,
            user_id=self.user_id
        ).games_db.get_dev_id_int()

        if UserObfuscator.DEV_ID_INT in request_payload and request_payload[UserObfuscator.DEV_ID_INT] is not None:
            dev_id_int = request_payload[UserObfuscator.DEV_ID_INT]

        input_data[UserObfuscator.DEV_ID_INT] = dev_id_int

        app_found = False

        tier = 0
        growth = 0
        if UserObfuscator.APP_ID_INT in request_payload and request_payload[UserObfuscator.APP_ID_INT] is not None:
            app_id_int = request_payload[UserObfuscator.APP_ID_INT]
            row_app = self.conn.select_one_or_none("""
            SELECT growth, tier
              FROM gb_platform.platform_values_apps a 
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

        input_data["tier"] = tier
        input_data["growth"] = growth

        input_data["audience_angle_platform_id"] = request_payload["audience_angle_platform_id"]
        input_data[UserObfuscator.AUDIENCE_ANGLE_ID_INT] = request_payload[UserObfuscator.AUDIENCE_ANGLE_ID_INT]

        Utils.set_if_not_none(
            d=input_data,
            key="advanced_filter",
            val=Utils.try_get_from_dict(
                request_payload, "advanced_filter"
            )
        )

        Utils.set_if_not_none(
            d=input_data,
            key="store",
            val=Utils.try_get_from_dict(
                request_payload, "store"
            )
        )

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
        CalcCompetitorsDbModel(conn=self.conn).find_competitors_for_audience_angle(
            platform_id=platform_id,
            survey_id=self.survey_id,
            my_store=self.input_data["store"] if "store" in self.input_data else None,
            my_tier=self.input_data["tier"],
            my_growth=self.input_data["growth"],
            my_tags_details=self.input_data["tag_details"],
            exclude_apps_from_competitors=self.input_data["exclude_apps_from_competitors"],
            audience_angle_id=self.input_data[UserObfuscator.AUDIENCE_ANGLE_ID_INT],
            audience_angle_platform_id=self.input_data["audience_angle_platform_id"],
            dev_id_int=self.input_data[UserObfuscator.DEV_ID_INT],
            advanced_filter=self.input_data["advanced_filter"] if "advanced_filter" in self.input_data else None
        )

    def generate_client_data(
            self,
            platform_id: int,
            obfuscator: UserObfuscator
    ):
        return CalcCompetitorsDbModel(conn=self.conn).generate_client_data(
                platform_id=platform_id,
                obfuscator=obfuscator
            )
