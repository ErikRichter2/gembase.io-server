import json

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.calc.calc_product_nodes_audience_ts_db_model import CalcProductNodesAudienceTsDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.calc.factory.base_calc_interface import BaseCalcInterface
from src.db.user_db_model import UserDbModel
from src.models.user_obfuscator import UserObfuscator


class CalcInterfaceProductNodesAudiencesTs(BaseCalcInterface):

    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        super(CalcInterfaceProductNodesAudiencesTs, self).__init__(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id
        )
        self.calc = CalcUtils.CALC_PRODUCT_NODES_AUDIENCES_TS

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

        growth = None
        tier = None
        store = None

        found_app = False
        if UserObfuscator.APP_ID_INT in request_payload and request_payload[UserObfuscator.APP_ID_INT] is not None:
            app_id_int = request_payload[UserObfuscator.APP_ID_INT]
            row_app = self.conn.select_one_or_none("""
                SELECT growth, tier, store
                  FROM gb_platform.platform_values_apps a 
                 WHERE a.app_id_int = %s
                """, [app_id_int])
            if row_app is not None:
                growth = row_app["growth"]
                tier = row_app["tier"]
                store = row_app["store"]
                found_app = True

        if not found_app:
            growth = Utils.try_get_from_dict(
                request_payload,
                "growth"
            )
            tier = Utils.try_get_from_dict(
                request_payload,
                "tier"
            )
            store = Utils.try_get_from_dict(
                request_payload,
                "store"
            )

        if tier is not None:
            input_data["tier"] = tier
        if growth is not None:
            input_data["growth"] = growth
        if store is not None:
            input_data["store"] = store

        hash_data = json.loads(json.dumps(input_data))

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
        CalcProductNodesAudienceTsDbModel(conn=self.conn).calc(
            platform_id=platform_id,
            survey_id=self.survey_id,
            dev_id_int=self.input_data[UserObfuscator.DEV_ID_INT],
            my_tier=Utils.try_get_from_dict(self.input_data, "tier"),
            my_growth=Utils.try_get_from_dict(self.input_data, "growth"),
            my_store=Utils.try_get_from_dict(self.input_data, "store"),
            selected_tags_details=self.input_data["tag_details"],
            exclude_apps_from_competitors=self.input_data["exclude_apps_from_competitors"],
            update_progress_data=update_progress_data
        )

    def generate_client_data(
            self,
            platform_id: int,
            obfuscator: UserObfuscator
    ):
        return CalcProductNodesAudienceTsDbModel(conn=self.conn).generate_client_data(
            platform_id=platform_id,
            survey_id=self.survey_id
        )
