import json

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.platform_values.models.platform_values_calc_audience_angles_model import \
    PlatformValuesCalcAudienceAnglesModel
from src.server.models.platform_values.models.base.platform_values_calc_base_model import PlatformValuesCalcBaseModel
from src.server.models.platform_values.models.platform_values_calc_competitors_for_audience_angle_model import \
    PlatformValuesCalcCompetitorsForAudienceAngleModel
from src.server.models.platform_values.models.platform_values_calc_product_nodes_audiences_ts_model import \
    PlatformValuesCalcProductNodesAudiencesTsModel
from src.server.models.platform_values.models.platform_values_calc_gaps_search_opportunities_model import \
    PlatformValuesCalcGapsSearchOpportunitiesModel
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper


class PlatformValuesCalcModelFactory:

    @staticmethod
    def calc(
            conn: DbConnection,
            platform_id: int,
            update_progress_data=None,
    ):
        row = conn.select_one("""
            SELECT calc, 
                   survey_id, 
                   input_data, 
                   user_id, 
                   hash_key
              FROM platform_values.requests
             WHERE platform_id = %s
            """, [platform_id])

        input_data = None if row["input_data"] is None else json.loads(row["input_data"])

        m = PlatformValuesCalcModelFactory.create(
            conn=conn,
            user_id=row["user_id"],
            survey_id=row["survey_id"],
            calc=row["calc"]
        ).set_input_server_data(
            input_data=input_data,
            hash_key=row["hash_key"]
        )

        m.do_calc(
            platform_id=platform_id,
            update_progress_data=update_progress_data
        )

    @staticmethod
    def create(
            conn: DbConnection,
            user_id: int,
            survey_id: int,
            calc: str
    ) -> PlatformValuesCalcBaseModel:
        if calc == PlatformValuesHelper.CALC_AUDIENCES_ANGLES:
            res = PlatformValuesCalcAudienceAnglesModel(
                conn=conn,
                user_id=user_id,
                survey_id=survey_id
            )
        elif calc == PlatformValuesHelper.CALC_COMPETITORS_FOR_AUDIENCE_ANGLE:
            res = PlatformValuesCalcCompetitorsForAudienceAngleModel(
                conn=conn,
                user_id=user_id,
                survey_id=survey_id
            )
        elif calc == PlatformValuesHelper.CALC_PRODUCT_NODES_AUDIENCES_TS:
            res = PlatformValuesCalcProductNodesAudiencesTsModel(
                conn=conn,
                user_id=user_id,
                survey_id=survey_id
            )
        elif calc == PlatformValuesHelper.CALC_GAPS_SEARCH_OPPORTUNITIES:
            res = PlatformValuesCalcGapsSearchOpportunitiesModel(
                conn=conn,
                user_id=user_id,
                survey_id=survey_id
            )
        else:
            raise Exception(f"Unknown calc: {calc}")

        return res
