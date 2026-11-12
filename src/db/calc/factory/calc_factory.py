import json

from server_core.db.db_connection import DbConnection
from server_core.logs.logs_db_model import LogsDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.calc.factory.base_calc_interface import BaseCalcInterface


class CalcFactory:

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
              FROM gb_calc.requests
             WHERE platform_id = %s
            """, [platform_id])

        input_data = {} if row["input_data"] is None else json.loads(row["input_data"])

        m = CalcFactory.create(
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
    ) -> BaseCalcInterface:
        if calc == CalcUtils.CALC_AUDIENCES_ANGLES:
            from src.db.calc.factory.calc_interface_audience_angles import CalcInterfaceAudienceAngles
            res = CalcInterfaceAudienceAngles(
                conn=conn,
                user_id=user_id,
                survey_id=survey_id
            )
        elif calc == CalcUtils.CALC_COMPETITORS_FOR_AUDIENCE_ANGLE:
            from src.db.calc.factory.calc_interface_competitors import CalcInterfaceCompetitors
            res = CalcInterfaceCompetitors(
                conn=conn,
                user_id=user_id,
                survey_id=survey_id
            )
        elif calc == CalcUtils.CALC_PRODUCT_NODES_AUDIENCES_TS:
            from src.db.calc.factory.calc_interface_product_nodes_audience_ts import CalcInterfaceProductNodesAudiencesTs
            res = CalcInterfaceProductNodesAudiencesTs(
                conn=conn,
                user_id=user_id,
                survey_id=survey_id
            )
        elif calc == CalcUtils.CALC_GAPS_SEARCH_OPPORTUNITIES:
            from src.db.calc.factory.calc_interface_gaps_opportunities import CalcInterfaceGapsOpportunities
            res = CalcInterfaceGapsOpportunities(
                conn=conn,
                user_id=user_id,
                survey_id=survey_id
            )
        elif calc == CalcUtils.CALC_GAPS_AUDIENCES_TS:
            from src.db.calc.factory.calc_interface_audience_gaps import CalcInterfaceAudienceGaps
            res = CalcInterfaceAudienceGaps(
                conn=conn,
                user_id=user_id,
                survey_id=survey_id
            )
        else:
            raise Exception(f"Unknown calc: {calc}")

        return res
