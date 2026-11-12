from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.calc.calc_gaps_opportunities_db_model import CalcGapsOpportunitiesDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.calc.factory.base_calc_interface import BaseCalcInterface
from src.db.user_db_model import UserDbModel
from src.models.user_obfuscator import UserObfuscator


class CalcInterfaceGapsOpportunities(BaseCalcInterface):

    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            survey_id: int
    ):
        super(CalcInterfaceGapsOpportunities, self).__init__(
            conn=conn,
            user_id=user_id,
            survey_id=survey_id
        )
        self.calc = CalcUtils.CALC_GAPS_SEARCH_OPPORTUNITIES


    def create_input_data_and_hash_data_from_request_payload(
            self,
            request_payload: dict
    ):
        input_data = {}

        input_data["tag_details"] = request_payload["tag_details"]

        arr_unique = []
        unique_ids = []
        for it in input_data["tag_details"]:
            if it[UserObfuscator.TAG_ID_INT] in unique_ids:
                continue
            unique_ids.append(it[UserObfuscator.TAG_ID_INT])
            arr_unique.append(it)

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
        CalcGapsOpportunitiesDbModel(conn=self.conn).calc(
            platform_id=platform_id,
            survey_id=self.survey_id,
            dev_id_int=self.input_data[UserObfuscator.DEV_ID_INT],
            selected_tags_details=self.input_data["tag_details"],
            update_progress_data=update_progress_data
        )

    def generate_client_data(
            self,
            obfuscator: UserObfuscator,
            platform_id: int | None = None,
            opportunity_guid: str | None = None
    ):
        return CalcGapsOpportunitiesDbModel(conn=self.conn).generate_client_data(
            platform_id=platform_id,
            opportunity_guid=opportunity_guid
        )
