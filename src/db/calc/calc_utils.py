import math
import statistics

from server_core.db.db_connection import DbConnection
from src.db.base.base_db_model import BaseDbModel
from src.db.calc.helpers.audience_stats import AudienceStats
from src.models.user_obfuscator import UserObfuscator


class CalcUtils(BaseDbModel):

    CALC_AUDIENCES_ANGLES = "audiences_angles"
    CALC_COMPETITORS_FOR_AUDIENCE_ANGLE = "competitors_for_audience_angle"
    CALC_PRODUCT_NODES_AUDIENCES_TS = "product_nodes_audiences_ts"
    CALC_GAPS_SEARCH_OPPORTUNITIES = "gaps_search_opportunities"
    CALC_GAPS_AUDIENCES_TS = "gaps_audiences_ts"
    
    POTENTIAL_DOWNLOADS_MOBILE_WW = "potential_downloads_mobile_ww"
    
    def __init__(self, conn: DbConnection):
        super(CalcUtils, self).__init__(conn=conn)
        self.rows_age_groups = None
        self.potential_downloads = None

    def get_bin_bytes_cnt(self):
        return self.conn.select_one("SELECT cnt FROM gb_platform.bin_bytes_cnt")["cnt"]

    def get_potential_downloads(self) -> int:

        row = self.conn.select_one("""
        SELECT CAST(c.v1 as UNSIGNED) as potential_downloads
          FROM gb_app.def_sheet_platform_values_constants c
         WHERE c.param = %s
        """, [CalcUtils.POTENTIAL_DOWNLOADS_MOBILE_WW])

        return int(row["potential_downloads"])

    @staticmethod
    def get_temporary() -> str:
        return "TEMPORARY" if CalcUtils.is_temporary() else ""

    @staticmethod
    def is_temporary() -> bool:
        # return False
        return True

    def get_calc_version(self):
        row = self.conn.select_one_or_none("""
                SELECT value 
                  FROM gb_app.config 
                 WHERE gr = 'platform_calc' 
                   AND id = 'version'
                """)
        if row is None:
            return 0
        version = int(row["value"])
        return version

    def get_loved_ratio_fix(self) -> dict:
        rows = self.conn.select_all("""
            SELECT tag_id_int, loved_ratio FROM gb_platform.ext_loved_ratio
            """)
        res = {}
        for row in rows:
            res[row["tag_id_int"]] = float(row["loved_ratio"])
        return res

    @staticmethod
    def threat_score_color(val: int) -> str:
        if val <= 30:
            return "g"
        elif val >= 60:
            return "r"
        return ""

    @staticmethod
    def create_audience_angle_2_comb_id(tag_id_int_1: int, tag_id_int_2: int):
        if tag_id_int_1 < tag_id_int_2:
            return tag_id_int_1 * 10000 + tag_id_int_2
        else:
            return tag_id_int_2 * 10000 + tag_id_int_1

    def recreate_table(self, table_name: str, query: str, temporary=True):
        query = query.replace("x__table_name__x", table_name)
        if temporary:
            query = query.replace("CREATE TABLE", f"CREATE {CalcUtils.get_temporary()} TABLE")
        self.conn.query(f"drop table if exists gb_calc.{table_name}")
        self.conn.query(query)
        self.conn.analyze(f"platform_values.{table_name}")

    def __age_to_def(self, age: int):
        if self.rows_age_groups is None:
            self.rows_age_groups = self.conn.select_all("""
                        SELECT g.age_from, 
                               g.age_to, 
                               g.group_name
                          FROM gb_app.def_sheet_platform_age_groups g
                        """)

        for row in self.rows_age_groups:
            if row["age_from"] <= age <= row["age_to"]:
                return {
                    "from": row["age_from"],
                    "to": row["age_to"],
                    "group_name": row["group_name"]
                }
        return None

    def audience_to_client_data_2(
            self,
            platform_id: int,
            loved_cnt: int,
            data: dict,
            potential_downloads: int | None = None
    ):
        age_def = self.__age_to_def(data["age"])
        
        if age_def is None:
            raise Exception('Age def not defined')
        
        female = data["female"]
        female_cnt = data["female_cnt"]
        gender_cnt = female_cnt
        ltv = data["ltv"]
        country = "US"

        if female == 0:
            gender_cnt = loved_cnt - female_cnt
        gender_ratio = round((gender_cnt / loved_cnt) * 100)

        if potential_downloads is None:
            if self.potential_downloads is None:
                self.potential_downloads = self.get_potential_downloads()
            potential_downloads = self.potential_downloads

        audience_stats = AudienceStats(
            loved_survey_cnt=loved_cnt,
            total_survey_cnt=data["total_cnt"],
            rejected_survey_cnt=data["rejected_cnt"],
            potential_downloads=potential_downloads,
            loyalty_installs=data["loyalty_installs"]
        )

        return {
            "platform_id": platform_id,
            UserObfuscator.AUDIENCE_ANGLE_ID_INT: data["audience_angle_id"],
            "audience_stats": audience_stats.generate_client_data(),
            "age_interval": {
                "from": age_def["from"],
                "to": age_def["to"],
                "group_name": age_def["group_name"]
            },
            "female": female,
            "gender_ratio": gender_ratio,
            "ltv": ltv,
            "country": country,
            UserObfuscator.TAG_IDS_INT: [],
            "tam": round(audience_stats.total_audience * float(data["arpu"])),
        }

    @staticmethod
    def calc_ts(ts_arr: list[float]) -> int:

        if len(ts_arr) == 0:
            return 0

        ts_arr.sort()

        avg_arr = ts_arr[-3:]
        avg_top_3 = 0
        for v in avg_arr:
            avg_top_3 += v
        avg_top_3 = avg_top_3 / len(avg_arr)

        median = statistics.median(ts_arr)

        # ROUND(MAX(1,AVERAGE(TOP3_TS))*(1+(LOG(COMPETITORS_COUNT)*LOG(MAX(1,MEDIAN(TOP_X_VISIBLE_TS))))/4))

        ts_final = round(
            max(1.0, avg_top_3) * (
                    1.0 + (math.log10(len(ts_arr)) * math.log10(max(1.0, median))) / 4))

        return ts_final
