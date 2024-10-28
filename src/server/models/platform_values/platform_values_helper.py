import math
import statistics

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.environment.runtime_constants import rr
from src.server.models.platform_values.platform_values_audience import PlatformValuesAudienceStats
from src.server.models.tags.tags_constants import TagsConstants
from src.server.models.user.user_obfuscator import UserObfuscator


class PlatformValuesHelper:

    CALC_AUDIENCES_ANGLES = "audiences_angles"
    CALC_COMPETITORS_FOR_AUDIENCE_ANGLE = "competitors_for_audience_angle"
    CALC_PRODUCT_NODES_AUDIENCES_TS = "product_nodes_audiences_ts"
    CALC_GAPS_SEARCH_OPPORTUNITIES = "gaps_search_opportunities"

    POTENTIAL_DOWNLOADS_MOBILE = 2848 * 1000 * 1000
    POTENTIAL_DOWNLOADS_PC = 908 * 1000 * 1000
    POTENTIAL_DOWNLOADS_CONSOLE = 630 * 1000 * 1000

    POTENTIAL_DOWNLOADS_PLATFORM_WW = POTENTIAL_DOWNLOADS_MOBILE + POTENTIAL_DOWNLOADS_PC + POTENTIAL_DOWNLOADS_CONSOLE

    POTENTIAL_DOWNLOADS_NA_RATIO = 7.13 / 100
    POTENTIAL_DOWNLOADS_EU_RATIO = 13.27 / 100
    POTENTIAL_DOWNLOADS_LATAM_RATIO = 10.37 / 100
    POTENTIAL_DOWNLOADS_MENA_RATIO = 16.34 / 100
    POTENTIAL_DOWNLOADS_APAC_RATIO = 1 - (POTENTIAL_DOWNLOADS_NA_RATIO + POTENTIAL_DOWNLOADS_EU_RATIO + POTENTIAL_DOWNLOADS_LATAM_RATIO + POTENTIAL_DOWNLOADS_MENA_RATIO)

    def __init__(self, conn: DbConnection):
        self.conn = conn
        self.rows_age_groups = None
        self.potential_downloads_def = None

    @staticmethod
    def get_bin_bytes_cnt(conn: DbConnection):
        return conn.select_one("SELECT cnt FROM platform.bin_bytes_cnt")["cnt"]

    @staticmethod
    def get_temporary() -> str:
        return "TEMPORARY" if PlatformValuesHelper.is_temporary() else ""

    @staticmethod
    def is_temporary() -> bool:
        return True
        if rr.ENV == rr.ENV_DEV:
            return False
        return not rr.is_debug()

    @staticmethod
    def get_calc_version(conn: DbConnection):
        row = conn.select_one_or_none("""
                SELECT value 
                  FROM app.config 
                 WHERE gr = 'platform_calc' 
                   AND id = 'version'
                """)
        if row is None:
            return 0
        version = int(row["value"])
        return version

    @staticmethod
    def threat_score_color(val: int) -> str:
        if val <= 30:
            return "g"
        elif val >= 60:
            return "r"
        return ""

    @staticmethod
    def get_valid_angles(
            conn: DbConnection,
            filter_tags: [int] = None
    ) -> [int]:
        rows_tags = conn.select_all(f"""
        SELECT p.tag_id_int, 
               p.subcategory_int
          FROM platform.def_tags p
          order by p.tag_id_int
        """)

        genres = []
        topics = []

        for row in rows_tags:
            subcategory_int = row["subcategory_int"]
            tag_id_int = row["tag_id_int"]
            if subcategory_int == TagsConstants.SUBCATEGORY_GENRE_ID:
                if filter_tags is None or tag_id_int in filter_tags:
                    genres.append(tag_id_int)
            if subcategory_int == TagsConstants.SUBCATEGORY_TOPICS_ID:
                if filter_tags is None or tag_id_int in filter_tags:
                    topics.append(tag_id_int)

        res = []

        for i in range(len(genres)):
            if genres[i] not in res:
                res.append(genres[i])
            for j in range(i + 1, len(genres)):
                aa_id = PlatformValuesHelper.create_audience_angle_2_comb_id(genres[i], genres[j])
                if aa_id not in res:
                    res.append(aa_id)
        for i in range(len(topics)):
            if topics[i] not in res:
                res.append(topics[i])
            for j in range(i + 1, len(topics)):
                aa_id = PlatformValuesHelper.create_audience_angle_2_comb_id(topics[i], topics[j])
                if aa_id not in res:
                    res.append(aa_id)
            for j in range(len(genres)):
                aa_id = PlatformValuesHelper.create_audience_angle_2_comb_id(topics[i], genres[j])
                if aa_id not in res:
                    res.append(aa_id)

        return res

    @staticmethod
    def create_audience_angle_2_comb_id(tag_id_int_1: int, tag_id_int_2: int):
        if tag_id_int_1 < tag_id_int_2:
            return tag_id_int_1 * 10000 + tag_id_int_2
        else:
            return tag_id_int_2 * 10000 + tag_id_int_1

    @staticmethod
    def recreate_table(conn: DbConnection, table_name: str, query: str, schema="platform_values", temporary=True):
        query = query.replace(f"CREATE TABLE x__table_name__x", f"CREATE TABLE {schema}.x__table_name__x")
        query = query.replace("x__table_name__x", table_name)
        if temporary:
            query = query.replace("CREATE TABLE", f"CREATE {PlatformValuesHelper.get_temporary()} TABLE")
        conn.query(f"drop table if exists {schema}.{table_name}")
        conn.query(query)
        conn.analyze(f"{schema}.{table_name}")

    def __age_to_def(self, age: int):
        if self.rows_age_groups is None:
            self.rows_age_groups = self.conn.select_all("""
                        SELECT g.age_from, 
                               g.age_to, 
                               g.group_name
                          FROM app.def_sheet_platform_age_groups g
                        """)

        for row in self.rows_age_groups:
            if row["age_from"] <= age <= row["age_to"]:
                return {
                    "from": row["age_from"],
                    "to": row["age_to"],
                    "group_name": row["group_name"]
                }
        return None

    @staticmethod
    def get_platforms_from_tag_details(tag_details: []) -> [int]:
        app_platforms = []
        for tag_detail in tag_details:
            if tag_detail["tag_id_int"] == TagsConstants.PLATFORM_PC or tag_detail["tag_id_int"] == TagsConstants.PLATFORM_MOBILE:
                app_platforms.append(tag_detail["tag_id_int"])
        return app_platforms

    def audience_to_client_data_2(
            self,
            platform_id: int,
            data: {},
            app_platforms: [int],
            is_admin=False,
            angle_tags: [int] = None
    ):
        age_def = self.__age_to_def(data["age"])
        female = data["female"]
        female_cnt = data["female_cnt"]
        gender_cnt = female_cnt
        ltv = data["ltv"]
        country = "US"

        if female == 0:
            gender_cnt = data["loved_cnt"] - female_cnt
        gender_ratio = round((gender_cnt / data["loved_cnt"]) * 100)

        audience_stats = PlatformValuesAudienceStats(
            loved_survey_cnt=data["loved_cnt"],
            loved_ratio_ext=data["loved_ratio_ext"],
            rejected_ratio_ext=data["rejected_ratio_ext"],
            total_survey_cnt=data["total_cnt"],
            rejected_survey_cnt=data["rejected_cnt"],
            potential_downloads=data["potential_downloads"],
            loyalty_installs=data["loyalty_installs"],
            app_platforms=app_platforms,
            is_admin=is_admin,
            installs=data["installs"]
        )

        return {
            "platform_id": platform_id,
            "row_id": data["row_id"],
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
            UserObfuscator.TAG_IDS_INT: angle_tags if angle_tags is not None else [],
            "tam": round(audience_stats.total_audience * float(data["arpu"])),
            "total_audience": data["total_audience"]
        }

    @staticmethod
    def calc_ts(ts_arr: [float]) -> int:

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

    @staticmethod
    def get_app_platforms(tag_details: []) -> [int]:
        res = []

        for it in tag_details:
            if it["tag_id_int"] == TagsConstants.PLATFORM_PC or it["tag_id_int"] == TagsConstants.PLATFORM_MOBILE:
                res.append(it["tag_id_int"])

        return res
