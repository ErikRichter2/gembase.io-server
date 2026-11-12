from math import log

from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.calc.cache.queries.cache_apps_query_db_model import CacheAppsQueryDbModel
from src.db.calc.cache.queries.cache_tags_query_db_model import CacheTagsQueryDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.calc_db_model import CalcDbModel


class CalcCacheDbModel(CalcDbModel):

    def __init__(self, conn: DbConnection):
        super(CalcCacheDbModel, self).__init__(conn=conn)

    def process(self, run_tam_changed=False, run_tam_full=False):
        #############################################
        # gb_platform.def_tags                         #
        #############################################

        self.conn.query_safe("DROP TABLE IF EXISTS gb_platform.def_tags")
        self.conn.query("""
            CREATE TABLE gb_platform.def_tags (
                tag_id_int INT unsigned NOT NULL,
                is_survey TINYINT UNSIGNED NOT NULL,
                subcategory_int int unsigned not null,
                competitors_pool_w int unsigned not null,
                CONSTRAINT platform_def_tags_pk
                    PRIMARY KEY (tag_id_int)
            )
            SELECT p.tag_id_int, 
                   p.is_survey,
                   p.subcategory_int,
                   p.competitors_pool_w
              FROM gb_app.def_sheet_platform_product p
             WHERE p.is_prompt = 1
            """)
        self.conn.analyze("platform.def_tags")

        #############################################
        # gb_platform.platform_values_apps             #
        #############################################

        CacheAppsQueryDbModel(conn=self.conn).run()

        #############################################
        # gb_platform.platform_values_devs_apps        #
        #############################################

        self.conn.query_safe("DROP TABLE IF EXISTS gb_platform.platform_values_devs_apps")
        self.conn.query("""
            CREATE TABLE gb_platform.platform_values_devs_apps (
                dev_id_int MEDIUMINT NOT NULL,
                app_id_int MEDIUMINT NOT NULL,
                CONSTRAINT platform_values_devs_apps_pk
                    PRIMARY KEY (dev_id_int, app_id_int)
            )
            SELECT da.dev_id_int, a.app_id_int
              FROM gb_scraped_data.devs_apps da,
                   gb_platform.platform_values_apps a
             WHERE da.app_id_int = a.app_id_int
               AND da.primary_dev = 1
            """)
        self.conn.analyze("platform.platform_values_devs_apps")

        #############################################
        # gb_platform.platform_values_tags             #
        #############################################

        CacheTagsQueryDbModel(conn=self.conn).run_tags()

        #############################################
        # gb_platform.platform_values_survey_tags      #
        #############################################

        query = f"""
            create table gb_platform.platform_values_survey_tags
            (
                survey_meta_id SMALLINT UNSIGNED NOT NULL,
                survey_instance_int mediumint unsigned not null,
                tag_id_int smallint unsigned not null,
                tag_value   TINYINT unsigned not NULL,
                loved TINYINT UNSIGNED NOT NULL,
                hated TINYINT UNSIGNED NOT NULL,
                constraint platform_values_survey_tags_pk
                    primary key (survey_meta_id, survey_instance_int, tag_id_int)
            )
            SELECT t.survey_meta_id, 
                   si.id as survey_instance_int, 
                   p.tag_id_int, 
                   t.tag_value,
                   IF(t.tag_value = 100, 1, 0) AS loved,
                   IF(t.tag_value < 50, 1, 0) AS hated
              FROM gb_survey_data.survey_tags t,
                   gb_survey_data.survey_info si,
                   gb_app.def_sheet_platform_product p,
                   gb_app.map_tag_id m
             WHERE si.survey_instance = t.survey_instance
               AND si.survey_meta_id = t.survey_meta_id
               AND p.tag_id_int = m.id
               AND m.tag_id = t.tag_id
            """
        self.conn.query_safe("DROP TABLE IF EXISTS gb_platform.platform_values_survey_tags")
        self.conn.query(query)
        self.conn.analyze("platform.platform_values_survey_tags")

        #############################################
        # gb_platform.platform_values_survey_flags     #
        #############################################

        query = f"""
            create table gb_platform.platform_values_survey_flags
            (
                survey_id SMALLINT UNSIGNED NOT NULL,
                survey_instance_int mediumint unsigned not null,
                tag_id_int smallint unsigned not null,
                constraint platform_values_survey_tags_pk
                    primary key (survey_id, survey_instance_int, tag_id_int)
            )
            SELECT t.survey_id, 
                   si.id as survey_instance_int, 
                   p.tag_id_int
              FROM gb_survey_data.survey_flags t,
                   gb_survey_data.survey_info si,
                   gb_app.def_sheet_platform_product p,
                   gb_app.map_tag_id m
             WHERE si.survey_instance = t.survey_instance
               AND si.survey_meta_id = t.survey_id
               AND p.tag_id_int = m.id
               AND m.tag_id = t.tag_id
            """
        self.conn.query_safe("DROP TABLE IF EXISTS gb_platform.platform_values_survey_flags")
        self.conn.query(query)
        self.conn.analyze("platform.platform_values_survey_flags")

        #############################################
        # gb_platform.platform_values_survey_info      #
        #############################################

        query = f"""
            create table gb_platform.platform_values_survey_info
            (
                survey_meta_id SMALLINT UNSIGNED NOT NULL,
                survey_instance_int mediumint unsigned not null,
                age smallint unsigned not null,
                female TINYINT unsigned not NULL,
                spending SMALLINT UNSIGNED NOT NULL,
                favorite_app_id_int int unsigned not null,
                constraint platform_values_survey_info_pk
                    primary key (survey_meta_id, survey_instance_int)
            )
            SELECT si.survey_meta_id, 
                   si.id as survey_instance_int, 
                   si.age,
                   IF (si.gender = 'f', 1, 0) AS female,
                   si.spending,
                   IF(m.app_id_int IS NULL, 0, m.app_id_int) as favorite_app_id_int
              FROM gb_survey_data.survey_info si
              LEFT JOIN gb_app.map_app_id_to_store_id m
                ON m.app_id_in_store = si.favorite_game
            """
        self.conn.query_safe("DROP TABLE IF EXISTS gb_platform.platform_values_survey_info")
        self.conn.query(query)
        self.conn.analyze("platform.platform_values_survey_info")

        ###############################################
        # gb_platform.platform_values_survey_competitors #
        ###############################################

        query = f"""
                    create table gb_platform.platform_values_survey_competitors
                    (
                        survey_meta_id SMALLINT UNSIGNED NOT NULL,
                        survey_instance_int mediumint unsigned not null,
                        app_id_int int unsigned not null,
                        tag_value int unsigned not null,
                        constraint platform_values_survey_info_pk
                            primary key (survey_meta_id, survey_instance_int, app_id_int)
                    )
                    SELECT si.survey_meta_id, 
                           si.id as survey_instance_int, 
                           m.app_id_int,
                           25 * (sc.value - 1) as tag_value
                      FROM gb_survey_data.survey_info si
                      JOIN gb_survey_data.survey_competitors sc
                        ON sc.survey_instance = si.survey_instance
                       AND sc.survey_meta_id = si.survey_meta_id
                      JOIN gb_app.map_app_id_to_store_id m
                        ON m.app_id_in_store = sc.app_id
                    """
        self.conn.query_safe("DROP TABLE IF EXISTS gb_platform.platform_values_survey_competitors")
        self.conn.query(query)
        self.conn.analyze("platform.platform_values_survey_competitors")

        ###############################################
        # gb_platform.platform_values_apps_tags_per_subc #
        ###############################################

        self.conn.query("DROP TABLE IF EXISTS gb_platform.platform_values_apps_tags_per_subc")
        self.conn.query("""
            CREATE TABLE gb_platform.platform_values_apps_tags_per_subc (
                app_id_int MEDIUMINT NOT NULL,
                subcategory_int SMALLINT UNSIGNED NOT NULL,
                cnt SMALLINT UNSIGNED NOT NULL,
                CONSTRAINT platform_values_apps_tags_per_subc_pk
                    PRIMARY KEY (app_id_int, subcategory_int)
            )
            SELECT a.app_id_int,
                   p.subcategory_int,
                   COUNT(1) AS cnt
              FROM gb_platform.platform_values_apps a,
                   gb_platform.platform_values_tags t,
                   gb_app.def_sheet_platform_product p
             WHERE a.app_id_int = t.app_id_int
               AND t.tag_id_int = p.tag_id_int
             GROUP BY 
                   a.app_id_int, 
                   p.subcategory_int
            """)
        self.conn.commit()
        self.conn.analyze("platform.platform_values_apps_tags_per_subc")

        ##################################################
        # gb_platform.platform_values_installs_arpu_per_tag #
        ##################################################

        self.conn.query("DROP TABLE IF EXISTS gb_calc.tmp_arpu_per_tag")
        self.conn.query("""
            CREATE TABLE gb_calc.tmp_arpu_per_tag (
                tag_id_int SMALLINT UNSIGNED NOT NULL,
                app_id_int MEDIUMINT NOT NULL,
                downloads_ww_row_num MEDIUMINT UNSIGNED NOT NULL,
                CONSTRAINT tmp_arpu_per_tag_pk
                    PRIMARY KEY (tag_id_int, app_id_int)
            )
            SELECT p.tag_id_int, 
                   a.app_id_int,
                   ROW_NUMBER() OVER (PARTITION BY p.tag_id_int ORDER BY a.installs DESC) AS downloads_ww_row_num
              FROM gb_platform.def_tags p
             INNER JOIN gb_platform.platform_values_tags t ON t.tag_id_int = p.tag_id_int
             INNER JOIN gb_platform.platform_values_apps a ON a.app_id_int = t.app_id_int
            """)

        self.conn.commit()

        self.conn.query("DROP TABLE IF EXISTS gb_platform.platform_values_installs_arpu_per_tag")
        self.conn.query("""
            CREATE TABLE gb_platform.platform_values_installs_arpu_per_tag (
                tag_id_int SMALLINT UNSIGNED NOT NULL,
                installs BIGINT UNSIGNED NOT NULL,
                loyalty_installs BIGINT UNSIGNED NOT NULL,
                arpu DECIMAL(8, 4) NOT NULL,
                CONSTRAINT platform_values_installs_arpu_per_tag_pk
                    PRIMARY KEY (tag_id_int)
            )
            SELECT p.tag_id_int,
                   SUM(a.installs) AS installs,
                   SUM(a.loyalty_installs) AS loyalty_installs,
                   IF(z2.arpu_ext IS NULL, IF(z2.arpu_ww IS NULL, 0, z2.arpu_ww), z2.arpu_ext) as arpu
              FROM gb_platform.def_tags p
             INNER JOIN gb_platform.platform_values_tags t ON t.tag_id_int = p.tag_id_int
             INNER JOIN gb_platform.platform_values_apps a ON a.app_id_int = t.app_id_int
              LEFT JOIN (
                         SELECT p.tag_id_int,
                                IF(z_ww.arpu_ww IS NULL, 0, z_ww.arpu_ww) AS arpu_ww,
                                apt.arpu as arpu_ext
                           FROM gb_platform.def_tags p
                           LEFT JOIN (
                                      SELECT t_ww.tag_id_int,
                                             ROUND(SUM(a_ww.revenues_gp_ww) / SUM(a_ww.installs), 4) AS arpu_ww
                                        FROM gb_calc.tmp_arpu_per_tag t_ww,
                                             gb_platform.platform_values_apps a_ww
                                       WHERE t_ww.app_id_int = a_ww.app_id_int
                                         AND t_ww.downloads_ww_row_num <= 3
                                       GROUP BY t_ww.tag_id_int
                                     ) z_ww 
                                  ON z_ww.tag_id_int = p.tag_id_int
                           LEFT JOIN external_data.arpu_per_tag apt
                                  ON apt.tag_id_int = p.tag_id_int
                         ) z2 
                      ON z2.tag_id_int = p.tag_id_int
             GROUP BY p.tag_id_int
            """)
        self.conn.query("DROP TABLE IF EXISTS gb_calc.tmp_arpu_per_tag")
        self.conn.analyze("platform.platform_values_installs_arpu_per_tag")

        ##################################################
        # gb_platform.platform_values_survey_data_per_tag   #
        ##################################################

        query = f"""
            CREATE TABLE gb_platform.platform_values_survey_data_per_tag (
                survey_id int unsigned not null,
                tag_id_int int unsigned not null,
                filtered_rows int unsigned not null,
                total_rows int unsigned not null,
                tam bigint unsigned not null,
                constraint platform_values_survey_data_per_tag_pk
                    primary key (survey_id, tag_id_int)
            )
            SELECT st.survey_meta_id as survey_id, 
                   st.tag_id_int,
                   st.filtered_cnt as filtered_rows,
                   st.total_cnt as total_rows,
                   round(GREATEST(0, ROUND(st.filtered_cnt / st.total_cnt * CAST(c.v1 AS UNSIGNED)) - a.loyalty_installs) * a.arpu, 0) as tam
              FROM gb_survey_data.survey_tam_per_tag st,
                   gb_app.def_sheet_platform_values_constants c,
                   gb_platform.platform_values_installs_arpu_per_tag a
             WHERE c.param = '{CalcUtils.POTENTIAL_DOWNLOADS_MOBILE_WW}'
               AND st.tag_id_int = a.tag_id_int
            """
        self.conn.query("drop table if exists gb_platform.platform_values_survey_data_per_tag")
        self.conn.query(query)
        self.conn.analyze("platform.platform_values_survey_data_per_tag")

        ##################################################
        # gb_platform.platform_values_tags_fake_concepts    #
        ##################################################

        query = """
            CREATE TABLE gb_platform.platform_values_tags_fake_concepts (
                survey_id int unsigned not null,
                tag_id_int SMALLINT UNSIGNED NOT NULL,
                fake_concept_id smallint unsigned not null,
                fake_concept_tag_id_int SMALLINT UNSIGNED NOT NULL,
                CONSTRAINT platform_values_fake_concepts_pk
                    PRIMARY KEY (survey_id, tag_id_int, fake_concept_tag_id_int)
            )
            SELECT z4.survey_id,
                   z4.tag_id_int, 
                   0 as fake_concept_id, 
                   z4.fake_concept_tag_id_int
              FROM (
                    SELECT aud.survey_id,
                           z3.tag_id_int, 
                           z3.fake_concept_tag_id_int,
                           ROW_NUMBER() OVER (PARTITION BY aud.survey_id, z3.tag_id_int ORDER BY aud.tam DESC) AS row_num
                      FROM (
                            SELECT z2.tag_id_int,
                                   t.tag_id_int AS fake_concept_tag_id_int
                              FROM (
                                    SELECT z1.tag_id_int, 
                                           z1.app_id_int
                                      FROM (
                                            SELECT p.tag_id_int,
                                                   a.app_id_int,
                                                   ROW_NUMBER() OVER (PARTITION BY p.tag_id_int ORDER BY a.installs DESC) AS row_num
                                              FROM gb_platform.def_tags p,
                                                   gb_platform.platform_values_apps a,
                                                   gb_platform.platform_values_tags t
                                             WHERE t.tag_id_int = p.tag_id_int
                                               AND a.app_id_int = t.app_id_int
                                           ) z1
                                       WHERE z1.row_num <= 20
                                   ) z2,
                                   gb_platform.platform_values_tags t
                             WHERE z2.app_id_int = t.app_id_int
                             GROUP BY 
                                   z2.tag_id_int, 
                                   t.tag_id_int
                           ) z3,
                           gb_platform.platform_values_survey_data_per_tag aud
                     WHERE z3.fake_concept_tag_id_int = aud.tag_id_int
                   ) z4
             WHERE z4.row_num <= 3
            """
        self.conn.query("drop table if exists gb_platform.platform_values_tags_fake_concepts")
        self.conn.query(query)
        self.conn.analyze("platform.platform_values_tags_fake_concepts")

        rows = self.conn.select_all("""
            select survey_id,
                   tag_id_int, 
                   fake_concept_tag_id_int
              from gb_platform.platform_values_tags_fake_concepts
            """)

        tags_ids_per_tag_id_per_survey_id = {}
        for row in rows:
            tag_id_int = row["tag_id_int"]
            survey_id = row["survey_id"]
            if survey_id not in tags_ids_per_tag_id_per_survey_id:
                tags_ids_per_tag_id_per_survey_id[survey_id] = {}
            if tag_id_int not in tags_ids_per_tag_id_per_survey_id[survey_id]:
                tags_ids_per_tag_id_per_survey_id[survey_id][tag_id_int] = []
            tags_ids_per_tag_id_per_survey_id[survey_id][tag_id_int].append(row["fake_concept_tag_id_int"])

        unique_tags_ids_per_survey_id = {}

        def get_unique_id(survey_id: int, tags_ids: []) -> int:
            if survey_id not in unique_tags_ids_per_survey_id:
                unique_tags_ids_per_survey_id[survey_id] = []
            for i in range(len(unique_tags_ids_per_survey_id[survey_id])):
                if Utils.compare_arr(tags_ids, unique_tags_ids_per_survey_id[survey_id][i]):
                    return i + 1
            unique_tags_ids_per_survey_id[survey_id].append(tags_ids)
            return len(unique_tags_ids_per_survey_id[survey_id])

        unique_tags_ids_per_tag_id_per_survey_id = {}
        for survey_id in tags_ids_per_tag_id_per_survey_id:
            unique_tags_ids_per_tag_id_per_survey_id[survey_id] = {}
            for tag_id_int in tags_ids_per_tag_id_per_survey_id[survey_id]:
                unique_tags_ids_per_tag_id_per_survey_id[survey_id][tag_id_int] = get_unique_id(survey_id,
                                                                                                tags_ids_per_tag_id_per_survey_id[
                                                                                                    survey_id][
                                                                                                    tag_id_int])

        arr = []
        for survey_id in unique_tags_ids_per_tag_id_per_survey_id:
            for tag_id_int in unique_tags_ids_per_tag_id_per_survey_id[survey_id]:
                arr.append(
                    f"WHEN survey_id = {survey_id} AND tag_id_int = {tag_id_int} THEN {unique_tags_ids_per_tag_id_per_survey_id[survey_id][tag_id_int]}")
            case_when = f"CASE {' '.join(arr)} ELSE fake_concept_id END"

        self.conn.query(f"""
            UPDATE gb_platform.platform_values_tags_fake_concepts
               SET fake_concept_id = {case_when}
            """)
        self.conn.commit()

        ##################################################
        # gb_platform.platform_values_devs                  #
        ##################################################

        self.conn.query("drop table if exists gb_platform.platform_values_devs")
        self.conn.query("""
            CREATE TABLE gb_platform.platform_values_devs (
                dev_id_int int unsigned not null,
                yearly_installs bigint unsigned not null,
                arpu decimal(8, 4) not null,
                yearly_revenues bigint unsigned not null,
                constraint platform_values_devs_pk
                    primary key (dev_id_int)
            )
            SELECT z2.dev_id_int,
                   max(yearly_installs) as yearly_installs,
                   round(AVG(z2.arpu), 4) AS arpu,
                   round(SUM(z2.yearly_installs * z2.arpu) * 12) AS yearly_revenues
              FROM (
                    SELECT z1.dev_id_int,
                           MAX(z1.yearly_installs) AS yearly_installs,
                           AVG(z1.arpu) AS arpu
                      FROM (
                            SELECT da.dev_id_int,
                                   a.app_id_int,
                                   ROUND(IF (a.released_years = 0, a.installs, a.installs / a.released_years), 0) AS yearly_installs,
                                   ta.arpu,
                                   ROW_NUMBER() OVER (PARTITION BY da.dev_id_int, a.app_id_int ORDER BY ta.arpu) AS arpu_row_num,
                                   COUNT(1) OVER (PARTITION BY da.dev_id_int, a.app_id_int) AS arpu_total_num
                              FROM gb_platform.platform_values_devs_apps da,
                                   gb_platform.platform_values_apps a,
                                   gb_platform.platform_values_tags t,
                                   gb_platform.platform_values_installs_arpu_per_tag ta
                             WHERE da.app_id_int = a.app_id_int
                               AND a.app_id_int = t.app_id_int
                               AND ta.tag_id_int = t.tag_id_int
                               AND t.tag_rank != 0
                           ) z1
                     WHERE z1.arpu_row_num IN (FLOOR((z1.arpu_total_num + 1) / 2), FLOOR((z1.arpu_total_num + 2) / 2) )
                     GROUP BY 
                           z1.dev_id_int, 
                           z1.app_id_int
                   ) z2
                   GROUP BY z2.dev_id_int
            """)
        self.conn.analyze("platform.platform_values_devs")

        self.process_bit_values()

        self.__create_audience_angles()
        self.__create_survey_tags_w()
        self.__create_fake_concepts_tam()

        if run_tam_full:
            self.__create_tam_per_app(skip_rejected=False)
        elif run_tam_changed:
            self.create_tam_per_app_for_changed_apps()

    def clear_old_result_data(self):
        self.conn.query("""
            DELETE FROM gb_calc.requests r WHERE DATE_ADD(r.t_start, INTERVAL 2 WEEK) < NOW()
            """)
        self.conn.commit()

        def delete_for_table(table_name: str):
            self.conn.query(f"""
                    DELETE FROM {table_name} rr
                      WHERE rr.platform_id NOT IN (
                          SELECT r.platform_id FROM gb_calc.requests r
                      )
                    """)
            self.conn.commit()

        delete_for_table("platform_values.results_audience_angles__competitors")
        delete_for_table("platform_values.results_audience_angles__final")
        delete_for_table("platform_values.results_audience_angles__top_tags")
        delete_for_table("platform_values.results_audience_angle__rejected")
        delete_for_table("platform_values.results_gaps__nodes_ts")
        delete_for_table("platform_values.results_gaps__opportunities")
        delete_for_table("platform_values.results_gaps__opportunities_tags")
        delete_for_table("platform_values.results_product_nodes__most_disliked")
        delete_for_table("platform_values.results__rejected_per_audience_angle")
        delete_for_table("platform_values.results__ts_per_audience_angle")

        self.conn.commit()

    def process_bit_values(self):

        rows_tags = self.conn.select_all("""
            SELECT p.tag_id_int, 
                   p.competitors_pool_w
              FROM gb_platform.def_tags p
             ORDER BY p.tag_id_int
            """)

        varbinary_per_tag_id = {}

        def bytes_needed(n):
            if n == 0:
                return 1
            return int(log(n, 256)) + 1

        bin_str = ""
        for j in range(len(rows_tags) + 1):
            bin_str += "0"
        bin_str = "1" + bin_str
        bin_int = int(bin_str, 2)
        bin_bytes_cnt = bytes_needed(bin_int)

        zero_bin_str = ""
        for j in range(len(rows_tags)):
            zero_bin_str += "0"

        for i in range(len(rows_tags)):
            tag_id_int = rows_tags[i]["tag_id_int"]
            bin_str = ""
            for j in range(len(rows_tags)):
                bin_str += "0"
            bin_str = bin_str[:len(rows_tags) - i] + "1" + bin_str[len(rows_tags) - i:]
            varbinary_per_tag_id[tag_id_int] = bin_str

        self.conn.query("DROP TABLE IF EXISTS gb_platform.zero_bin_value")
        self.conn.query(f"""
            CREATE TABLE gb_platform.zero_bin_value (
                b BINARY({bin_bytes_cnt}) not null
            )
            SELECT b'{zero_bin_str}' as b
            """)
        self.conn.commit()
        self.conn.analyze("platform.zero_bin_value")

        self.conn.query("DROP TABLE IF EXISTS gb_platform.bin_bytes_cnt")
        self.conn.query(f"""
                    CREATE TABLE gb_platform.bin_bytes_cnt (
                        cnt int unsigned not null
                    )
                    SELECT {bin_bytes_cnt} as cnt
                    """)
        self.conn.commit()
        self.conn.analyze("platform.bin_bytes_cnt")

        self.conn.query("DROP TABLE IF EXISTS gb_platform.def_tags_bin")
        self.conn.query(f"""
            CREATE TABLE gb_platform.def_tags_bin (
                tag_id_int smallint unsigned not null,
                b BINARY({bin_bytes_cnt}) not null,
                constraint def_tags_bin_pk
                    primary key (tag_id_int)
            )
            """)
        bulk_data_v = []
        for tag_id_int in varbinary_per_tag_id:
            bulk_data_v.append((tag_id_int, varbinary_per_tag_id[tag_id_int]))
        self.conn.bulk("""
            INSERT INTO gb_platform.def_tags_bin (tag_id_int, b) VALUES (%s, b%s)
            """, bulk_data_v)
        self.conn.commit()
        self.conn.analyze("platform.def_tags_bin")

        query = f"""
                    CREATE TABLE gb_platform.def_tags_subc_bin (
                        subcategory_int SMALLINT UNSIGNED NOT NULL,
                        b BINARY({bin_bytes_cnt}) NOT NULL,
                        competitors_pool_w smallint unsigned not null,
                        CONSTRAINT def_tags_subc_bin_pk
                            PRIMARY KEY (subcategory_int)
                    )
                    SELECT p.subcategory_int, 
                           BIT_OR(d.b) AS b,
                           p.competitors_pool_w
                      FROM gb_platform.def_tags_bin d,
                           gb_platform.def_tags p
                     WHERE d.tag_id_int = p.tag_id_int
                       AND p.competitors_pool_w != 0
                     GROUP BY p.subcategory_int, p.competitors_pool_w
                    """
        self.conn.query("drop table if exists gb_platform.def_tags_subc_bin")
        self.conn.query(query)
        self.conn.analyze("platform.def_tags_subc_bin")

        PlatformValuesTags.run_tags_bin(conn=self.conn)

        self.conn.query("DROP TABLE if exists gb_platform.platform_values_survey_tags_bin")
        self.conn.query(f"""
            CREATE TABLE gb_platform.platform_values_survey_tags_bin (
                survey_id smallint NOT NULL,
                survey_instance_int INT UNSIGNED NOT NULL,
                b BINARY({bin_bytes_cnt}) NOT NULL,
                b_hated BINARY({bin_bytes_cnt}) NOT NULL,
                b_loved BINARY({bin_bytes_cnt}) NOT NULL,
                CONSTRAINT platform_values_survey_tags_bin_pk
                    PRIMARY KEY (survey_id, survey_instance_int)
            )
            SELECT st.survey_meta_id AS survey_id,
                   st.survey_instance_int,
                   BIT_OR(tb.b) AS b,
                   BIT_OR(IF(st.hated = 1, tb.b, zero.b)) AS b_hated,
                   BIT_OR(IF(st.loved = 1, tb.b, zero.b)) AS b_loved
              FROM gb_platform.def_tags_bin tb,
                   gb_platform.platform_values_survey_tags st,
                   gb_platform.zero_bin_value zero
             WHERE tb.tag_id_int = st.tag_id_int
             GROUP BY  
                   st.survey_meta_id, 
                   st.survey_instance_int
            """)
        self.conn.analyze("platform.platform_values_survey_tags_bin")

        self.conn.query("drop table if exists gb_platform.platform_values_tags_fake_concepts_bin")
        self.conn.query(f"""
            create table gb_platform.platform_values_tags_fake_concepts_bin (
                survey_id int unsigned not null,
                tag_id_int int unsigned not null,
                b BINARY({bin_bytes_cnt}) not null,
                constraint platform_values_tags_fake_concepts_bin_pk
                primary key (survey_id, tag_id_int)
            )
            SELECT f.survey_id, f.tag_id_int, BIT_OR(d.b) as b
              FROM gb_platform.platform_values_tags_fake_concepts f,
                   gb_platform.def_tags_bin d
             WHERE f.fake_concept_tag_id_int = d.tag_id_int
             GROUP BY f.survey_id, f.tag_id_int
            """)
        self.conn.analyze("platform.platform_values_tags_fake_concepts_bin")

    def __create_audience_angles(self):

        bin_bytes_cnt = self.calc_utils.get_bin_bytes_cnt()

        self.conn.query("drop table if exists gb_platform.audience_angle_tags")
        self.conn.query("""
        CREATE TABLE gb_platform.audience_angle_tags (
            id bigint unsigned not null,
            tag_id_int int unsigned not null,
            constraint audience_angle_tags_pk
                primary key (id, tag_id_int)
        )
        SELECT d.tag_id_int as id, 
               d.tag_id_int
          FROM gb_platform.def_tags_bin d
         UNION ALL
        SELECT d1.tag_id_int * 10000 + d2.tag_id_int as id, 
               d1.tag_id_int
          FROM gb_platform.def_tags_bin d1,
               gb_platform.def_tags_bin d2
         WHERE d1.tag_id_int < d2.tag_id_int
         UNION ALL
        SELECT d1.tag_id_int * 10000 + d2.tag_id_int as id, 
               d2.tag_id_int
          FROM gb_platform.def_tags_bin d1,
               gb_platform.def_tags_bin d2
         WHERE d1.tag_id_int < d2.tag_id_int
        """)

        self.conn.query("drop table if exists gb_platform.audience_angle")
        self.conn.query(f"""
        CREATE TABLE gb_platform.audience_angle (
            id bigint unsigned not null,
            angle_cnt tinyint unsigned not null,
            b binary({bin_bytes_cnt}) not null,
            loyalty_installs bigint unsigned not null,
            arpu DECIMAL(8,4) not null,
            constraint audience_angle_pk
                primary key (id)
        )
        SELECT t.id, 
               COUNT(1) as angle_cnt,
               BIT_OR(d.b) as b,
               0 as loyalty_installs,
               0 as arpu
          FROM gb_platform.audience_angle_tags t,
               gb_platform.def_tags_bin d
         WHERE t.tag_id_int = d.tag_id_int
         GROUP BY t.id
        """)

        self.__update_audience_angle_loyalty_installs()
        self.__update_audience_angle_arpu()
        self.__create_audience_angle_potential()

    def __update_audience_angle_loyalty_installs(self):
        self.conn.query("""
        UPDATE gb_platform.audience_angle aa
        INNER JOIN (
            SELECT aaa.id, 
                   SUM(a.loyalty_installs) AS loyalty_installs
              FROM gb_platform.audience_angle aaa,
                   gb_platform.platform_values_tags_bin tb,
                   gb_platform.platform_values_apps a
             WHERE bit_count(tb.b & aaa.b) = aaa.angle_cnt
               AND a.app_id_int = tb.app_id_int
             GROUP BY aaa.id
        ) z1
        ON z1.id = aa.id
        SET aa.loyalty_installs = z1.loyalty_installs
        """)
        self.conn.commit()
        self.conn.analyze("platform.audience_angle")

    # survey change, tags def change
    # 26s
    def __create_audience_angle_potential(self):
        self.conn.query("drop table if exists gb_platform.audience_angle_potential")
        self.conn.query(f"""
        CREATE TABLE gb_platform.audience_angle_potential (
            id bigint unsigned not null,
            survey_id int unsigned not null,
            loved_cnt int unsigned default 0 not null,
            total_cnt int unsigned default 0 not null,
            constraint audience_angle_potential_pk
                primary key (id, survey_id)
        )
        SELECT t.id, 
               stb.survey_id, 
               COUNT(1) as loved_cnt, 
               total_cnt.total_cnt
          FROM gb_platform.audience_angle t,
               gb_platform.platform_values_survey_tags_bin stb,
               (
                   SELECT si.survey_meta_id as survey_id, 
                          COUNT(1) as total_cnt
                     FROM gb_platform.platform_values_survey_info si
                    GROUP BY si.survey_meta_id
               ) total_cnt
         WHERE BIT_COUNT(stb.b_loved & t.b) = t.angle_cnt
           AND stb.survey_id = total_cnt.survey_id
         GROUP BY 
               t.id, 
               total_cnt.survey_id, 
               total_cnt.total_cnt
        """)
        self.conn.analyze("platform.audience_angle_potential")
        self.__update_audience_angle_tam()

    # 5 sec
    def __update_audience_angle_tam(self):
        self.conn.query("drop table if exists gb_platform.audience_angle_tam_per_angle")
        self.conn.query(f"""
        CREATE TABLE gb_platform.audience_angle_tam_per_angle (
            id int unsigned not null,
            survey_id int unsigned not null,
            total_audience bigint unsigned not null,
            tam bigint unsigned not null,
            constraint audience_angle_tam_per_angle_pk
                primary key (id, survey_id)
        )
        SELECT aa.id,
               aa.survey_id,
               0 as total_audience,
               0 as tam
          FROM gb_platform.audience_angle_potential aa
        """)

        self.conn.query(f"""
        UPDATE gb_platform.audience_angle_tam_per_angle tam
        INNER JOIN (
        SELECT gb_platform.calc_tam(
                       z1.total_audience,
                       z1.arpu
                   ) as tam,
                   z1.id,
                   z1.survey_id,
                   z1.arpu,
                   z1.total_audience
          FROM (
            SELECT aap.id, 
                   aap.survey_id, 
                   aa.arpu,
                   gb_platform.calc_total_audience(
                           aap.loved_cnt,
                           aap.total_cnt,
                           0,
                           CAST(c.v1 as UNSIGNED),
                           aa.loyalty_installs, 0, 0
                       ) as total_audience
              FROM gb_platform.audience_angle aa,
                   gb_platform.audience_angle_potential aap,
                   gb_app.def_sheet_platform_values_constants c
             WHERE aa.id = aap.id
               AND c.param = '{CalcUtils.POTENTIAL_DOWNLOADS_MOBILE_WW}'
            ) z1 ) z2
           ON tam.id = z2.id 
          AND tam.survey_id = z2.survey_id
          SET tam.tam = z2.tam, tam.total_audience = z2.total_audience
        """)
        self.conn.commit()
        self.conn.analyze("platform.audience_angle_tam_per_angle")

    # 5s
    def __update_audience_angle_arpu(self):
        self.conn.query("""
        UPDATE gb_platform.audience_angle ct
        INNER JOIN (
                    SELECT z2.id, 
                           round(avg(z2.arpu), 4) as arpu
                      FROM (
                            SELECT z1.id, 
                                   z1.arpu,
                                   row_number() over (partition by z1.id) as row_num,
                                   count(1) over (partition by z1.id) as total_num
                              FROM (
                                    SELECT ctt.id, 
                                           arpu.arpu
                                      FROM gb_platform.platform_values_installs_arpu_per_tag arpu,
                                           gb_platform.audience_angle_tags ctt
                                     WHERE ctt.tag_id_int = arpu.tag_id_int
                                  ) z1
                           ) z2
                     WHERE z2.row_num IN (FLOOR((z2.total_num + 1) / 2), FLOOR((z2.total_num + 2) / 2) )
                     GROUP BY z2.id
                   ) z3
           ON z3.id = ct.id
          SET ct.arpu = z3.arpu
        """)
        self.conn.commit()
        self.conn.analyze("platform.audience_angle")

    # 2 mins
    def __create_survey_tags_w(self):
        print(f"create_survey_tags_w")

        bin_bytes_cnt = self.calc_utils.get_bin_bytes_cnt()

        self.conn.query("drop table if exists gb_platform.audience_angle_tags_w")
        self.conn.query(f"""
        CREATE TABLE gb_platform.audience_angle_tags_w (
            id INT UNSIGNED NOT NULL,
            survey_id INT UNSIGNED NOT NULL,
            w INT UNSIGNED NOT NULL,
            w_b binary({bin_bytes_cnt}) NOT NULL,
            CONSTRAINT audience_angle_tags_w_pk
                PRIMARY KEY (id, survey_id, w)
        )
        """)

        page = 0
        limit = 1000

        while True:

            print(f"Page: {page}")

            self.calc_utils.recreate_table(
                table_name="tmp_part",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    id bigint unsigned not null,
                    constraint x__table_name__x_pk
                        primary key (id)
                )
                SELECT aap.id
                  FROM gb_platform.audience_angle_potential aap
                 ORDER BY aap.id 
                 LIMIT {page * limit}, {limit}
                """
            )

            cnt = self.conn.select_one("SELECT count(1) AS cnt FROM gb_calc.tmp_part")["cnt"]

            if cnt == 0:
                break

            page += 1

            self.conn.query("""
            INSERT INTO gb_platform.audience_angle_tags_w
            (id, survey_id, w, w_b)
            SELECT z4.id, 
                   z4.survey_id, 
                   z4.w, 
                   BIT_OR(b.b) AS w_b
              FROM (
                    SELECT z3.id, 
                           z3.survey_id, 
                           z3.tag_id_int,
                      CASE WHEN z3.w > 85 THEN 100
                           WHEN z3.w > 65 THEN 75
                           WHEN z3.w > 45 THEN 50
                           WHEN z3.w > 15 THEN 25
                           ELSE 0 END AS w
                      FROM (
                            SELECT z2.id, 
                                   z2.survey_id, 
                                   z2.tag_id_int, 
                                   z2.loved_cnt / p.loved_cnt * 100 as w
                              FROM (
                                    SELECT z1.id, 
                                           z1.survey_id, 
                                           d.tag_id_int, 
                                           COUNT(1) as loved_cnt
                                      FROM (
                                            SELECT DISTINCT 
                                                   part.id, 
                                                   stb.survey_id, 
                                                   stb.survey_instance_int
                                              FROM gb_calc.tmp_part part,
                                                   gb_platform.audience_angle t,
                                                   gb_platform.platform_values_survey_tags_bin stb
                                             WHERE part.id = t.id
                                               AND BIT_COUNT(stb.b_loved & t.b) = t.angle_cnt
                                           ) z1,
                                           gb_platform.platform_values_survey_tags st,
                                           gb_platform.def_tags_bin d
                                     WHERE st.survey_meta_id = z1.survey_id
                                       AND st.survey_instance_int = z1.survey_instance_int
                                       AND st.tag_id_int = d.tag_id_int
                                       AND st.loved = 1
                                     GROUP BY 
                                           z1.id, 
                                           z1.survey_id, 
                                           d.tag_id_int
                                   ) z2,
                                   gb_platform.audience_angle_potential p
                             WHERE z2.id = p.id
                               AND z2.survey_id = p.survey_id
                           ) z3
                   ) z4,
                   gb_platform.def_tags_bin b
             WHERE z4.tag_id_int = b.tag_id_int
               AND z4.w != 0
             GROUP BY z4.id, z4.survey_id, z4.w
            """)
            self.conn.commit()

        self.conn.analyze("platform.audience_angle_tags_w")

        # 40s
    def __create_fake_concepts_tam(self):
        print("create_fake_concepts_tam")

        self.calc_utils.recreate_table(
            table_name="tmp_fake_concepts_rejected_per_angle_2_comb",
            query="""
            CREATE TABLE gb_calc.x__table_name__x (
                id BIGINT UNSIGNED NOT NULL,
                survey_id INT UNSIGNED NOT NULL,
                tag_id_int INT UNSIGNED NOT NULL,
                rejected_cnt INT UNSIGNED NOT NULL,
                CONSTRAINT x__table_name__x_pk
                    PRIMARY KEY (survey_id, tag_id_int, id)
            )
            SELECT aa.id, 
                   stb.survey_id, 
                   f.tag_id_int,
                   COUNT(1) AS rejected_cnt
              FROM gb_platform.platform_values_tags_fake_concepts_bin f,
                   gb_platform.audience_angle aa,
                   gb_platform.audience_angle_tags aat,
                   gb_platform.platform_values_survey_tags_bin stb
             WHERE f.tag_id_int = aat.tag_id_int
               AND aat.id = aa.id
               AND aa.angle_cnt = 2
               AND BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
               AND BIT_COUNT(stb.b_hated & f.b) > 0
               AND f.survey_id = stb.survey_id
             GROUP BY 
                   aa.id,
                   stb.survey_id, 
                   f.tag_id_int
              """
        )

        self.conn.query("drop table if exists gb_platform.platform_values_tags_fake_concepts_tam")
        self.conn.query(f"""
        create table gb_platform.platform_values_tags_fake_concepts_tam (
            tag_id_int int unsigned not null,
            survey_id int unsigned not null,
            tam bigint unsigned not null,
            constraint platform_values_tags_fake_concepts_tam_pk
              primary key (survey_id, tag_id_int)
        )
        SELECT distinct 
               z2.tag_id_int, 
               z2.survey_id,
               FIRST_VALUE(z2.tam) OVER (PARTITION BY z2.tag_id_int, z2.survey_id ORDER BY z2.tam DESC) AS tam
          FROM (
                SELECT z1.tag_id_int, 
                       z1.survey_id,
                       gb_platform.calc_tam(z1.total_audience, z1.arpu) AS tam
                  FROM (
                        SELECT f.tag_id_int, 
                               aap.survey_id,
                               gb_platform.calc_total_audience(
                                   aap.loved_cnt, 
                                   aap.total_cnt, 
                                   IF(r.rejected_cnt IS NULL, 0, r.rejected_cnt), 
                                   CAST(c.v1 AS UNSIGNED), 
                                   aa.loyalty_installs, 0, 0
                               ) AS total_audience,
                               aa.arpu
                          FROM gb_platform.platform_values_tags_fake_concepts_bin f
                         INNER JOIN gb_platform.audience_angle_tags aat
                            ON aat.tag_id_int = f.tag_id_int
                         INNER JOIN gb_platform.audience_angle aa
                            ON aa.id = aat.id
                           AND aa.angle_cnt = 2
                         INNER JOIN gb_platform.audience_angle_potential aap
                            ON aap.id = aa.id
                           AND aap.survey_id = f.survey_id
                          LEFT JOIN gb_calc.tmp_fake_concepts_rejected_per_angle_2_comb r
                            ON r.survey_id = aap.survey_id
                           AND r.id = aa.id
                           AND r.tag_id_int = f.tag_id_int
                         INNER JOIN gb_app.def_sheet_platform_values_constants c
                            ON c.param = '{CalcUtils.POTENTIAL_DOWNLOADS_MOBILE_WW}'
                       ) z1
               ) z2
        """)
        self.conn.analyze("platform.platform_values_tags_fake_concepts_tam")

    def create_tam_per_app_for_changed_apps(self):
        self.calc_utils.recreate_table(
            table_name="tmp_tam_changed_apps",
            query="""
            CREATE TABLE gb_calc.x__table_name__x (
                app_id_int int unsigned not null,
                constraint x__table_name__x_pk
                primary key (app_id_int)
            )
            SELECT DISTINCT pt.app_id_int
              FROM gb_platform.platform_values_tags_bin pt,
                   gb_platform.audience_angle_tam_per_app_state tam
             WHERE pt.app_id_int = tam.app_id_int
               AND pt.tags_md5 != tam.tags_md5
            UNION
            SELECT a.app_id_int
              FROM gb_platform.platform_values_tags_bin a
             WHERE a.app_id_int NOT IN (
             SELECT tam.app_id_int
               FROM gb_platform.audience_angle_tam_per_app_state tam
             )
            """
        )

        cnt = self.conn.select_one("""
        SELECT count(1) as cnt FROM gb_calc.tmp_tam_changed_apps
        """)["cnt"]

        if cnt > 0:
            self.__create_tam_per_app(
                skip_rejected=False,
                only_for_apps_filter_table="platform_values.tmp_tam_changed_apps"
            )

    # 2h
    def __create_tam_per_app(self, skip_rejected=True, only_for_apps_filter_table: str | None = None):

        if not skip_rejected:

            limit = 50 if only_for_apps_filter_table is None else 200
            page = 0

            self.calc_utils.recreate_table(
                table_name="tmp_calc",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    id bigint unsigned not null,
                    survey_id int unsigned not null,
                    app_id_int int unsigned not null,
                    rejected_cnt int unsigned not null,
                    constraint x__table_name__x_pk
                        primary key (id, survey_id, app_id_int)
                    )
                """
            )

            while True:

                print(f"Rejected per app: {page}")

                self.calc_utils.recreate_table(
                    table_name="tmp_part",
                    query=f"""
                    CREATE TABLE gb_calc.x__table_name__x (
                        id bigint unsigned not null,
                        constraint x__table_name__x_pk
                            primary key (id)
                        )
                        SELECT aap.id 
                          from gb_platform.audience_angle_potential aap,
                               gb_platform.audience_angle aa
                         where aap.id = aa.id
                         order by aap.id
                        LIMIT {page * limit}, {limit}
                    """
                )

                cnt = self.conn.select_one("select count(1) as cnt from gb_calc.tmp_part")["cnt"]
                if cnt == 0:
                    break

                page += 1

                apps_filter_q = ""
                if only_for_apps_filter_table is not None:
                    apps_filter_q = f""" 
                    AND t.app_id_int IN (
                        SELECT f.app_id_int FROM {only_for_apps_filter_table} f
                    ) 
                    """

                query = f"""
                    INSERT INTO gb_calc.tmp_calc (
                        id,
                        survey_id,
                        app_id_int,
                        rejected_cnt
                    )
                        SELECT part.id, 
                               stb.survey_id, 
                               t.app_id_int, 
                               COUNT(1) AS rejected_cnt
                          FROM gb_calc.tmp_part part,
                               gb_platform.audience_angle act,
                               gb_platform.platform_values_tags_bin t,
                               gb_platform.platform_values_survey_tags_bin stb
                         WHERE part.id = act.id
                           AND BIT_COUNT(t.b & act.b) = act.angle_cnt
                           AND BIT_COUNT(stb.b_loved & act.b) = act.angle_cnt
                           AND BIT_COUNT(t.b & stb.b_hated) > 0
                           {apps_filter_q}
                         GROUP BY 
                               part.id, 
                               stb.survey_id, 
                               t.app_id_int
                    """

                self.conn.query(query)
                self.conn.commit()

        if only_for_apps_filter_table is None:
            self.conn.query("TRUNCATE TABLE gb_platform.audience_angle_rejected_per_app")
        else:
            self.conn.query("""
            DELETE FROM gb_platform.audience_angle_rejected_per_app
            WHERE app_id_int IN (
                SELECT DISTINCT app_id_int FROM gb_calc.tmp_calc
            )
            """)

        self.conn.query(f"""
        INSERT INTO gb_platform.audience_angle_rejected_per_app (id, survey_id, app_id_int, rejected_cnt) 
        SELECT id, survey_id, app_id_int, rejected_cnt FROM gb_calc.tmp_calc
        """)

        self.conn.commit()
        self.conn.analyze("platform.audience_angle_rejected_per_app")

        self.calc_utils.recreate_table(
            table_name="tmp_calc",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                survey_id int unsigned not null,
                app_id_int int unsigned not null,
                angle_cnt int unsigned not null,
                total_audience bigint unsigned not null,
                tam bigint unsigned not null,
                tags_md5 binary(32) not null,
                constraint x__table_name__x_pk
                    primary key (survey_id, app_id_int, angle_cnt)
            )
            """
        )

        page = 0
        limit = 50

        while True:

            print(f"TAM per app: {page}")

            apps_filter_q = ""
            if only_for_apps_filter_table is not None:
                apps_filter_q = f"""
                AND a.app_id_int IN (
                    select f.app_id_int FROM {only_for_apps_filter_table} f
                )
                """

            self.calc_utils.recreate_table(
                temporary=False,
                table_name="tmp_part",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    app_id_int int unsigned not null,
                    tags_md5 binary(32) not null,
                    constraint x__table_name__x_pk
                        primary key (app_id_int)
                )
                SELECT a.app_id_int, t.tags_md5
                  from gb_platform.platform_values_apps a,
                       gb_platform.platform_values_tags_bin t
                 WHERE a.app_id_int = t.app_id_int
                  {apps_filter_q}
                 order by a.app_id_int
                 LIMIT {page * limit}, {limit}
                """
            )

            cnt = self.conn.select_one("select count(1) as cnt from gb_calc.tmp_part")["cnt"]
            if cnt == 0:
                break

            page += 1

            self.calc_utils.recreate_table(
                table_name="tmp_app_with_rejected_tam_per_angle_id",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    id bigint unsigned not null,
                    survey_id int unsigned not null,
                    app_id_int int unsigned not null,
                    total_audience bigint unsigned not null,
                    tam bigint unsigned not null,
                    constraint x__table_name__x_pk
                        primary key (id, survey_id, app_id_int)
                )
                SELECT z1.id, z1.survey_id, z1.app_id_int, z1.total_audience, gb_platform.calc_tam(
                           z1.total_audience,
                       z1.arpu) AS tam
                  FROM (
                SELECT aa.id,
                       p.survey_id,
                       part.app_id_int,
                       gb_platform.calc_total_audience(
                           p.loved_cnt,
                           p.total_cnt,
                           r.rejected_cnt,
                           CAST(pvc.v1 AS UNSIGNED),
                           aa.loyalty_installs, 0, 0
                        ) as total_audience,
                        aa.arpu
                  FROM gb_platform.audience_angle_potential p
                 INNER JOIN gb_platform.audience_angle aa
                    ON aa.id = p.id
                 INNER JOIN gb_calc.tmp_part part
                 INNER JOIN gb_platform.audience_angle_rejected_per_app r
                    ON r.app_id_int = part.app_id_int
                   AND r.survey_id = p.survey_id
                   AND r.id = p.id
                 INNER JOIN gb_app.def_sheet_platform_values_constants pvc
                    ON pvc.param = '{CalcUtils.POTENTIAL_DOWNLOADS_MOBILE_WW}'
                    ) z1
                """
            )

            self.conn.query(f"""
            INSERT INTO gb_calc.tmp_calc 
            (survey_id, app_id_int, angle_cnt, total_audience, tam, tags_md5)
            SELECT tpa.survey_id, 
                   part.app_id_int, 
                   aa.angle_cnt,
                   MAX(IF(r.total_audience is NULL, tpa.total_audience, r.total_audience)) as tam,
                   MAX(IF(r.tam is NULL, tpa.tam, r.tam)) as tam,
                   part.tags_md5
              FROM gb_calc.tmp_part part
             INNER JOIN gb_platform.audience_angle_tam_per_angle tpa
             INNER JOIN gb_platform.audience_angle aa
                ON aa.id = tpa.id
             INNER JOIN gb_platform.platform_values_tags_bin tb
                ON tb.app_id_int = part.app_id_int
               AND BIT_COUNT(tb.b & aa.b) = aa.angle_cnt
              LEFT JOIN gb_calc.tmp_app_with_rejected_tam_per_angle_id r
                ON r.id = tpa.id
               AND r.survey_id = tpa.survey_id
               AND r.app_id_int = part.app_id_int
             GROUP BY 
                   tpa.survey_id, 
                   part.app_id_int, 
                   aa.angle_cnt
            """)
            self.conn.commit()

        if only_for_apps_filter_table is None:
            self.conn.query("TRUNCATE TABLE gb_platform.audience_angle_tam_per_app")
            self.conn.query("TRUNCATE TABLE gb_platform.audience_angle_tam_per_app_state")
        else:
            self.conn.query("""
            DELETE FROM gb_platform.audience_angle_tam_per_app WHERE app_id_int IN (
                SELECT DISTINCT app_id_int FROM gb_calc.tmp_calc
            )
            """)
            self.conn.query("""
            DELETE FROM gb_platform.audience_angle_tam_per_app_state WHERE app_id_int IN (
                SELECT DISTINCT app_id_int FROM gb_calc.tmp_calc
            )
            """)

        self.conn.query("""
        INSERT INTO gb_platform.audience_angle_tam_per_app (app_id_int, survey_id, angle_cnt, total_audience, tam, tags_md5) 
        SELECT app_id_int, survey_id, angle_cnt, total_audience, tam, tags_md5 FROM gb_calc.tmp_calc
        """)
        self.conn.query("""
        INSERT INTO gb_platform.audience_angle_tam_per_app_state
        (app_id_int, tags_md5)
        SELECT DISTINCT t.app_id_int, t.tags_md5
        FROM gb_calc.tmp_calc t
        """)
        self.conn.commit()

        self.conn.analyze("platform.audience_angle_tam_per_app")
        self.conn.analyze("platform.audience_angle_tam_per_app_state")

    def recreate_single_app_tags(
            self,
            user_id: int,
            app_id_int: int
    ):
        PlatformValuesApps.run(conn=self.conn, app_ids_int=[app_id_int], commit=False)
        PlatformValuesTags.run_tags(conn=self.conn, app_ids_int=[app_id_int], commit=False)
        PlatformValuesTags.run_tags_bin(conn=self.conn, app_ids_int=[app_id_int], commit=False)

        service_conn = DbConnection()
        service_conn.query("""
            DELETE FROM gb_calc.requests
            WHERE user_id = %s
            AND calc IN ('audiences_angles', 'competitors_for_audience_angle')
            """, [user_id])
        service_conn.query("""
                    DELETE FROM gb_calc.calc_tam_per_app_on_demand WHERE app_id_int = %s
                    """, [app_id_int])
        service_conn.query("""
                    INSERT INTO gb_calc.calc_tam_per_app_on_demand (app_id_int) VALUES (%s)
                    """, [app_id_int])
        service_conn.commit()
        service_conn.close()

        ServiceWrapperModel.run(s="src/services/platform_values_on_demand_calc.py")

    def calc_tam_per_app_on_demand(self) -> bool:

        self.conn.query("LOCK TABLE gb_calc.calc_tam_per_app_on_demand WRITE")

        rows = self.conn.select_all("""
            SELECT app_id_int FROM gb_calc.calc_tam_per_app_on_demand
            """)

        app_ids_int = [row["app_id_int"] for row in rows]

        self.conn.query("""DELETE FROM gb_calc.calc_tam_per_app_on_demand""")

        self.conn.unlock_tables()

        if len(app_ids_int) <= 0:
            return False

        PlatformValuesApps.run(conn=self.conn, app_ids_int=app_ids_int)
        PlatformValuesTags.run_tags(conn=self.conn, app_ids_int=app_ids_int)
        PlatformValuesTags.run_tags_bin(conn=self.conn, app_ids_int=app_ids_int)

        arr = []
        for app_id_int in app_ids_int:
            arr.append(f"SELECT {app_id_int} as app_id_int")

        q = " UNION ".join(arr)

        self.calc_utils.recreate_table(
            table_name="tmp_calc_tam_per_app_on_demand",
            query=f"""
                                CREATE TABLE gb_calc.x__table_name__x (
                                    app_id_int bigint unsigned not null,
                                    constraint x__table_name__x_pk
                                        primary key (app_id_int)
                                    )
                                    {q}
                                """
        )

        self.__create_tam_per_app(skip_rejected=False,
                                  only_for_apps_filter_table="platform_values.tmp_calc_tam_per_app_on_demand")
        return True
