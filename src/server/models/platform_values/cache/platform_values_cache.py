from math import log

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.environment.runtime_constants import rr
from src.server.models.platform_values.cache.queries.platform_values_apps import PlatformValuesApps
from src.server.models.platform_values.cache.queries.platform_values_tags import PlatformValuesTags
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.server.models.tags.tags_constants import TagsConstants
from src.utils.gembase_utils import GembaseUtils


class PlatformValuesCache:

    def __init__(self, conn: DbConnection, update_progress=None):
        self.__conn = conn
        self.__update_progress = update_progress
        self.__progress_index = 0

    def __log_progress(self, step_desc):
        if self.__update_progress is not None:
            self.__progress_index += 1
            self.__update_progress({
                "step_index": self.__progress_index,
                "step_desc": step_desc
            })

    def process(self):

        self.__log_progress("Def")

        #############################################
        # platform_rebuild_tmp.def_tags                         #
        #############################################

        self.__conn.query_safe("DROP TABLE IF EXISTS platform_rebuild_tmp.def_tags")
        self.__conn.query("""
            CREATE TABLE platform_rebuild_tmp.def_tags (
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
              FROM app.def_sheet_platform_product p
             WHERE p.is_prompt = 1
            """)
        self.__conn.analyze("platform_rebuild_tmp.def_tags")

        #############################################
        # platform_rebuild_tmp.platform_values_apps             #
        #############################################

        self.__log_progress("Apps")
        PlatformValuesApps.run(conn=self.__conn)

        #############################################
        # platform_rebuild_tmp.platform_values_devs_apps        #
        #############################################

        self.__conn.query_safe("DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_devs_apps")
        self.__conn.query("""
            CREATE TABLE platform_rebuild_tmp.platform_values_devs_apps (
                dev_id_int MEDIUMINT NOT NULL,
                app_id_int MEDIUMINT NOT NULL,
                CONSTRAINT platform_values_devs_apps_pk
                    PRIMARY KEY (dev_id_int, app_id_int)
            )
            SELECT da.dev_id_int, a.app_id_int
              FROM scraped_data.devs_apps da,
                   platform_rebuild_tmp.platform_values_apps a
             WHERE da.app_id_int = a.app_id_int
               AND da.primary_dev = 1
            """)
        self.__conn.analyze("platform_rebuild_tmp.platform_values_devs_apps")

        #############################################
        # platform_rebuild_tmp.platform_values_tags             #
        #############################################

        self.__log_progress("Tags")
        PlatformValuesTags.run_tags(conn=self.__conn)

        #############################################
        # platform_rebuild_tmp.platform_values_survey_tags      #
        #############################################

        self.__log_progress("Survey")
        # todo hack - subcategory Platform ma loved pre vsetkych respondentov, aby nedochadzalo k rozdielom pre zobrazenie audience anglu platformy
        query = f"""
            create table platform_rebuild_tmp.platform_values_survey_tags
            (
                survey_meta_id SMALLINT UNSIGNED NOT NULL,
                survey_instance_int mediumint unsigned not null,
                tag_id_int smallint unsigned not null,
                tag_value   TINYINT unsigned not NULL,
                loved TINYINT UNSIGNED NOT NULL,
                hated TINYINT UNSIGNED NOT NULL,
                rejected TINYINT UNSIGNED NOT NULL,
                constraint platform_values_survey_tags_pk
                    primary key (survey_meta_id, survey_instance_int, tag_id_int)
            )
            SELECT z1.survey_meta_id,
                   z1.survey_instance_int,
                   z1.tag_id_int,
                   z1.tag_value,
                   IF(z1.tag_value = 100, 1, 0) AS loved,
                   IF(z1.tag_value < 50, 1, 0) AS hated,
                   IF(z1.tag_value < 50 AND z1.not_rejected = 0, 1, 0) AS rejected
              FROM (
                    SELECT t.survey_meta_id, 
                           si.id as survey_instance_int, 
                           p.tag_id_int, 
                           p.not_rejected,
                           IF(p.subcategory_int = {TagsConstants.SUBCATEGORY_PLATFORMS_ID}, 100, t.tag_value) as tag_value
                      FROM survey_data.survey_tags t,
                           survey_data.survey_info si,
                           app.def_sheet_platform_product p,
                           app.map_tag_id m
                     WHERE si.survey_instance = t.survey_instance
                       AND si.survey_meta_id = t.survey_meta_id
                       AND p.tag_id_int = m.id
                       AND m.tag_id = t.tag_id
               ) z1
            """
        self.__conn.query_safe("DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_survey_tags")
        self.__conn.query(query)
        self.__conn.analyze("platform_rebuild_tmp.platform_values_survey_tags")

        #############################################
        # platform_rebuild_tmp.platform_values_survey_flags     #
        #############################################

        query = f"""
            create table platform_rebuild_tmp.platform_values_survey_flags
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
              FROM survey_data.survey_flags t,
                   survey_data.survey_info si,
                   app.def_sheet_platform_product p,
                   app.map_tag_id m
             WHERE si.survey_instance = t.survey_instance
               AND si.survey_meta_id = t.survey_id
               AND p.tag_id_int = m.id
               AND m.tag_id = t.tag_id
            """
        self.__conn.query_safe("DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_survey_flags")
        self.__conn.query(query)
        self.__conn.analyze("platform_rebuild_tmp.platform_values_survey_flags")

        #############################################
        # platform_rebuild_tmp.platform_values_survey_info      #
        #############################################

        query = f"""
            create table platform_rebuild_tmp.platform_values_survey_info
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
              FROM survey_data.survey_info si
              LEFT JOIN app.map_app_id_to_store_id m
                ON m.app_id_in_store = si.favorite_game
            """
        self.__conn.query_safe("DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_survey_info")
        self.__conn.query(query)
        self.__conn.analyze("platform_rebuild_tmp.platform_values_survey_info")

        ###############################################
        # platform_rebuild_tmp.platform_values_survey_competitors #
        ###############################################

        query = f"""
                    create table platform_rebuild_tmp.platform_values_survey_competitors
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
                      FROM survey_data.survey_info si
                      JOIN survey_data.survey_competitors sc
                        ON sc.survey_instance = si.survey_instance
                       AND sc.survey_meta_id = si.survey_meta_id
                      JOIN app.map_app_id_to_store_id m
                        ON m.app_id_in_store = sc.app_id
                    """
        self.__conn.query_safe("DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_survey_competitors")
        self.__conn.query(query)
        self.__conn.analyze("platform_rebuild_tmp.platform_values_survey_competitors")

        ###############################################
        # platform_rebuild_tmp.platform_values_apps_tags_per_subc #
        ###############################################

        self.__conn.query("DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_apps_tags_per_subc")
        self.__conn.query("""
            CREATE TABLE platform_rebuild_tmp.platform_values_apps_tags_per_subc (
                app_id_int MEDIUMINT NOT NULL,
                subcategory_int SMALLINT UNSIGNED NOT NULL,
                cnt SMALLINT UNSIGNED NOT NULL,
                CONSTRAINT platform_values_apps_tags_per_subc_pk
                    PRIMARY KEY (app_id_int, subcategory_int)
            )
            SELECT a.app_id_int,
                   p.subcategory_int,
                   COUNT(1) AS cnt
              FROM platform_rebuild_tmp.platform_values_apps a,
                   platform_rebuild_tmp.platform_values_tags t,
                   app.def_sheet_platform_product p
             WHERE a.app_id_int = t.app_id_int
               AND t.tag_id_int = p.tag_id_int
             GROUP BY 
                   a.app_id_int, 
                   p.subcategory_int
            """)
        self.__conn.commit()
        self.__conn.analyze("platform_rebuild_tmp.platform_values_apps_tags_per_subc")

        ##################################################
        # platform_rebuild_tmp.platform_values_installs_arpu_per_tag #
        ##################################################

        self.__log_progress("Installs/ARPU")
        self.__conn.query("DROP TABLE IF EXISTS platform_calc_tmp.tmp_arpu_per_tag")
        self.__conn.query("""
            CREATE TABLE platform_calc_tmp.tmp_arpu_per_tag (
                tag_id_int SMALLINT UNSIGNED NOT NULL,
                app_id_int MEDIUMINT NOT NULL,
                downloads_ww_row_num MEDIUMINT UNSIGNED NOT NULL,
                CONSTRAINT tmp_arpu_per_tag_pk
                    PRIMARY KEY (tag_id_int, app_id_int)
            )
            SELECT p.tag_id_int, 
                   a.app_id_int,
                   ROW_NUMBER() OVER (PARTITION BY p.tag_id_int ORDER BY a.installs DESC) AS downloads_ww_row_num
              FROM platform_rebuild_tmp.def_tags p
             INNER JOIN platform_rebuild_tmp.platform_values_tags t ON t.tag_id_int = p.tag_id_int
             INNER JOIN platform_rebuild_tmp.platform_values_apps a ON a.app_id_int = t.app_id_int
            """)

        self.__conn.commit()

        self.__conn.query("DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_installs_arpu_per_tag")
        self.__conn.query("""
            CREATE TABLE platform_rebuild_tmp.platform_values_installs_arpu_per_tag (
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
                   IF(e.arpu IS NULL OR e.arpu = 0, 
                       IF(z2.arpu_ext IS NULL, IF(z2.arpu_ww IS NULL, 0, z2.arpu_ww), z2.arpu_ext),
                        e.arpu) as arpu
              FROM platform_rebuild_tmp.def_tags p
             INNER JOIN platform_rebuild_tmp.platform_values_tags t ON t.tag_id_int = p.tag_id_int
             INNER JOIN platform_rebuild_tmp.platform_values_apps a ON a.app_id_int = t.app_id_int
              LEFT JOIN scraped_data.ext_data_2_arpu_per_tag e ON e.tag_id_int = p.tag_id_int
              LEFT JOIN (
                         SELECT p.tag_id_int,
                                IF(z_ww.arpu_ww IS NULL, 0, z_ww.arpu_ww) AS arpu_ww,
                                apt.arpu as arpu_ext
                           FROM platform_rebuild_tmp.def_tags p
                           LEFT JOIN (
                                      SELECT t_ww.tag_id_int,
                                             ROUND(SUM(a_ww.revenues_gp_ww) / SUM(a_ww.installs), 4) AS arpu_ww
                                        FROM platform_calc_tmp.tmp_arpu_per_tag t_ww,
                                             platform_rebuild_tmp.platform_values_apps a_ww
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
        self.__conn.query("DROP TABLE IF EXISTS platform_calc_tmp.tmp_arpu_per_tag")
        self.__conn.analyze("platform_rebuild_tmp.platform_values_installs_arpu_per_tag")

        ##################################################
        # platform_rebuild_tmp.platform_values_survey_data_per_tag   #
        ##################################################

        query = f"""
            CREATE TABLE platform_rebuild_tmp.platform_values_survey_data_per_tag (
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
                   round(GREATEST(0, ROUND(st.filtered_cnt / st.total_cnt * {PlatformValuesHelper.POTENTIAL_DOWNLOADS_PLATFORM_WW}) - a.loyalty_installs) * a.arpu, 0) as tam
              FROM survey_data.survey_tam_per_tag st,
                   platform_rebuild_tmp.platform_values_installs_arpu_per_tag a
             WHERE st.tag_id_int = a.tag_id_int
            """
        self.__conn.query("drop table if exists platform_rebuild_tmp.platform_values_survey_data_per_tag")
        self.__conn.query(query)
        self.__conn.analyze("platform_rebuild_tmp.platform_values_survey_data_per_tag")

        ##################################################
        # platform_rebuild_tmp.platform_values_tags_fake_concepts    #
        ##################################################

        query = """
            CREATE TABLE platform_rebuild_tmp.platform_values_tags_fake_concepts (
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
                                              FROM platform_rebuild_tmp.def_tags p,
                                                   platform_rebuild_tmp.platform_values_apps a,
                                                   platform_rebuild_tmp.platform_values_tags t
                                             WHERE t.tag_id_int = p.tag_id_int
                                               AND a.app_id_int = t.app_id_int
                                           ) z1
                                       WHERE z1.row_num <= 20
                                   ) z2,
                                   platform_rebuild_tmp.platform_values_tags t
                             WHERE z2.app_id_int = t.app_id_int
                             GROUP BY 
                                   z2.tag_id_int, 
                                   t.tag_id_int
                           ) z3,
                           platform_rebuild_tmp.platform_values_survey_data_per_tag aud
                     WHERE z3.fake_concept_tag_id_int = aud.tag_id_int
                   ) z4
             WHERE z4.row_num <= 3
            """
        self.__conn.query("drop table if exists platform_rebuild_tmp.platform_values_tags_fake_concepts")
        self.__conn.query(query)
        self.__conn.analyze("platform_rebuild_tmp.platform_values_tags_fake_concepts")

        rows = self.__conn.select_all("""
            select survey_id,
                   tag_id_int, 
                   fake_concept_tag_id_int
              from platform_rebuild_tmp.platform_values_tags_fake_concepts
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
                if GembaseUtils.compare_arr(tags_ids, unique_tags_ids_per_survey_id[survey_id][i]):
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

        self.__conn.query(f"""
            UPDATE platform_rebuild_tmp.platform_values_tags_fake_concepts
               SET fake_concept_id = {case_when}
            """)
        self.__conn.commit()

        ##################################################
        # platform_rebuild_tmp.platform_values_devs                  #
        ##################################################

        self.__conn.query("drop table if exists platform_rebuild_tmp.platform_values_devs")
        self.__conn.query("""
            CREATE TABLE platform_rebuild_tmp.platform_values_devs (
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
                              FROM platform_rebuild_tmp.platform_values_devs_apps da,
                                   platform_rebuild_tmp.platform_values_apps a,
                                   platform_rebuild_tmp.platform_values_tags t,
                                   platform_rebuild_tmp.platform_values_installs_arpu_per_tag ta
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
        self.__conn.analyze("platform_rebuild_tmp.platform_values_devs")

        self.process_bit_values()

        self.__log_progress("Audience")
        self.__create_audience_angles()

        self.create_db_functions(tmp=True)

        self.__create_survey_tags_w()

        self.__calc_loved_ratio_ext()

        self.__create_fake_concepts_tam()

        self.create_tam_per_app()

        self.copy_from_tmp_to_live()

    def clear_old_result_data(self):
        self.__conn.query("""
            DELETE FROM platform_values.requests r WHERE DATE_ADD(r.t_start, INTERVAL 2 WEEK) < NOW()
            """)
        self.__conn.commit()

        def delete_for_table(table_name: str):
            self.__conn.query(f"""
                    DELETE FROM {table_name} rr
                      WHERE rr.platform_id NOT IN (
                          SELECT r.platform_id FROM platform_values.requests r
                      )
                    """)
            self.__conn.commit()

        delete_for_table("platform_values.results_audience_angles__competitors")
        delete_for_table("platform_values.results_audience_angles__final")
        delete_for_table("platform_values.results_audience_angles__top_tags")
        delete_for_table("platform_values.results_audience_angle__input_tags")
        delete_for_table("platform_values.results_competitors_cnt")
        delete_for_table("platform_values.results_affinities")

        self.__conn.commit()

    def process_bit_values(self):

        rows_tags = self.__conn.select_all("""
            SELECT p.tag_id_int,
                   pp.not_rejected,
                   p.competitors_pool_w
              FROM platform_rebuild_tmp.def_tags p,
                   app.def_sheet_platform_product pp
             WHERE pp.tag_id_int = p.tag_id_int
             ORDER BY p.tag_id_int
            """)

        varbinary_per_tag_id = {}
        not_rejected_per_tag_id = {}

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
            not_rejected_per_tag_id[tag_id_int] = rows_tags[i]["not_rejected"]

        self.__conn.query("DROP TABLE IF EXISTS platform_rebuild_tmp.zero_bin_value")
        self.__conn.query(f"""
            CREATE TABLE platform_rebuild_tmp.zero_bin_value (
                b BINARY({bin_bytes_cnt}) not null
            )
            SELECT b'{zero_bin_str}' as b
            """)
        self.__conn.commit()
        self.__conn.analyze("platform_rebuild_tmp.zero_bin_value")

        self.__conn.query("DROP TABLE IF EXISTS platform_rebuild_tmp.bin_bytes_cnt")
        self.__conn.query(f"""
                    CREATE TABLE platform_rebuild_tmp.bin_bytes_cnt (
                        cnt int unsigned not null
                    )
                    SELECT {bin_bytes_cnt} as cnt
                    """)
        self.__conn.commit()
        self.__conn.analyze("platform_rebuild_tmp.bin_bytes_cnt")

        self.__conn.query("DROP TABLE IF EXISTS platform_rebuild_tmp.def_tags_bin")
        self.__conn.query(f"""
            CREATE TABLE platform_rebuild_tmp.def_tags_bin (
                tag_id_int smallint unsigned not null,
                b BINARY({bin_bytes_cnt}) not null,
                not_rejected int not null,
                constraint def_tags_bin_pk
                    primary key (tag_id_int)
            )
            """)
        bulk_data_v = []
        for tag_id_int in varbinary_per_tag_id:
            bulk_data_v.append((tag_id_int, varbinary_per_tag_id[tag_id_int], not_rejected_per_tag_id[tag_id_int]))
        self.__conn.bulk("""
            INSERT INTO platform_rebuild_tmp.def_tags_bin (tag_id_int, b, not_rejected) VALUES (%s, b%s, %s)
            """, bulk_data_v)
        self.__conn.commit()
        self.__conn.analyze("platform_rebuild_tmp.def_tags_bin")

        self.__conn.query("""
        DROP TABLE IF EXISTS platform_rebuild_tmp.def_potential_downloads_bin
        """)

        self.__conn.query(f"""
        CREATE TABLE platform_rebuild_tmp.def_potential_downloads_bin
        (
            id                  int auto_increment,
            tags_b              binary({bin_bytes_cnt}) not null,
            potential_downloads bigint     not null,
            constraint def_potential_downloads_bin_pk
                primary key (id)
        )
        SELECT tb.b as tags_b,
               CASE WHEN tb.tag_id_int = {TagsConstants.PLATFORM_PC} THEN {PlatformValuesHelper.POTENTIAL_DOWNLOADS_PC}
               WHEN tb.tag_id_int = {TagsConstants.PLATFORM_MOBILE} THEN {PlatformValuesHelper.POTENTIAL_DOWNLOADS_MOBILE}
               ELSE 0 END as potential_downloads
          FROM platform_rebuild_tmp.def_tags_bin tb
         WHERE tb.tag_id_int IN ({TagsConstants.PLATFORM_PC}, {TagsConstants.PLATFORM_MOBILE})
        """)

        self.__conn.commit()
        self.__conn.analyze("platform_rebuild_tmp.def_potential_downloads_bin")

        query = f"""
                    CREATE TABLE platform_rebuild_tmp.def_tags_subc_bin (
                        subcategory_int SMALLINT UNSIGNED NOT NULL,
                        b BINARY({bin_bytes_cnt}) NOT NULL,
                        competitors_pool_w smallint unsigned not null,
                        CONSTRAINT def_tags_subc_bin_pk
                            PRIMARY KEY (subcategory_int)
                    )
                    SELECT p.subcategory_int, 
                           BIT_OR(d.b) AS b,
                           p.competitors_pool_w
                      FROM platform_rebuild_tmp.def_tags_bin d,
                           platform_rebuild_tmp.def_tags p
                     WHERE d.tag_id_int = p.tag_id_int
                       AND p.competitors_pool_w != 0
                     GROUP BY p.subcategory_int, p.competitors_pool_w
                    """
        self.__conn.query("drop table if exists platform_rebuild_tmp.def_tags_subc_bin")
        self.__conn.query(query)
        self.__conn.analyze("platform_rebuild_tmp.def_tags_subc_bin")

        PlatformValuesTags.run_tags_bin(conn=self.__conn)

        self.__conn.query("DROP TABLE if exists platform_rebuild_tmp.platform_values_survey_tags_bin")
        self.__conn.query(f"""
            CREATE TABLE platform_rebuild_tmp.platform_values_survey_tags_bin (
                survey_id smallint NOT NULL,
                survey_instance_int INT UNSIGNED NOT NULL,
                b BINARY({bin_bytes_cnt}) NOT NULL,
                b_hated BINARY({bin_bytes_cnt}) NOT NULL,
                b_loved BINARY({bin_bytes_cnt}) NOT NULL,
                b_rejected BINARY({bin_bytes_cnt}) NOT NULL,
                CONSTRAINT platform_values_survey_tags_bin_pk
                    PRIMARY KEY (survey_id, survey_instance_int)
            )
            SELECT st.survey_meta_id AS survey_id,
                   st.survey_instance_int,
                   BIT_OR(tb.b) AS b,
                   BIT_OR(IF(st.hated = 1, tb.b, zero.b)) AS b_hated,
                   BIT_OR(IF(st.loved = 1, tb.b, zero.b)) AS b_loved,
                   BIT_OR(IF(st.rejected = 1, tb.b, zero.b)) AS b_rejected
              FROM platform_rebuild_tmp.def_tags_bin tb,
                   platform_rebuild_tmp.platform_values_survey_tags st,
                   platform_rebuild_tmp.zero_bin_value zero
             WHERE tb.tag_id_int = st.tag_id_int
             GROUP BY  
                   st.survey_meta_id, 
                   st.survey_instance_int
            """)
        self.__conn.analyze("platform_rebuild_tmp.platform_values_survey_tags_bin")

        self.__conn.query("drop table if exists platform_rebuild_tmp.platform_values_tags_fake_concepts_bin")
        self.__conn.query(f"""
            create table platform_rebuild_tmp.platform_values_tags_fake_concepts_bin (
                survey_id int unsigned not null,
                tag_id_int int unsigned not null,
                b BINARY({bin_bytes_cnt}) not null,
                constraint platform_values_tags_fake_concepts_bin_pk
                primary key (survey_id, tag_id_int)
            )
            SELECT f.survey_id, f.tag_id_int, BIT_OR(d.b) as b
              FROM platform_rebuild_tmp.platform_values_tags_fake_concepts f,
                   platform_rebuild_tmp.def_tags_bin d
             WHERE f.fake_concept_tag_id_int = d.tag_id_int
             GROUP BY f.survey_id, f.tag_id_int
            """)
        self.__conn.analyze("platform_rebuild_tmp.platform_values_tags_fake_concepts_bin")

    def __create_audience_angles(self):

        bin_bytes_cnt = PlatformValuesHelper.get_bin_bytes_cnt(self.__conn)

        self.__conn.query("drop table if exists platform_rebuild_tmp.audience_angle_tags")
        self.__conn.query("""
        CREATE TABLE platform_rebuild_tmp.audience_angle_tags (
            id bigint unsigned not null,
            tag_id_int int unsigned not null,
            constraint audience_angle_tags_pk
                primary key (id, tag_id_int)
        )
        SELECT d.tag_id_int as id, 
               d.tag_id_int
          FROM platform_rebuild_tmp.def_tags_bin d
         UNION ALL
        SELECT d1.tag_id_int * 10000 + d2.tag_id_int as id, 
               d1.tag_id_int
          FROM platform_rebuild_tmp.def_tags_bin d1,
               platform_rebuild_tmp.def_tags_bin d2
         WHERE d1.tag_id_int < d2.tag_id_int
         UNION ALL
        SELECT d1.tag_id_int * 10000 + d2.tag_id_int as id, 
               d2.tag_id_int
          FROM platform_rebuild_tmp.def_tags_bin d1,
               platform_rebuild_tmp.def_tags_bin d2
         WHERE d1.tag_id_int < d2.tag_id_int
        """)

        self.__conn.query("drop table if exists platform_rebuild_tmp.audience_angle")
        self.__conn.query(f"""
        CREATE TABLE platform_rebuild_tmp.audience_angle (
            id bigint unsigned not null,
            angle_cnt tinyint unsigned not null,
            b binary({bin_bytes_cnt}) not null,
            platform_b binary({bin_bytes_cnt}),
            installs bigint unsigned not null,
            loyalty_installs bigint unsigned not null,
            arpu DECIMAL(8,4) not null,
            valid_combination int default 0 not null,
            is_platform int default 0 not null,
            potential_downloads bigint default 0 not null,
            constraint audience_angle_pk
                primary key (id)
        )
        SELECT t.id, 
               COUNT(1) as angle_cnt,
               BIT_OR(d.b) as b,
               z1.b as platform_b,
               0 as installs,
               0 as loyalty_installs,
               0 as arpu,
               0 as valid_combination,
               IF(z1.id IS NULL, 0, 1) as is_platform,
               0 as potential_downloads
          FROM platform_rebuild_tmp.audience_angle_tags t
         INNER JOIN platform_rebuild_tmp.def_tags_bin d
         LEFT JOIN (
             SELECT aat.id, BIT_OR(dtb.b) as b
               FROM platform_rebuild_tmp.audience_angle_tags aat,
                    platform_rebuild_tmp.def_tags dt,
                    platform_rebuild_tmp.def_tags_bin dtb
              WHERE aat.tag_id_int = dt.tag_id_int
                AND aat.tag_id_int = dtb.tag_id_int
                AND dt.subcategory_int = {TagsConstants.SUBCATEGORY_PLATFORMS_ID}
              GROUP BY aat.id
         ) z1 ON z1.id = t.id
         WHERE t.tag_id_int = d.tag_id_int
         GROUP BY t.id
        """)

        bulk_data = []
        for angle_id in PlatformValuesHelper.get_valid_angles(conn=self.__conn):
            bulk_data.append((angle_id,))
        self.__conn.bulk("""
        UPDATE platform_rebuild_tmp.audience_angle
           SET valid_combination = 1
         WHERE id = %s
        """, bulk_data)
        self.__conn.commit()

        self.__conn.query("""
        UPDATE platform_rebuild_tmp.audience_angle aa
        INNER JOIN (
            SELECT aa.id, SUM(d.potential_downloads) AS potential_downloads
                 FROM platform_rebuild_tmp.def_potential_downloads_bin d,
                      platform_rebuild_tmp.audience_angle aa
                WHERE BIT_COUNT(d.tags_b & aa.b) > 0
                GROUP BY aa.id
        ) z1 ON z1.id = aa.id
        SET aa.potential_downloads = z1.potential_downloads
        WHERE aa.is_platform = 1
                """)

        self.__conn.commit()

        self.__update_audience_angle_loyalty_installs()
        self.__update_audience_angle_arpu()
        self.__create_audience_angle_potential()

    def __update_audience_angle_loyalty_installs(self):
        self.__conn.query("""
        UPDATE platform_rebuild_tmp.audience_angle aa
        INNER JOIN (
            SELECT aaa.id,
                   SUM(a.installs) as installs,
                   SUM(a.loyalty_installs) AS loyalty_installs
              FROM platform_rebuild_tmp.audience_angle aaa,
                   platform_rebuild_tmp.platform_values_tags_bin tb,
                   platform_rebuild_tmp.platform_values_apps a
             WHERE bit_count(tb.b & aaa.b) = aaa.angle_cnt
               AND a.app_id_int = tb.app_id_int
             GROUP BY aaa.id
        ) z1
        ON z1.id = aa.id
        SET aa.loyalty_installs = z1.loyalty_installs, aa.installs = z1.installs
        """)
        self.__conn.commit()
        self.__conn.analyze("platform_rebuild_tmp.audience_angle")

    # survey change, tags def change
    # 26s
    def __create_audience_angle_potential(self):
        self.__conn.query("drop table if exists platform_rebuild_tmp.audience_angle_potential")
        self.__conn.query(f"""
        CREATE TABLE platform_rebuild_tmp.audience_angle_potential (
            id bigint unsigned not null,
            survey_id int unsigned not null,
            loved_cnt int unsigned default 0 not null,
            total_cnt int unsigned default 0 not null,
            loved_ratio_ext decimal(8,4) default 0 not null,
            rejected_ratio_ext decimal(8,4) default 0 not null,
            loyalty_installs bigint default 0 not null,
            loved_hack int default 0 not null,
            constraint audience_angle_potential_pk
                primary key (id, survey_id)
        )
        SELECT t.id, 
               stb.survey_id, 
               COUNT(1) as loved_cnt, 
               total_cnt.total_cnt,
               0 as loved_ratio_ext,
               1 as rejected_ratio_ext,
               0 as loyalty_installs,
               0 as loved_hack
          FROM platform_rebuild_tmp.audience_angle t,
               platform_rebuild_tmp.platform_values_survey_tags_bin stb,
               (
                   SELECT si.survey_meta_id as survey_id, 
                          COUNT(1) as total_cnt
                     FROM platform_rebuild_tmp.platform_values_survey_info si
                    GROUP BY si.survey_meta_id
               ) total_cnt
         WHERE BIT_COUNT(stb.b_loved & t.b) = t.angle_cnt
           AND stb.survey_id = total_cnt.survey_id
         GROUP BY 
               t.id, 
               total_cnt.survey_id, 
               total_cnt.total_cnt
        """)
        self.__conn.analyze("platform_rebuild_tmp.audience_angle_potential")

    # 5s
    def __update_audience_angle_arpu(self):
        self.__conn.query("""
        UPDATE platform_rebuild_tmp.audience_angle ct
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
                                      FROM platform_rebuild_tmp.platform_values_installs_arpu_per_tag arpu,
                                           platform_rebuild_tmp.audience_angle_tags ctt
                                     WHERE ctt.tag_id_int = arpu.tag_id_int
                                  ) z1
                           ) z2
                     WHERE z2.row_num IN (FLOOR((z2.total_num + 1) / 2), FLOOR((z2.total_num + 2) / 2) )
                     GROUP BY z2.id
                   ) z3
           ON z3.id = ct.id
          SET ct.arpu = z3.arpu
        """)
        self.__conn.commit()
        self.__conn.analyze("platform_rebuild_tmp.audience_angle")

    # 2 mins
    def __create_survey_tags_w(self):
        print(f"create_survey_tags_w")

        bin_bytes_cnt = PlatformValuesHelper.get_bin_bytes_cnt(self.__conn)

        self.__conn.query("drop table if exists platform_rebuild_tmp.audience_angle_tags_w")
        self.__conn.query(f"""
        CREATE TABLE platform_rebuild_tmp.audience_angle_tags_w (
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
            self.__log_progress("Survey weights")

            PlatformValuesHelper.recreate_table(
                conn=self.__conn,
                table_name="tmp_part",
                schema="platform_calc_tmp",
                query=f"""
                CREATE TABLE x__table_name__x (
                    id bigint unsigned not null,
                    constraint x__table_name__x_pk
                        primary key (id)
                )
                SELECT aap.id
                  FROM platform_rebuild_tmp.audience_angle_potential aap
                 ORDER BY aap.id 
                 LIMIT {page * limit}, {limit}
                """
            )

            cnt = self.__conn.select_one("SELECT count(1) AS cnt FROM platform_calc_tmp.tmp_part")["cnt"]

            if cnt == 0:
                break

            page += 1

            self.__conn.query("""
            INSERT INTO platform_rebuild_tmp.audience_angle_tags_w
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
                                              FROM platform_calc_tmp.tmp_part part,
                                                   platform_rebuild_tmp.audience_angle t,
                                                   platform_rebuild_tmp.platform_values_survey_tags_bin stb
                                             WHERE part.id = t.id
                                               AND BIT_COUNT(stb.b_loved & t.b) = t.angle_cnt
                                           ) z1,
                                           platform_rebuild_tmp.platform_values_survey_tags st,
                                           platform_rebuild_tmp.def_tags_bin d
                                     WHERE st.survey_meta_id = z1.survey_id
                                       AND st.survey_instance_int = z1.survey_instance_int
                                       AND st.tag_id_int = d.tag_id_int
                                       AND st.loved = 1
                                     GROUP BY 
                                           z1.id, 
                                           z1.survey_id, 
                                           d.tag_id_int
                                   ) z2,
                                   platform_rebuild_tmp.audience_angle_potential p
                             WHERE z2.id = p.id
                               AND z2.survey_id = p.survey_id
                           ) z3
                   ) z4,
                   platform_rebuild_tmp.def_tags_bin b
             WHERE z4.tag_id_int = b.tag_id_int
               AND z4.w != 0
             GROUP BY z4.id, z4.survey_id, z4.w
            """)
            self.__conn.commit()

        self.__conn.analyze("platform_rebuild_tmp.audience_angle_tags_w")

        # 40s
    def __create_fake_concepts_tam(self):
        print("create_fake_concepts_tam")

        PlatformValuesHelper.recreate_table(
            conn=self.__conn,
            table_name="tmp_fake_concepts_rejected_per_angle_2_comb",
            schema="platform_calc_tmp",
            query="""
            CREATE TABLE x__table_name__x (
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
              FROM platform_rebuild_tmp.platform_values_tags_fake_concepts_bin f,
                   platform_rebuild_tmp.audience_angle aa,
                   platform_rebuild_tmp.audience_angle_tags aat,
                   platform_rebuild_tmp.platform_values_survey_tags_bin stb
             WHERE f.tag_id_int = aat.tag_id_int
               AND aat.id = aa.id
               AND aa.angle_cnt = 2
               AND BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
               AND BIT_COUNT(stb.b_rejected & f.b) > 0
               AND f.survey_id = stb.survey_id
             GROUP BY 
                   aa.id,
                   stb.survey_id, 
                   f.tag_id_int
              """
        )

        self.__conn.query("drop table if exists platform_rebuild_tmp.platform_values_tags_fake_concepts_tam")
        self.__conn.query(f"""
        create table platform_rebuild_tmp.platform_values_tags_fake_concepts_tam (
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
                       platform.calc_tam(z1.total_audience, z1.arpu) AS tam
                  FROM (
                        SELECT f.tag_id_int, 
                               aap.survey_id,
                               platform.calc_total_audience(
                                   aap.loved_cnt, 
                                   aap.total_cnt, 
                                   IF(r.rejected_cnt IS NULL, 0, r.rejected_cnt), 
                                   platform_rebuild_tmp.get_potential_downloads(f.b, aa.id), 
                                   platform_rebuild_tmp.get_loyalty_installs(aa.loyalty_installs, platform_rebuild_tmp.get_potential_downloads(f.b, aa.id)),
                                   0,
                                   1
                               ) AS total_audience,
                               aa.arpu
                          FROM platform_rebuild_tmp.platform_values_tags_fake_concepts_bin f
                         INNER JOIN platform_rebuild_tmp.audience_angle_tags aat
                            ON aat.tag_id_int = f.tag_id_int
                         INNER JOIN platform_rebuild_tmp.audience_angle aa
                            ON aa.id = aat.id
                           AND aa.angle_cnt = 2
                         INNER JOIN platform_rebuild_tmp.audience_angle_potential aap
                            ON aap.id = aa.id
                           AND aap.survey_id = f.survey_id
                          LEFT JOIN platform_calc_tmp.tmp_fake_concepts_rejected_per_angle_2_comb r
                            ON r.survey_id = aap.survey_id
                           AND r.id = aa.id
                           AND r.tag_id_int = f.tag_id_int
                       ) z1
               ) z2
        """)
        self.__conn.analyze("platform_rebuild_tmp.platform_values_tags_fake_concepts_tam")

    def __calc_loved_ratio_ext(self):
        query = f"""
        UPDATE platform_rebuild_tmp.audience_angle_potential aap
           SET aap.loved_ratio_ext = aap.loved_cnt / aap.total_cnt
        """
        self.__conn.query(query)
        self.__conn.commit()

        query = f"""
        UPDATE platform_rebuild_tmp.audience_angle_potential aap
         INNER JOIN app.def_sheet_platform_product d
            ON d.tag_id_int = aap.id
           AND d.loved_ratio_ext > 0
           SET aap.loved_ratio_ext = d.loved_ratio_ext, aap.loved_hack = 1
        """
        self.__conn.query(query)
        self.__conn.commit()

        query = """
        UPDATE platform_rebuild_tmp.audience_angle_potential aap
        INNER JOIN (
            SELECT z1.id, z1.survey_id, ROUND(AVG(z1.val), 4) AS loved_ratio_ext
            FROM (
            SELECT aa.id, aap1.survey_id, aap1.loved_ratio_ext * (aap2.loved_cnt / aap1.loved_cnt) AS val
              FROM platform_rebuild_tmp.audience_angle aa,
                   platform_rebuild_tmp.audience_angle_tags aat,
                   platform_rebuild_tmp.audience_angle_potential aap1,
                   platform_rebuild_tmp.audience_angle_potential aap2
             WHERE aa.angle_cnt = 2
               AND aa.id = aat.id
               AND aap2.id = aa.id
               AND aap1.id = aat.tag_id_int
               AND aap1.survey_id = aap2.survey_id
               ) z1 GROUP BY z1.id, z1.survey_id
           ) z2
           ON z2.id = aap.id AND z2.survey_id = aap.survey_id
           SET aap.loved_ratio_ext = z2.loved_ratio_ext
        """
        self.__conn.query(query)
        self.__conn.commit()

        self.__conn.query("""
        UPDATE platform_rebuild_tmp.audience_angle_potential aap
           SET aap.rejected_ratio_ext = ROUND(aap.loved_ratio_ext / (aap.loved_cnt / aap.total_cnt), 4)
         WHERE aap.loved_cnt > 0
        """)
        self.__conn.commit()

        # loyalty installs
        query = """
        UPDATE platform_rebuild_tmp.audience_angle_potential aap
         INNER JOIN platform_rebuild_tmp.audience_angle aa
            ON aa.id = aap.id
           SET aap.loyalty_installs = aa.installs * aap.loved_ratio_ext / 200
        """
        self.__conn.query(query)
        self.__conn.commit()

    # 2h
    def create_tam_per_app(self, app_ids_int: [int] = None):
        
        print("create_tam_per_app")

        suffix = ""
        prefix = "platform_rebuild_tmp"
        if app_ids_int is not None:
            suffix = "__single"
            prefix = "platform"

        limit = 50
        page = 0

        PlatformValuesHelper.recreate_table(
            conn=self.__conn,
            table_name="tmp_calc",
            schema="platform_calc_tmp",
            query=f"""
            CREATE TABLE x__table_name__x (
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

            self.__log_progress("TAM - Rejected")

            PlatformValuesHelper.recreate_table(
                conn=self.__conn,
                table_name="tmp_part",
                schema="platform_calc_tmp",
                query=f"""
                CREATE TABLE x__table_name__x (
                    id bigint unsigned not null,
                    constraint x__table_name__x_pk
                        primary key (id)
                    )
                    SELECT aap.id 
                      from {prefix}.audience_angle_potential aap,
                           {prefix}.audience_angle aa
                     where aap.id = aa.id
                       and aa.valid_combination = 1
                     order by aap.id
                    LIMIT {page * limit}, {limit}
                """
            )

            cnt = self.__conn.select_one("""
            select count(1) as cnt from platform_calc_tmp.tmp_part
            """)["cnt"]
            if cnt == 0:
                break

            page += 1

            apps_filter_q = ""
            if app_ids_int is not None:
                apps_filter_q = f""" 
                AND t.app_id_int IN ({self.__conn.values_arr_to_db_in(app_ids_int, int_values=True)})
                """

            query = f"""
                INSERT INTO platform_calc_tmp.tmp_calc (
                    id,
                    survey_id,
                    app_id_int,
                    rejected_cnt
                )
                    SELECT part.id, 
                           stb.survey_id, 
                           t.app_id_int, 
                           COUNT(1) AS rejected_cnt
                      FROM platform_calc_tmp.tmp_part part,
                           {prefix}.audience_angle act,
                           {prefix}.platform_values_tags_bin t,
                           {prefix}.platform_values_survey_tags_bin stb
                     WHERE part.id = act.id
                       AND BIT_COUNT(t.b & act.b) = act.angle_cnt
                       AND BIT_COUNT(stb.b_loved & act.b) = act.angle_cnt
                       AND BIT_COUNT(stb.b_rejected & t.b) > 0
                       {apps_filter_q}
                     GROUP BY 
                           part.id, 
                           stb.survey_id, 
                           t.app_id_int
                """

            self.__conn.query(query)
            self.__conn.commit()

        self.__conn.query(f"DROP TABLE IF EXISTS platform_rebuild_tmp.audience_angle_rejected_per_app{suffix}")

        self.__conn.query(f"""
        CREATE TABLE platform_rebuild_tmp.audience_angle_rejected_per_app{suffix} (
            id int not null, 
            survey_id int not null, 
            app_id_int int not null, 
            rejected_cnt int not null,
            constraint audience_angle_rejected_per_app_pk{suffix}
                primary key (id, survey_id, app_id_int)
        ) 
        SELECT id, survey_id, app_id_int, rejected_cnt FROM platform_calc_tmp.tmp_calc
        """)

        self.__conn.analyze(f"platform_rebuild_tmp.audience_angle_rejected_per_app{suffix}")

        PlatformValuesHelper.recreate_table(
            conn=self.__conn,
            table_name="tmp_calc",
            schema="platform_calc_tmp",
            query=f"""
            CREATE TABLE x__table_name__x (
                survey_id int unsigned not null,
                app_id_int int unsigned not null,
                angle_cnt int unsigned not null,
                total_audience bigint unsigned not null,
                tam bigint unsigned not null,
                tags_md5 binary(32) not null,
                max_angle_cnt int unsigned not null,
                constraint x__table_name__x_pk
                    primary key (survey_id, app_id_int, angle_cnt)
            )
            """
        )

        page = 0
        limit = 50

        self.__log_progress("TAM - Final")

        while True:

            apps_filter_q = ""
            if app_ids_int is not None:
                apps_filter_q = f"""
                AND a.app_id_int IN ({self.__conn.values_arr_to_db_in(app_ids_int, int_values=True)})
                """

            PlatformValuesHelper.recreate_table(
                conn=self.__conn,
                table_name="tmp_part",
                schema="platform_calc_tmp",
                query=f"""
                CREATE TABLE x__table_name__x (
                    app_id_int int unsigned not null,
                    tags_md5 binary(32) not null,
                    constraint x__table_name__x_pk
                        primary key (app_id_int)
                )
                SELECT a.app_id_int, t.tags_md5
                  from {prefix}.platform_values_apps a,
                       {prefix}.platform_values_tags_bin t
                 WHERE a.app_id_int = t.app_id_int
                  {apps_filter_q}
                 order by a.app_id_int
                 LIMIT {page * limit}, {limit}
                """
            )

            cnt = self.__conn.select_one("""
            select count(1) as cnt from platform_calc_tmp.tmp_part
            """)["cnt"]
            if cnt == 0:
                break

            page += 1

            PlatformValuesHelper.recreate_table(
                conn=self.__conn,
                table_name="tmp_app_with_rejected_tam_per_angle_id",
                schema="platform_calc_tmp",
                query=f"""
                CREATE TABLE x__table_name__x (
                    id bigint unsigned not null,
                    survey_id int unsigned not null,
                    app_id_int int unsigned not null,
                    total_audience bigint unsigned not null,
                    tam bigint unsigned not null,
                    constraint x__table_name__x_pk
                        primary key (id, survey_id, app_id_int)
                )
                SELECT z1.id, z1.survey_id, z1.app_id_int, z1.total_audience, platform.calc_tam(
                           z1.total_audience,
                       z1.arpu) AS tam
                  FROM (
                SELECT aa.id,
                       p.survey_id,
                       part.app_id_int,
                       platform.calc_total_audience(
                           p.loved_cnt,
                           p.total_cnt,
                           r.rejected_cnt,
                           tb.potential_downloads,
                           p.loyalty_installs,
                           p.loved_ratio_ext,
                           p.rejected_ratio_ext
                        ) as total_audience,
                        aa.arpu
                  FROM {prefix}.audience_angle_potential p
                 INNER JOIN {prefix}.audience_angle aa
                    ON aa.id = p.id
                   AND aa.valid_combination = 1
                 INNER JOIN platform_calc_tmp.tmp_part part
                 INNER JOIN platform_rebuild_tmp.audience_angle_rejected_per_app{suffix} r
                    ON r.app_id_int = part.app_id_int
                   AND r.survey_id = p.survey_id
                   AND r.id = p.id
                 INNER JOIN {prefix}.platform_values_tags_platform_bin tb
                    ON tb.app_id_int = part.app_id_int
                    ) z1
                """
            )

            self.__conn.query(f"""
            INSERT INTO platform_calc_tmp.tmp_calc 
            (survey_id, app_id_int, angle_cnt, max_angle_cnt, tam, tags_md5, total_audience)
            SELECT DISTINCT
                   z1.survey_id,
                   z1.app_id_int,
                   z1.angle_cnt,
                   FIRST_VALUE(z1.angle_cnt) over (PARTITION BY z1.survey_id, z1.app_id_int ORDER BY z1.total_audience DESC) as max_angle_cnt,
                   FIRST_VALUE(z1.tam) over (PARTITION BY z1.survey_id, z1.app_id_int, z1.angle_cnt ORDER BY z1.total_audience DESC) as tam,
                   FIRST_VALUE(z1.tags_md5) over (PARTITION BY z1.survey_id, z1.app_id_int, z1.angle_cnt ORDER BY z1.total_audience DESC) as tags_md5,
                   MAX(z1.total_audience) over (PARTITION BY z1.survey_id, z1.app_id_int, z1.angle_cnt) as total_audience
              FROM (
            SELECT r.survey_id, 
                   part.app_id_int, 
                   aa.angle_cnt,
                   r.total_audience,
                   r.tam,
                   part.tags_md5
              FROM platform_calc_tmp.tmp_part part
             INNER JOIN {prefix}.audience_angle aa
                ON aa.valid_combination = 1 
             INNER JOIN {prefix}.platform_values_tags_bin tb
                ON tb.app_id_int = part.app_id_int
               AND BIT_COUNT(tb.b & aa.b) = aa.angle_cnt
             INNER JOIN platform_calc_tmp.tmp_app_with_rejected_tam_per_angle_id r
                ON r.app_id_int = part.app_id_int
               AND r.id = aa.id
               ) z1
            """)
            self.__conn.commit()

        self.__conn.query(f"DROP TABLE IF EXISTS platform_rebuild_tmp.audience_angle_tam_per_app{suffix}")
        self.__conn.query(f"DROP TABLE IF EXISTS platform_rebuild_tmp.audience_angle_tam_per_app_state{suffix}")

        self.__conn.query(f"""
        CREATE TABLE platform_rebuild_tmp.audience_angle_tam_per_app{suffix} (
            app_id_int int not null, 
            survey_id int not null, 
            angle_cnt int not null, 
            total_audience bigint not null,
            tam bigint not null,
            tags_md5 binary(32) not null,
            max_angle_cnt int not null,
            constraint audience_angle_tam_per_app_pk{suffix}
                primary key (survey_id, app_id_int, angle_cnt)
        ) 
        SELECT app_id_int, survey_id, angle_cnt, total_audience, tam, tags_md5, max_angle_cnt
        FROM platform_calc_tmp.tmp_calc
        """)

        self.__conn.query(f"""
        CREATE TABLE platform_rebuild_tmp.audience_angle_tam_per_app_state{suffix} (
            app_id_int int not null, 
            tags_md5 binary(32) not null,
            constraint audience_angle_tam_per_app_pk{suffix}
                    primary key (app_id_int)
        )
        SELECT DISTINCT t.app_id_int, t.tags_md5
        FROM platform_calc_tmp.tmp_calc t
        """)

        self.__conn.analyze(f"platform_rebuild_tmp.audience_angle_tam_per_app{suffix}")
        self.__conn.analyze(f"platform_rebuild_tmp.audience_angle_tam_per_app_state{suffix}")

    @staticmethod
    def start_service_for_single_app(app_id_int: int):
        service_conn = DbConnection()
        service_conn.query("""
        INSERT INTO platform_values.requests_app_recalc (app_id_int)
        VALUES (%s)
        """, [app_id_int])
        service_conn.commit()
        service_conn.close()

        ServiceWrapperModel.run(
            d=ServiceWrapperModel.SERVICE_PLATFORM_VALUES_CALC
        )
        ServiceWrapperModel.update_service_shared_mem(f"service_platform_values__{rr.ENV}")

    def create_db_functions(self, tmp: bool):

        schema = "platform"
        if tmp:
            schema = "platform_rebuild_tmp"

        bin_cnt = self.__conn.select_one(f"""
        SELECT cnt FROM {schema}.bin_bytes_cnt
        """)["cnt"]

        self.__conn.query(f"""
        DROP FUNCTION IF EXISTS {schema}.get_app_platforms
        """)

        self.__conn.query(f"""
        CREATE FUNCTION {schema}.get_app_platforms(
            tags_b binary({bin_cnt}),
            audience_angle_id int
        )
        RETURNS binary({bin_cnt})
        DETERMINISTIC
        BEGIN

            DECLARE app_platforms BINARY({bin_cnt});

            SET app_platforms = NULL;

            IF audience_angle_id IS NOT NULL THEN
               SELECT aa.platform_b INTO app_platforms
                 FROM {schema}.audience_angle aa
                WHERE aa.id = audience_angle_id
                  AND aa.is_platform = 1;
            END IF;

            IF app_platforms IS NULL AND tags_b IS NOT NULL THEN
                SET app_platforms = tags_b;
            END IF; 

            RETURN app_platforms;
        END;
        """)

        self.__conn.query(f"""
        DROP FUNCTION IF EXISTS {schema}.get_potential_downloads
        """)

        self.__conn.query(f"""
        CREATE FUNCTION {schema}.get_potential_downloads(
            tags_b binary({bin_cnt}),
            audience_angle_id int
        )
        RETURNS bigint unsigned
        DETERMINISTIC
        BEGIN

            DECLARE potential_downloads bigint UNSIGNED;

            SET potential_downloads = NULL;

            IF audience_angle_id IS NOT NULL THEN
               SELECT aa.potential_downloads INTO potential_downloads
                 FROM {schema}.audience_angle aa
                WHERE aa.id = audience_angle_id
                  AND aa.is_platform = 1;
            END IF;

            IF potential_downloads IS NULL AND tags_b IS NOT NULL THEN
                SELECT z1.potential_downloads INTO potential_downloads
                FROM (
                SELECT SUM(d.potential_downloads) as potential_downloads
                  FROM {schema}.def_potential_downloads_bin d
                 WHERE BIT_COUNT(d.tags_b & tags_b) > 0 ) z1;
            END IF; 

            IF potential_downloads IS NULL OR potential_downloads = 0 THEN
               SET potential_downloads = {PlatformValuesHelper.POTENTIAL_DOWNLOADS_PLATFORM_WW};
            END IF;

            RETURN potential_downloads;
        END;
        """)

        self.__conn.query(f"""
        DROP FUNCTION IF EXISTS {schema}.get_potential_downloads_ratio
        """)

        self.__conn.query(f"""
        CREATE FUNCTION {schema}.get_potential_downloads_ratio(
        potential_downloads bigint unsigned
        )
        RETURNS decimal(5, 4)
        DETERMINISTIC
        BEGIN
            DECLARE potential_downloads_ratio decimal(5, 4);
            SET potential_downloads_ratio = ROUND(potential_downloads / {PlatformValuesHelper.POTENTIAL_DOWNLOADS_PLATFORM_WW}, 4);
            RETURN potential_downloads_ratio;
        END;
        """)

        self.__conn.query(f"""
        DROP FUNCTION IF EXISTS {schema}.get_loyalty_installs
        """)

        self.__conn.query(f"""
        CREATE FUNCTION {schema}.get_loyalty_installs(
        loyalty_installs bigint unsigned,
        potential_downloads bigint unsigned
        )
        RETURNS bigint unsigned
        DETERMINISTIC
        BEGIN
            DECLARE loyalty_installs_final bigint unsigned;
            SET loyalty_installs_final = FLOOR(loyalty_installs * {schema}.get_potential_downloads_ratio(potential_downloads));
            RETURN loyalty_installs_final;
        END;
        """)

    def copy_from_tmp_to_live(self):

        rows = self.__conn.select_all("""
        SELECT table_name as table_name
          FROM information_schema.tables
         WHERE table_schema = 'platform_rebuild_tmp'
        """)

        for row in rows:
            table_name = row["table_name"]

            self.__conn.query(f"""
            DROP TABLE IF EXISTS platform.{table_name}
            """)

            self.__conn.query(f"""
            ALTER TABLE platform_rebuild_tmp.{table_name} RENAME platform.{table_name}
            """)

            self.__conn.analyze(f"platform.{table_name}")

        self.create_db_functions(tmp=False)

    def rebuild_for_single_app(
            self,
            app_id_int: int
    ):
        PlatformValuesApps.run(conn=self.__conn, app_ids_int=[app_id_int])
        PlatformValuesTags.run_tags(conn=self.__conn, app_ids_int=[app_id_int])
        PlatformValuesTags.run_tags_bin(conn=self.__conn, app_ids_int=[app_id_int])

        self.create_tam_per_app(
            app_ids_int=[app_id_int]
        )

        for table in [
            "platform_values_apps",
            "audience_angle_tam_per_app",
            "audience_angle_tam_per_app_state",
            "audience_angle_rejected_per_app",
        ]:
            self.__conn.query(f"""
            DELETE FROM platform.{table} a
            WHERE a.app_id_int IN (
                SELECT aa.app_id_int FROM platform_rebuild_tmp.{table}__single aa
            )
            """)
            self.__conn.query(f"""
            INSERT INTO platform.{table}
            SELECT * FROM platform_rebuild_tmp.{table}__single
            """)
            self.__conn.commit()
            self.__conn.query(f"""
            DROP TABLE platform_rebuild_tmp.{table}__single
            """)

        for table in [
            "platform_values_tags",
            "platform_values_tags_platform",
            "platform_values_tags_bin",
            "platform_values_tags_platform_bin"
        ]:
            self.__conn.query(f"""
                    DELETE FROM platform.{table} a
                    WHERE a.app_id_int IN (
                        SELECT aa.app_id_int FROM platform_rebuild_tmp.{table}__single aa
                    )
                    """)
            self.__conn.query(f"""
                    INSERT INTO platform.{table}
                    SELECT * FROM platform_rebuild_tmp.{table}__single
                    """)
            self.__conn.commit()

            self.__conn.query(f"""
            DROP TABLE platform_rebuild_tmp.{table}__single
            """)
