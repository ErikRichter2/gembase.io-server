import datetime
import statistics

from server_core.db.db_connection import DbConnection
from src.db.base.base_db_model import BaseDbModel
from src.db.calc.calc_audience_angles_db_model import CalcAudienceAnglesDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.games_db_model import GamesDbModel
from src.models.user_obfuscator import UserObfuscator


class CalcCompetitorsDbModel(BaseDbModel):

    def __init__(self, conn: DbConnection):
        super(CalcCompetitorsDbModel, self).__init__(conn=conn)
        self.calc_utils = CalcUtils(conn=conn)
        
    def find_competitors_for_audience_angle(
            self,
            platform_id: int,
            survey_id: int,
            dev_id_int: int,
            my_tier: int | None,
            my_growth: int | None,
            my_store: int | None,
            my_tags_details: list[dict],
            audience_angle_id: int,
            audience_angle_platform_id: int,
            exclude_apps_from_competitors: list[int],
            advanced_filter: dict | None,
            skip_results_copy=False
    ):
        if my_tier is None:
            my_tier = 0
        if my_growth is None:
            my_growth = 0

        bin_bytes_cnt = self.calc_utils.get_bin_bytes_cnt()

        row_yearly_revenues = self.conn.select_one_or_none(f"""
        SELECT d.yearly_revenues
          FROM gb_platform.platform_values_devs d
         WHERE d.dev_id_int = {dev_id_int}
        """)
        yearly_revenues = 0
        if row_yearly_revenues is not None:
            yearly_revenues = row_yearly_revenues["yearly_revenues"]

        exclude_apps_from_competitors_cnd = ""
        if len(exclude_apps_from_competitors) > 0:
            exclude_apps_from_competitors_cnd = f" AND a.app_id_int NOT IN ({self.conn.values_arr_to_db_in(exclude_apps_from_competitors, int_values=True)}) "

        tags_ids = []
        for tag_detail in my_tags_details:
            tags_ids.append(tag_detail["tag_id_int"])
        if len(tags_ids) == 0:
            tags_ids.append(-1)
        tags_ids_db = self.conn.values_arr_to_db_in(tags_ids, int_values=True)

        self.calc_utils.recreate_table(
            table_name="tmp_my_tags",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                b BINARY({bin_bytes_cnt}) not null
            )
            SELECT z1.id, 
                   BIT_OR(z1.b) as b 
              FROM (
                    SELECT {audience_angle_id} as id, 
                           BIT_OR(d.b) as b
                      FROM gb_platform.def_tags_bin d
                     WHERE d.tag_id_int IN ({tags_ids_db})
                     UNION ALL
                    SELECT {audience_angle_id} as id, 
                           aa.b as b 
                      FROM gb_platform.audience_angle aa 
                     WHERE aa.id = {audience_angle_id}
                 ) z1
             GROUP BY z1.id
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_my_tags_subc_per_audience",
            query="""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                subcategory_int int unsigned not null,
                cnt int unsigned not null,
                constraint x__table_name__x_pk
                primary key (id, subcategory_int)
            )
            SELECT my_tags.id, 
                   p.subcategory_int, 
                   count(1) as cnt
              FROM gb_calc.tmp_my_tags my_tags,
                   gb_platform.def_tags_bin d,
                   gb_app.def_sheet_platform_product p
             WHERE bit_count(my_tags.b & d.b) = 1
               and d.tag_id_int = p.tag_id_int
             group by my_tags.id, p.subcategory_int
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_my_angle",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                angle_cnt int unsigned not null,
                b BINARY({bin_bytes_cnt}) not null,
                loved_cnt int unsigned not null,
                total_cnt int unsigned not null
            )
            SELECT aa.id, aa.angle_cnt, aa.b, aap.loved_cnt, aap.total_cnt
              FROM gb_platform.audience_angle aa,
                   gb_platform.audience_angle_potential aap
             WHERE aa.id = {audience_angle_id}
               AND aa.id = aap.id
            """
        )

        # todo kvoli temtporary
        self.calc_utils.recreate_table(
            table_name="tmp_my_angle_2",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                angle_cnt int unsigned not null,
                b BINARY({bin_bytes_cnt}) not null,
                loved_cnt int unsigned not null
            )
            SELECT aa.id, aa.angle_cnt, aa.b, aap.loved_cnt
              FROM gb_platform.audience_angle aa,
                   gb_platform.audience_angle_potential aap
             WHERE aa.id = {audience_angle_id}
               AND aa.id = aap.id
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_competitors_pool_w",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                subcategory_int int unsigned not null,
                competitors_pool_w int unsigned not null,
                constraint x__table_name__x_pk
                    primary key (subcategory_int)
            )
            SELECT DISTINCT 
                   d.subcategory_int,
                   d.competitors_pool_w
              FROM gb_platform.def_tags d
            """
        )

        app_stores_filter = ""
        stores = []

        if my_store is not None and my_store != 0:
            stores.append(my_store)

        if advanced_filter is not None:
            bulk_data = []
            for w in advanced_filter["weights"]:
                bulk_data.append((w["weight"], w['subcategory_int']))
            if len(bulk_data) > 0:
                self.conn.bulk("""
                UPDATE gb_calc.tmp_competitors_pool_w w
                   SET w.competitors_pool_w = %s
                 WHERE w.subcategory_int = %s
                """, bulk_data)
                self.conn.commit()

            if "current_store" in advanced_filter and not advanced_filter["current_store"]:
                if my_store in stores:
                    stores.remove(my_store)
            if len(advanced_filter["stores"]) > 0:
                for store_id in advanced_filter["stores"]:
                    if store_id not in stores:
                        stores.append(store_id)
        if len(stores) > 0:
            app_stores_filter = f" AND a.store IN ({self.conn.values_arr_to_db_in(stores, int_values=True)}) "

        if len(tags_ids) > 0:

            # rejected per angle
            self.calc_utils.recreate_table(
                table_name="tmp_rejected_per_angle",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    id bigint unsigned not null, 
                    rejected_cnt int unsigned not null,
                    constraint x__table_name__x_pk
                    primary key (id)
                )
                SELECT my_tags.id, 
                       count(1) as rejected_cnt
                  FROM gb_platform.audience_angle aa,
                       gb_platform.platform_values_survey_tags_bin stb,
                       gb_calc.tmp_my_tags my_tags
                 WHERE bit_count(stb.b_loved & aa.b) = aa.angle_cnt
                   AND bit_count(stb.b_hated & my_tags.b) > 0
                   AND stb.survey_id = {survey_id}
                   AND my_tags.id = aa.id
                 GROUP BY my_tags.id
                """
            )
        else:
            self.calc_utils.recreate_table(
                table_name="tmp_rejected_per_angle",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    id bigint unsigned not null, 
                    rejected_cnt int unsigned not null,
                    constraint x__table_name__x_pk
                    primary key (id)
                )
                """)

        # tam per angle
        self.calc_utils.recreate_table(
            table_name="tmp_tam_per_angle",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                tam bigint unsigned not null,
                constraint x__table_name__x_pk
                primary key (id)
            )
            SELECT aa.id, 
                   gb_platform.calc_tam(
                       gb_platform.calc_total_audience(
                           aap.loved_cnt, 
                           aap.total_cnt, 
                           IF(r.rejected_cnt IS NULL, 0, r.rejected_cnt), 
                           CAST(c.v1 AS UNSIGNED), 
                           aa.loyalty_installs, 0, 0
                       ), 
                       aa.arpu
                   ) AS tam
              FROM gb_platform.audience_angle aa
             INNER JOIN gb_platform.audience_angle_potential aap
                ON aap.id = aa.id
               AND aap.survey_id = {survey_id}
              LEFT JOIN gb_calc.tmp_rejected_per_angle r
                ON r.id = aa.id
             INNER JOIN gb_app.def_sheet_platform_values_constants c
                ON c.param = '{CalcUtils.POTENTIAL_DOWNLOADS_MOBILE_WW}'
             WHERE aa.id = {audience_angle_id}
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_survey_w",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                tag_id_int INT UNSIGNED NOT NULL,
                w INT UNSIGNED NOT NULL,
                CONSTRAINT x__table_name__x_pk
                PRIMARY KEY (tag_id_int)
            )
            SELECT z1.tag_id_int, 
                   ROUND(z1.loved_cnt / my_angle.loved_cnt * 100) as w
              FROM (
                    SELECT d.tag_id_int, 
                           COUNT(1) AS loved_cnt
                      FROM (
                            SELECT stb.survey_instance_int
                              FROM gb_calc.tmp_my_angle my_angle,
                                   gb_platform.platform_values_survey_tags_bin stb,
                                   gb_calc.tmp_my_tags my_tags
                             WHERE stb.survey_id = {survey_id}
                               AND BIT_COUNT(stb.b_loved & my_angle.b) = my_angle.angle_cnt
                               AND BIT_COUNT(stb.b_hated & my_tags.b) = 0
                           ) z1,
                           gb_platform.platform_values_survey_tags st,
                           gb_platform.def_tags d
                     WHERE st.survey_meta_id = {survey_id}
                       AND st.survey_instance_int = z1.survey_instance_int
                       AND st.tag_id_int = d.tag_id_int
                       AND st.loved = 1
                     GROUP BY d.tag_id_int
                   ) z1,
                   gb_calc.tmp_my_angle_2 my_angle
            """
        )

        # discount per node

        self.calc_utils.recreate_table(
            table_name="tmp_discount_per_audience_angle",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id int unsigned not null,
                quality int unsigned default 40 not null,
                quality_full int unsigned default 40 not null,
                quality_portfolio int unsigned default 0 not null,
                released_years int unsigned default 0 not null,
                constraint x__table_name__x_pk
                primary key (id)
            )
            select {audience_angle_id} as id
            """
        )

        self.conn.query(f"""
        UPDATE gb_calc.tmp_discount_per_audience_angle aa
        INNER JOIN (
                    SELECT z3.id, 
                           MAX(z3.quality_portfolio) as quality_portfolio,
                           ROUND(AVG(z3.quality)) AS quality
                      FROM (
                            SELECT z2.id, 
                                   z2.quality,
                                   MAX(z2.quality_portfolio) OVER (PARTITION BY z2.id) as quality_portfolio,
                                   ROW_NUMBER() OVER (PARTITION BY z2.id ORDER BY z2.quality) AS row_num,
                                   COUNT(1) OVER (PARTITION BY z2.id) AS total_num
                              FROM (
                                    SELECT aa.id,
                                           apps.rating * IF(BIT_COUNT(tb.b & aa.b) = aa.angle_cnt, 1, 0.75) AS quality,
                                           IF(BIT_COUNT(tb.b & aa.b) = aa.angle_cnt, 1, 0) as quality_portfolio
                                      FROM (
                                            SELECT a.app_id_int, 
                                                   a.rating
                                            FROM gb_platform.platform_values_devs_apps da,
                                                 gb_platform.platform_values_apps a
                                           WHERE da.dev_id_int = {dev_id_int}
                                             AND da.app_id_int = a.app_id_int
                                             AND a.tier >= 3
                                           ) apps,
                                           gb_platform.platform_values_tags_bin tb,
                                           gb_platform.audience_angle aa
                                     WHERE tb.app_id_int = apps.app_id_int
                                       AND aa.id = {audience_angle_id}
                                   ) z2
                           ) z3
                     WHERE z3.row_num IN (FLOOR((z3.total_num + 1) / 2), FLOOR((z3.total_num + 2) / 2) ) 
                     GROUP BY z3.id 
                   ) z4
         ON z4.id = aa.id
        SET aa.quality = z4.quality,
            aa.quality_portfolio = z4.quality_portfolio
        """)
        self.conn.commit()

        self.conn.query(f"""
        UPDATE gb_calc.tmp_discount_per_audience_angle aa
        INNER JOIN (
                    SELECT z3.id, 
                           ROUND(AVG(z3.quality_full)) AS quality_full
                      FROM (
                            SELECT z2.id, 
                                   z2.quality_full,
                                   ROW_NUMBER() OVER (PARTITION BY z2.id ORDER BY z2.quality_full) AS row_num,
                                   COUNT(1) OVER (PARTITION BY z2.id) AS total_num
                              FROM (
                                    SELECT aa.id,
                                           apps.rating as quality_full
                                      FROM (
                                            SELECT a.app_id_int, 
                                                   a.rating
                                              FROM gb_platform.platform_values_devs_apps da,
                                                   gb_platform.platform_values_apps a
                                             WHERE da.dev_id_int = {dev_id_int}
                                               AND da.app_id_int = a.app_id_int
                                               AND a.tier >= 3
                                           ) apps,
                                           gb_platform.platform_values_tags_bin tb,
                                           gb_platform.audience_angle aa
                                     WHERE tb.app_id_int = apps.app_id_int
                                       AND aa.id = {audience_angle_id}
                                   ) z2
                           ) z3
                     WHERE z3.row_num IN (FLOOR((z3.total_num + 1) / 2), FLOOR((z3.total_num + 2) / 2) ) 
                     GROUP BY z3.id 
                   ) z4
         ON z4.id = aa.id
        SET aa.quality_full = z4.quality_full
        """)
        self.conn.commit()

        self.conn.query(f"""
        UPDATE gb_calc.tmp_discount_per_audience_angle aa
        INNER JOIN (
                    SELECT z3.id, MAX(z3.released_years) AS released_years
                      FROM (
                            SELECT z2.id, 
                                   z2.released_years
                              FROM (
                                    SELECT aa.id,
                                           apps.released_years
                                      FROM (
                                            SELECT a.app_id_int, 
                                                   a.released_years
                                              FROM gb_platform.platform_values_devs_apps da,
                                                   gb_platform.platform_values_apps a
                                             WHERE da.dev_id_int = {dev_id_int}
                                               AND da.app_id_int = a.app_id_int
                                               AND a.tier >= 3
                                           ) apps,
                                           gb_platform.platform_values_tags_bin tb,
                                           gb_platform.audience_angle aa
                                     WHERE tb.app_id_int = apps.app_id_int
                                       AND BIT_COUNT(tb.b & aa.b) = aa.angle_cnt
                                       AND aa.id = {audience_angle_id}
                                   ) z2
                           ) z3
                       GROUP BY z3.id
                   ) z4
         ON z4.id = aa.id
        SET aa.released_years = z4.released_years
        """)
        self.conn.commit()

        competitors_pool_query = f"""
        FROM gb_calc.tmp_my_angle my_angle,
             gb_platform.platform_values_tags_bin tb,
             gb_platform.platform_values_apps a
       WHERE BIT_COUNT(tb.b & my_angle.b) = my_angle.angle_cnt
         AND a.app_id_int = tb.app_id_int
         AND (
                 (a.tier = {my_tier} - 1 AND a.growth >= 0.15 * {my_growth})
              OR (a.tier >= {my_tier})
              OR (a.growth >= 0.15 * {my_growth})
             )
        {exclude_apps_from_competitors_cnd}
        {app_stores_filter}
        """

        self.calc_utils.recreate_table(
            table_name="tmp_competitors_pool_cnt",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                audience_angle_id bigint unsigned not null,
                competitors_cnt int UNSIGNED NOT NULL,
                CONSTRAINT x__table_name__x_pk 
                    PRIMARY KEY (audience_angle_id)
            )
            SELECT my_angle.id as audience_angle_id, count(1) as competitors_cnt
            {competitors_pool_query}
            GROUP BY my_angle.id
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_competitors_pool",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                app_id_int int UNSIGNED NOT NULL,
                score int NOT NULL,
                CONSTRAINT x__table_name__x_pk 
                PRIMARY KEY (app_id_int)
            )
            SELECT z2.id, 
                   z2.app_id_int, 
                   round(z2.score) as score
              FROM (
                    SELECT z1.id, 
                           z1.app_id_int, 
                           SUM(CAST(z1.score1 as SIGNED) - CAST((z1.score2 + z1.score3) as SIGNED)) AS score
                      FROM (
                            SELECT pool.id, 
                                   pool.app_id_int,   
                                   3 * BIT_COUNT(tb.b & my_tags.b & d.b) * pool_w.competitors_pool_w * survey_w.w AS score1,
                                   BIT_COUNT(tb.b & d.b) * pool_w.competitors_pool_w * survey_w.w AS score2,
                                   BIT_COUNT(my_tags.b & d.b) * pool_w.competitors_pool_w * survey_w.w AS score3
                              FROM (
                                    SELECT my_angle.id,  a.app_id_int
                                    {competitors_pool_query}                                    
                                    GROUP BY my_angle.id, a.app_id_int
                           ) pool,
                           gb_platform.platform_values_tags_bin tb,
                           gb_calc.tmp_my_tags my_tags,
                           gb_calc.tmp_survey_w AS survey_w,
                           gb_platform.def_tags_bin d,
                           gb_app.def_sheet_platform_product p,
                           gb_calc.tmp_competitors_pool_w pool_w
                     WHERE pool.app_id_int = tb.app_id_int
                       AND survey_w.tag_id_int = d.tag_id_int
                       AND p.tag_id_int = d.tag_id_int
                       AND pool_w.subcategory_int = p.subcategory_int
                   ) z1
               GROUP BY z1.id, z1.app_id_int
             ) z2
         ORDER BY z2.score DESC
         LIMIT 50
        """
        )

        competitors_count = self.conn.select_one("""
        SELECT count(1) as cnt 
          FROM gb_calc.tmp_competitors_pool
        """)["cnt"]

        if competitors_count == 0:
            return 0

        # threat score - similarity

        self.calc_utils.recreate_table(
            table_name="tmp_ts_similarity",
            query=f"""
                    CREATE TABLE gb_calc.x__table_name__x (
                        id int unsigned not null,
                        app_id_int int unsigned NOT NULL,
                        w smallint unsigned not null,
                        color TINYINT NOT NULL,
                        constraint x__table_name__x_pk
                           primary key (id, app_id_int)
                    )
                    SELECT z3.id, 
                     z3.app_id_int, 
                     round(z3.w) as w, 
                     IF(tp.color = 'r', -1, (IF(tp.color = 'g', 1, 0))) as color 
                FROM (
                      SELECT z2.id, 
                             z2.app_id_int, 
                             SUM(z2.w) AS w
                        FROM (
                              SELECT z1.id, 
                                     z1.app_id_int, 
                                     (z1.same_cnt / GREATEST(m_all_cnt, c_all_cnt)) * z1.def_w AS w
                                FROM (
                                      SELECT pool.id, 
                                             pool.app_id_int, 
                                             BIT_COUNT(t.b & def_subc.b) AS c_all_cnt, 
                                             my_tags_subc.cnt AS m_all_cnt, 
                                             BIT_COUNT(t.b & def_subc.b & my_tags.b) AS same_cnt,
                                             subc_w.w AS def_w
                                        FROM gb_calc.tmp_competitors_pool pool,
                                             gb_platform.platform_values_tags_bin t,
                                             gb_platform.def_tags_subc_bin def_subc,
                                             gb_calc.tmp_my_tags my_tags,
                                             gb_calc.tmp_my_tags_subc_per_audience my_tags_subc,
                                             (
                                              SELECT p.subcategory_int,
                                                     p.threatscore_similarity_w AS w
                                                FROM gb_app.def_sheet_platform_product p
                                               WHERE p.threatscore_similarity_w != 0
                                               GROUP BY 
                                                     p.subcategory_int, 
                                                     p.threatscore_similarity_w
                                             ) subc_w
                                       WHERE my_tags.id = pool.id
                                         AND pool.app_id_int = t.app_id_int
                                         AND my_tags.id = my_tags_subc.id
                                         AND my_tags_subc.subcategory_int = subc_w.subcategory_int
                                         AND def_subc.subcategory_int = my_tags_subc.subcategory_int
                                     ) z1
                               WHERE z1.m_all_cnt + z1.c_all_cnt > 0
                             ) z2
                         GROUP BY 
                               z2.id, 
                               z2.app_id_int 
                      ) z3,
                      gb_app.def_sheet_platform_threats_params tp
                WHERE tp.param = 'similar_color'
                  AND CAST(tp.v1 AS UNSIGNED) <= round(z3.w)
                  AND round(z3.w) <= CAST(tp.v2 AS UNSIGNED)   
                                """
        )

        # threat score - trend, size, quality
        self.calc_utils.recreate_table(
            table_name="tmp_ts_tqs",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint UNSIGNED NOT NULL,
                app_id_int MEDIUMINT UNSIGNED NOT NULL,
                trend_w SMALLINT UNSIGNED NOT NULL,
                trend_color TINYINT NOT NULL,
                quality_w SMALLINT UNSIGNED NOT NULL,
                quality_color TINYINT NOT NULL,
                size_w SMALLINT UNSIGNED NOT NULL,
                size_color TINYINT NOT NULL,
                growth_w smallint unsigned not null,
                growth_color tinyint not null,
                constraint x__table_name__x_pk
                primary key (id, app_id_int)
            )
            SELECT z1.id, 
                   z1.app_id_int, 
                   z1.trend_w, 
                   z1.trend_color,
                   z1.quality_w, 
                   z1.quality_color, 
                   z1.size_w, 
                   z1.size_color,
                   CAST(growth.v3 AS UNSIGNED ) AS growth_w,
                   IF(growth.color = 'r', -1, IF(growth.color = 'g', 1, 0)) AS growth_color
              FROM (
                    SELECT pool.id, 
                           pool.app_id_int,
                           CAST(trend.v3 AS UNSIGNED ) AS trend_w,
                           IF(trend.color = 'r', -1, IF(trend.color = 'g', 1, 0)) AS trend_color,
                           CAST(quality.v3 AS UNSIGNED ) AS quality_w,
                           IF(quality.color = 'r', -1, IF(quality.color = 'g', 1, 0)) AS quality_color,
                           CAST(size.v3 AS UNSIGNED ) AS size_w,
                           IF(size.color = 'r', -1, IF(size.color = 'g', 1, 0)) AS size_color,
                           round(IF({my_growth} = 0, -1, ac.growth / {my_growth} * 100)) AS growth_ratio
                      FROM gb_calc.tmp_competitors_pool pool,
                           gb_platform.platform_values_apps ac,
                           gb_app.def_sheet_platform_threats_params trend,
                           gb_app.def_sheet_platform_threats_params quality,
                           gb_app.def_sheet_platform_threats_params size
                     WHERE pool.app_id_int = ac.app_id_int
                       AND trend.param = 'trend'
                       AND ac.released_years >= CAST(trend.v1 as UNSIGNED)
                       AND ac.released_years <= CAST(trend.v2 as UNSIGNED)
                       AND quality.param = 'quality'
                       AND ac.rating >= CAST(quality.v1 as UNSIGNED)
                       AND ac.rating <= CAST(quality.v2 as UNSIGNED)
                       AND size.param = 'size'
                       AND CAST(ac.tier as SIGNED) - {my_tier} >= CAST(size.v1 as SIGNED)
                       AND CAST(ac.tier as SIGNED) - {my_tier} <= CAST(size.v2 as SIGNED)
                   ) z1,
                   gb_app.def_sheet_platform_threats_params growth
             WHERE growth.param = 'growth'
               AND (
                    (
                         z1.growth_ratio < 0 
                     AND growth.v2 IS NULL
                    )
                    OR 
                    (
                         z1.growth_ratio >= 0
                     AND round(z1.growth_ratio) >= CAST(growth.v1 AS UNSIGNED)                 
                     AND (growth.v2 IS NULL OR round(z1.growth_ratio) <= CAST(growth.v2 AS UNSIGNED))   
                    )
                   )    
            """
        )

        # rejected per angle
        self.calc_utils.recreate_table(
            table_name="tmp_rejected_per_app_per_angle",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                app_id_int int unsigned not null,
                rejected_cnt int unsigned not null,
                constraint x__table_name__x_pk
                primary key (id, app_id_int)
            )
            SELECT pool.id, 
                   pool.app_id_int, 
                   count(1) as rejected_cnt
              FROM gb_calc.tmp_competitors_pool pool,
                   gb_calc.tmp_my_angle my_angle,
                   gb_platform.platform_values_tags_bin tb,
                   gb_platform.platform_values_survey_tags_bin stb
             WHERE tb.app_id_int = pool.app_id_int
               AND stb.survey_id = {survey_id}
               AND BIT_COUNT(tb.b & my_angle.b) = my_angle.angle_cnt
               AND BIT_COUNT(stb.b_loved & my_angle.b) = my_angle.angle_cnt
               AND BIT_COUNT(stb.b_hated & tb.b) > 0
             group by pool.id, pool.app_id_int
            """
        )

        # threat score - tam
        self.calc_utils.recreate_table(
            table_name="tmp_ts_competitor_tam",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                app_id_int int unsigned not null,
                tam bigint unsigned not null,
                constraint x__table_name__x_pk
                primary key (app_id_int)
            )
            SELECT pool.app_id_int,
                   gb_platform.calc_tam(
                       gb_platform.calc_total_audience(
                           p.loved_cnt,
                           p.total_cnt,
                           IF(r.rejected_cnt IS NULL, 0, r.rejected_cnt),
                           CAST(pvc.v1 AS UNSIGNED),
                           aac.loyalty_installs, 0, 0
                       ),
                       aac.arpu
                   ) AS tam
              FROM gb_platform.audience_angle_potential p
             INNER JOIN gb_platform.audience_angle aac
                ON aac.id = p.id
             INNER JOIN gb_calc.tmp_competitors_pool pool
              LEFT JOIN gb_calc.tmp_rejected_per_app_per_angle r
                ON r.app_id_int = pool.app_id_int
               AND r.id = p.id
             INNER JOIN gb_app.def_sheet_platform_values_constants pvc
                ON pvc.param = '{CalcUtils.POTENTIAL_DOWNLOADS_MOBILE_WW}'
             WHERE p.id = {audience_angle_id}
               AND p.survey_id = {survey_id}
            """
        )
        self.calc_utils.recreate_table(
            table_name="tmp_ts_tam",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                app_id_int MEDIUMINT NOT NULL,
                tam_w SMALLINT UNSIGNED NOT NULL,
                tam_color TINYINT NOT NULL,
                tam_raw bigint unsigned not null,
                no_data TINYINT DEFAULT 0 NOT NULL,
                constraint x__table_name__x_pk
                    primary key (id, app_id_int)
            )
            SELECT z2.id, 
                   z2.app_id_int,
                   CAST(tp.v3 AS UNSIGNED) AS tam_w,
                   IF (tp.color = 'r', -1, IF (tp.color = 'g', 1, 0)) AS tam_color,
                   z2.no_data,
                   z2.c_tam as tam_raw
              FROM (
                    SELECT z1.id, 
                           z1.app_id_int,
                           z1.c_tam,
                           round(IF(z1.my_tam = 0, -1, z1.c_tam / z1.my_tam * 100)) AS tam_ratio,
                           IF(z1.my_tam = 0, 1, 0) as no_data
                      FROM (
                            SELECT pool.id, 
                                   pool.app_id_int, 
                                   tam_per_app.tam as c_tam,
                                   my_tam.tam as my_tam
                              FROM gb_calc.tmp_competitors_pool pool,
                                   gb_calc.tmp_ts_competitor_tam tam_per_app,
                                   gb_calc.tmp_tam_per_angle my_tam
                             WHERE my_tam.id = pool.id
                               AND pool.app_id_int = tam_per_app.app_id_int
                           ) z1
                   ) z2,
                   gb_app.def_sheet_platform_threats_params tp
             WHERE tp.param = 'tam'
               AND (
                    (
                         z2.tam_ratio < 0 
                     AND tp.v2 IS NULL
                    )
                    OR 
                    (
                         z2.tam_ratio >= 0
                     AND round(z2.tam_ratio) >= CAST(tp.v1 AS UNSIGNED)                 
                     AND (tp.v2 IS NULL OR round(z2.tam_ratio) <= CAST(tp.v2 AS UNSIGNED))   
                    )
                   )    
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_ts_final",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                app_id_int int not null,
                competitor_score smallint not null,
                threat_score smallint unsigned not null,
                threat_score_discount int unsigned not null,
                similarity_w smallint unsigned not null,
                similarity_c tinyint not null,
                trend_w smallint unsigned not null,
                trend_c tinyint not null,
                quality_w smallint unsigned not null,
                quality_c tinyint not null,
                quality_full smallint unsigned not null,
                quality_portfolio tinyint unsigned not null,
                size_w smallint unsigned not null,
                size_c tinyint not null,
                growth_w smallint unsigned not null,
                growth_c tinyint not null,
                tam_w smallint unsigned not null,
                tam_c tinyint not null,
                tam_raw bigint unsigned not null,
                no_data TINYINT DEFAULT 0 NOT NULL,
                constraint x__table_name__x_pk
                    primary key (id, app_id_int)
            )
            SELECT pool.id, 
                   pool.app_id_int,
                   round(0) AS competitor_score,
                   ROUND((
                           CAST(p_similar.v2 AS UNSIGNED) * IF(s.w IS NULL, 0, s.w) +
                           CAST(p_size.v2 AS UNSIGNED) * IF(tqs.size_w IS NULL, 0, tqs.size_w) +
                           CAST(p_growth.v2 AS UNSIGNED) * IF(tqs.growth_w IS NULL, 0, tqs.growth_w) +
                           CAST(p_tam.v2 AS UNSIGNED) * IF(tam.tam_w IS NULL, 0, tam.tam_w) +
                           CAST(p_quality.v2 AS UNSIGNED) * IF(tqs.quality_w IS NULL, 0, tqs.quality_w) +
                           CAST(p_trend.v2 AS UNSIGNED) * IF(tqs.trend_w IS NULL, 0, tqs.trend_w)
                         ) / 100, 0) AS threat_score,
                   gb_platform.calc_discount(
                       {yearly_revenues},
                       IF(ds.quality is null, 0, ds.quality),
                       IF(ds.released_years is null, 0, ds.released_years)
                   ) as threat_score_discount,
                   IF(ds.quality_full is null, 0, ds.quality_full) as quality_full,
                   IF(ds.quality_portfolio is null, 0, ds.quality_portfolio) as quality_portfolio,
                   IF(s.w IS NULL, 0, s.w) AS similarity_w,
                   IF(s.color IS NULL, 0, s.color) AS similarity_c,
                   IF(tqs.trend_w IS NULL, 0, tqs.trend_w) AS trend_w,
                   IF(tqs.trend_color IS NULL, 0, tqs.trend_color)  AS trend_c,
                   IF(tqs.quality_w IS NULL, 0, tqs.quality_w)  AS quality_w,
                   IF(tqs.quality_color IS NULL, 0, tqs.quality_color)  AS quality_c,
                   IF(tqs.size_w IS NULL, 0, tqs.size_w)  AS size_w,
                   IF(tqs.size_color IS NULL, 0, tqs.size_color) AS size_c,
                   IF(tqs.growth_w IS NULL, 0, tqs.growth_w)  AS growth_w,
                   IF(tqs.growth_color IS NULL, 0, tqs.growth_color) AS growth_c,
                   IF(tam.tam_w IS NULL, 0, tam.tam_w) AS tam_w,
                   IF(tam.tam_color IS NULL, 0, tam.tam_color) AS tam_c,
                   tam.tam_raw,
                   IF(tam.no_data = 1, 1, 0) as no_data
              FROM gb_calc.tmp_competitors_pool pool
              LEFT JOIN gb_calc.tmp_discount_per_audience_angle ds
                ON ds.id = pool.id
              LEFT JOIN gb_calc.tmp_ts_similarity s 
                ON s.id = pool.id
               AND s.app_id_int = pool.app_id_int
              LEFT JOIN gb_calc.tmp_ts_tqs tqs 
                ON tqs.id = pool.id
               AND tqs.app_id_int = pool.app_id_int
              LEFT JOIN gb_calc.tmp_ts_tam tam 
                ON tam.id = pool.id
               AND tam.app_id_int = pool.app_id_int
             INNER JOIN gb_app.def_sheet_platform_threats_params p_similar
                ON p_similar.param = 'final_score_weight'
               AND p_similar.v1 = 'similar' 
             INNER JOIN gb_app.def_sheet_platform_threats_params p_size
                ON p_size.param = 'final_score_weight'
               AND p_size.v1 = 'size' 
             INNER JOIN gb_app.def_sheet_platform_threats_params p_growth
                ON p_growth.param = 'final_score_weight'
               AND p_growth.v1 = 'growth' 
             INNER JOIN gb_app.def_sheet_platform_threats_params p_tam
                ON p_tam.param = 'final_score_weight'
               AND p_tam.v1 = 'tam' 
             INNER JOIN gb_app.def_sheet_platform_threats_params p_quality
                ON p_quality.param = 'final_score_weight'
               AND p_quality.v1 = 'quality' 
             INNER JOIN gb_app.def_sheet_platform_threats_params p_trend
                ON p_trend.param = 'final_score_weight'
               AND p_trend.v1 = 'trend'
            """
        )

        if not skip_results_copy:
            self.__copy_data_to_results(
                platform_id=platform_id,
                audience_angle_id=audience_angle_id,
                audience_angle_platform_id=audience_angle_platform_id,
                dev_id_int=dev_id_int
            )

        return competitors_count

    def __copy_data_to_results(
            self,
            platform_id: int,
            dev_id_int: int, 
            audience_angle_id: int,
            audience_angle_platform_id: int
    ):

        self.conn.query("""
        DELETE FROM gb_calc.results_audience_angles__competitors r
         WHERE r.platform_id = %s 
           AND r.audience_angle_id = %s
        """, [platform_id, audience_angle_id])

        self.conn.query("""
        INSERT INTO gb_calc.results_audience_angles__competitors
        (platform_id, dev_id_int, audience_angle_id, audience_angle_platform_id, app_id_int, competitor_score, 
        threat_score, similarity_w, similarity_c, trend_w, trend_c, quality_w, quality_c, tam_w, 
        tam_c, size_w, size_c, growth_w, growth_c, quality_portfolio, quality_full, tam_raw)
        SELECT %s as platform_id, %s as dev_id_int, %s as audience_angle_id, %s as audience_angle_platform_id,
               f.app_id_int, f.competitor_score, f.threat_score, f.similarity_w, f.similarity_c,
               f.trend_w, f.trend_c, f.quality_w, f.quality_c, f.tam_w, f.tam_c, f.size_w, f.size_c,
               f.growth_w, f.growth_c, f.quality_portfolio, f.quality_full, f.tam_raw
          FROM gb_calc.tmp_ts_final f
        """, [platform_id, dev_id_int, audience_angle_id, audience_angle_platform_id])

        self.conn.query("""
        DELETE FROM gb_calc.results_competitors_cnt
         WHERE platform_id = %s
        """, [platform_id])

        self.conn.query("""
        INSERT INTO gb_calc.results_competitors_cnt (platform_id, audience_angle_id, competitors_cnt)
        SELECT %s as platform_id, audience_angle_id, competitors_cnt
          FROM gb_calc.tmp_competitors_pool_cnt 
        """, [platform_id])
        
        self.conn.commit()

    def generate_client_data(
            self,
            platform_id: int,
            obfuscator: UserObfuscator
    ) -> dict:

        rows = self.conn.select_all("""
        SELECT r.platform_id, r.audience_angle_id, r.audience_angle_platform_id, r.app_id_int, r.competitor_score, 
               r.threat_score, r.similarity_w, r.similarity_c, r.size_w, r.size_c, r.growth_w, r.growth_c, 
               r.tam_w, r.tam_c, 
               r.quality_w, r.quality_c, r.trend_w, r.trend_c, a.title, a.icon, a.platform,
               a.store, a.app_id_in_store, a.url, m.app_id_in_store as app_id_in_store_raw,
               IF (p_size.name is NULL, '', p_size.name) as size_name,
               IF (p_growth.name is NULL, '', p_growth.name) as growth_name,
               IF (p_quality.name is NULL, '', p_quality.name) as quality_name,
               IF (p_trend.name is NULL, '', p_trend.name) as trend_name,
               IF (p_tam.name is NULL, '', p_tam.name) as tam_name,
               TIMESTAMPDIFF(YEAR, a.released, CURDATE()) as released_years,
               r.dev_id_int, r.quality_portfolio, r.quality_full,
               p.growth, p.rating, p.installs,
               YEAR(a.released) as release_year,
               r.tam_raw,
               IF (cc.competitors_cnt IS NULL, 0, cc.competitors_cnt) as competitors_pool_cnt
          FROM gb_calc.results_audience_angles__competitors r
         INNER JOIN gb_platform.platform_values_apps p
            ON p.app_id_int = r.app_id_int
         INNER JOIN gb_scraped_data.apps_v a 
            ON a.app_id_int = r.app_id_int
         INNER JOIN gb_app.map_app_id_to_store_id m
            ON m.app_id_int = a.app_id_int
          LEFT JOIN gb_app.def_sheet_platform_threats_params p_size
            ON p_size.param = 'size'
           AND CAST(p_size.v3 as UNSIGNED) = r.size_w
          LEFT JOIN gb_app.def_sheet_platform_threats_params p_growth
            ON p_growth.param = 'growth'
           AND CAST(p_growth.v3 as UNSIGNED) = r.growth_w
          LEFT JOIN gb_app.def_sheet_platform_threats_params p_quality
            ON p_quality.param = 'quality'
           AND CAST(p_quality.v3 as UNSIGNED) = r.quality_w
          LEFT JOIN gb_app.def_sheet_platform_threats_params p_trend
            ON p_trend.param = 'trend'
           AND CAST(p_trend.v3 as UNSIGNED) = r.trend_w
          LEFT JOIN gb_app.def_sheet_platform_threats_params p_tam
            ON p_tam.param = 'tam'
           AND CAST(p_tam.v3 as UNSIGNED) = r.tam_w
          LEFT JOIN gb_calc.results_competitors_cnt cc
            ON cc.platform_id = r.platform_id
           AND cc.audience_angle_id = r.audience_angle_id
         WHERE r.platform_id = %s 
         ORDER BY r.threat_score DESC, r.competitor_score DESC, a.loyalty_installs DESC
        """, [platform_id])

        rows_tags = self.conn.select_all("""
        SELECT z1.app_id_int, t.tag_id_int
          FROM (
                SELECT DISTINCT r.app_id_int
                  FROM gb_calc.results_audience_angles__competitors r
                 WHERE r.platform_id = %s
               ) z1,
               gb_platform.platform_values_tags t
         WHERE t.app_id_int = z1.app_id_int
        """, [platform_id])

        rows_def_w = self.conn.select_all("""
        SELECT p.v1 as group_id, CAST(p.v2 as UNSIGNED) as w
          FROM gb_app.def_sheet_platform_threats_params p
         WHERE p.param = 'final_score_weight'
        """)
        threat_score_def_w = {}
        for row in rows_def_w:
            threat_score_def_w[row["group_id"]] = row["w"]

        tags_per_app_id = {}
        for row in rows_tags:
            app_id_int = row[UserObfuscator.APP_ID_INT]
            if app_id_int not in tags_per_app_id:
                tags_per_app_id[app_id_int] = []
            tags_per_app_id[app_id_int].append(row[UserObfuscator.TAG_ID_INT])

        res = {
            "platform_id": platform_id,
            "discounts": [],
            "ts_items": []
        }

        discount_groups = ["qua_angle", "exp", "rev"]

        company_title = None
        quality_portfolio = 0
        discount_sum = 0
        ts_items_per_gr = {}
        competitors_pool_cnt = 0
        
        games_db = GamesDbModel(conn=self.conn)

        if len(rows) > 0:
            
            row_audience_angle_platform_id = rows[0]["audience_angle_platform_id"]
            row_audience_angle_id = rows[0]["audience_angle_id"]

            audience_angle = CalcAudienceAnglesDbModel(conn=self.conn).generate_client_data(
                platform_id=row_audience_angle_platform_id,
                exclusive_angle_id=row_audience_angle_id
            )
            if len(audience_angle) > 0:
                res["audience_detail"] = audience_angle[0]

            quality_portfolio = rows[0]["quality_portfolio"]
            competitors_pool_cnt = rows[0]["competitors_pool_cnt"]

            if rows[0]["dev_id_int"] != 0:
                row_company_title = self.conn.select_one_or_none("""
                SELECT d.title 
                  FROM gb_scraped_data.devs d 
                 WHERE d.dev_id_int = %s
                """, [rows[0]["dev_id_int"]])
                if row_company_title is not None:
                    res["company_title"] = row_company_title["title"]

            row_discount = self.conn.select_one("""
            SELECT r.qua_d as qua_angle, r.exp_d as exp, r.rev_d as rev
              FROM gb_calc.results_audience_angles__final r
             WHERE r.platform_id = %s 
               AND r.audience_angle_id = %s
            """, [row_audience_angle_platform_id, row_audience_angle_id])
            row_discount_calc = self.conn.select_one("""
            SELECT gb_platform.calc_discount_experience(%s) as exp,
                   gb_platform.calc_discount_quality(%s) as qua_angle,
                   gb_platform.calc_discount_quality(%s) as qua_full,
                   gb_platform.calc_discount_revenues(%s) as rev
            """, [row_discount["exp"], row_discount["qua_angle"], rows[0]["quality_full"], row_discount["rev"]])

            if row_discount_calc is not None:
                def discountColor(k: str):
                    v = row_discount_calc[k]
                    if k == "qua_angle":
                        return "r" if v < 6 else "g" if v > 8 else ""
                    if k == "exp":
                        return "r" if v < 2 else "g" if v > 6 else ""
                    if k == "rev":
                        return "r" if v < 3 else "g" if v > 10 else ""
                    return ""
                res["discounts"] = [{
                    "gr": k,
                    "ts_perc": round(row_discount_calc[k]),
                    "ts_raw": row_discount[k],
                    "c": discountColor(k)
                } for k in discount_groups]

            for k in discount_groups:
                discount_sum += round(row_discount_calc[k])

            res["qua_full"] = row_discount_calc["qua_full"]

            res["competitor_apps_details"] = []

            for row in rows:

                app_id_int = row[UserObfuscator.APP_ID_INT]

                app_detail = {
                    UserObfuscator.APP_ID_INT: app_id_int,
                    "title": row["title"],
                    "app_id_in_store": row["app_id_in_store"],
                    "app_store_url": games_db.get_store_url(
                        url=row["url"],
                        app_id_in_store=row["app_id_in_store_raw"],
                        app_type=GamesDbModel.APP_TYPE__STORE,
                        store=row["store"]
                    ),
                    "icon": row["icon"],
                    "platform": row["platform"],
                    "store": row["store"],
                    "locked": False
                }

                res["competitor_apps_details"].append(app_detail)

                if app_id_int in tags_per_app_id:
                    app_detail[UserObfuscator.TAG_IDS_INT] = tags_per_app_id[app_id_int]

                ts_data = {
                    UserObfuscator.APP_ID_INT: app_id_int,
                    "ts": 0,
                    "ts_groups": [],
                    "quality": row["rating"],
                    "novelty": row["released_years"],
                    "growth": row["growth"]
                }

                res["ts_items"].append(ts_data)

                def color2str(c: int) -> str:
                    if c == -1:
                        return "r"
                    elif c == 1:
                        return "g"
                    return ""

                ts_data["ts_groups"] = [
                    {
                        "gr": "similar",
                        "ts_raw": row["similarity_w"],
                        "c": color2str(row["similarity_c"]),
                        "raw_val": row['similarity_w']
                    },
                    {
                        "gr": "size",
                        "ts_raw": row["size_w"],
                        "ts_name": row["size_name"],
                        "c": color2str(row["size_c"]),
                        "installs": row["installs"],
                        "raw_val": row['installs']
                    },
                    {
                        "gr": "growth",
                        "ts_raw": row['growth_w'],
                        "ts_name": row["growth_name"],
                        "c": color2str(row["growth_c"]),
                        "growth": row["growth"],
                        "raw_val": row['growth']
                    },
                    {
                        "gr": "trend",
                        "ts_raw": row['trend_w'],
                        "ts_name": row["trend_name"],
                        "c": color2str(row["trend_c"]),
                        "released": datetime.datetime.now().year - row["released_years"],
                        "raw_val": row['release_year'],
                        "locked": False
                    },
                    {
                        "gr": "quality",
                        "ts_raw": row['quality_w'],
                        "ts_name": row["quality_name"],
                        "c": color2str(row["quality_c"]),
                        "rating": row['rating'],
                        "raw_val": row['rating']
                    },
                    {
                        "gr": "tam",
                        "ts_raw": row['tam_w'],
                        "ts_name": row["tam_name"],
                        "c": color2str(row["tam_c"]),
                        "tam": row["tam_raw"],
                        "raw_val": int(row['tam_raw'])
                    }
                ]

                for ts_score in ts_data["ts_groups"]:
                    gr = ts_score["gr"]
                    if gr not in ts_items_per_gr:
                        ts_items_per_gr[gr] = []
                    ts_items_per_gr[gr].append(ts_score)
                    ts_score["ts_perc"] = round(
                        ts_score["ts_raw"] / 100 * threat_score_def_w[gr]
                    )

                ts_final = int(row["threat_score"]) - discount_sum
                if ts_final > 100:
                    ts_final = 100
                if ts_final < 0:
                    ts_final = 0

                ts_data["ts"] = ts_final

        if quality_portfolio != 0:
            res["quality_portfolio"] = quality_portfolio

        res["ts_items"].sort(key=lambda x: x["ts"], reverse=True)
        res["ts_items"] = res["ts_items"][:50]

        res["competitors_count"] = len(res["ts_items"])
        res["competitors_pool_cnt"] = competitors_pool_cnt

        rows_ts_params = self.conn.select_all("""
        SELECT p.param, p.v3, p.name
          FROM gb_app.def_sheet_platform_threats_params p
         WHERE p.param IN ('similar', 'size', 'growth', 'quality', 'trend', 'tam')
        """)
        def get_param_name(p: str, w: int) -> str:
            for row in rows_ts_params:
                    if row["param"] == p:
                        if int(row["v3"]) == w:
                            return row["name"]
            return ""

        median_per_ts_group = []
        for gr in ts_items_per_gr:
            m_arr = []
            for ts_score in ts_items_per_gr[gr]:
                m_arr.append(ts_score["ts_raw"])
            if len(m_arr) > 0:
                m_arr.sort(reverse=True)
                m = round(statistics.median(m_arr))
                median_per_ts_group.append({
                    "gr": gr,
                    "m": m,
                    "n": get_param_name(gr, m)
                })

        res["median_per_ts_group"] = median_per_ts_group

        #return threat_score_result
        return res
