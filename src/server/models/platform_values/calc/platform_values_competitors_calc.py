import statistics
from datetime import datetime

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.apps.app_model import AppModel
from src.server.models.user.user_obfuscator import UserObfuscator
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.tags.tags_constants import TagsConstants


class PlatformValuesCompetitorsCalc:

    @staticmethod
    def find_competitors_for_audience_angle(
            conn: DbConnection,
            platform_id: int,
            survey_id: int,
            dev_id_int: int,
            my_tier: int | None,
            my_growth: int | None,
            my_tags_details: [],
            audience_angle_row_id: int,
            exclude_apps_from_competitors: [],
            tags_weights: [] = None,
            skip_results_copy=False,
            multi_tags=None
    ):
        if multi_tags is None:
            multi_tags = [
                {
                    "multi_id": 0,
                    "tag_details": my_tags_details,
                    "audience_angle_row_id": audience_angle_row_id
                }
            ]

        if my_tier is None:
            my_tier = 0
        if my_growth is None:
            my_growth = 0

        bin_bytes_cnt = PlatformValuesHelper.get_bin_bytes_cnt(conn)

        exclude_apps_from_competitors_cnd = ""
        if len(exclude_apps_from_competitors) > 0:
            exclude_apps_from_competitors_cnd = f" WHERE a.app_id_int NOT IN ({conn.values_arr_to_db_in(exclude_apps_from_competitors, int_values=True)}) "

        query = """
        CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                tag_id_int int not null,
                tag_rank int not null,
                constraint x__table_name__x_pk
                primary key (multi_id, tag_id_int)
            )
        """
        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_tags",
            query=query
        )

        bulk_data = []
        for it in multi_tags:
            for tag_detail in it["tag_details"]:
                bulk_data.append((it["multi_id"], tag_detail[UserObfuscator.TAG_ID_INT], tag_detail["tag_rank"]))
        conn.bulk("""
        INSERT INTO platform_values.tmp_my_tags
        VALUES (%s, %s, %s)
        """, bulk_data)
        conn.commit()

        query = """
        CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                audience_angle_row_id int not null,
                audience_angle_id int not null,
                constraint x__table_name__x_pk
                primary key (multi_id)
            )
        """
        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_angles_base",
            query=query
        )
        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_angles_base_2",
            query=query
        )
        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_angles_base_3",
            query=query
        )

        bulk_data = []
        for it in multi_tags:
            bulk_data.append((it["multi_id"], it["audience_angle_row_id"]))
        conn.bulk("""
        INSERT INTO platform_values.tmp_my_angles_base
        SELECT %s as multi_id, f.row_id, f.audience_angle_id
        FROM platform_values.results_audience_angles__final f
        WHERE f.row_id = %s
        """, bulk_data)
        conn.query("""
        INSERT INTO platform_values.tmp_my_angles_base_2
        select * from platform_values.tmp_my_angles_base
        """)
        conn.query("""
        INSERT INTO platform_values.tmp_my_angles_base_3
        select * from platform_values.tmp_my_angles_base
        """)
        conn.commit()

        query = f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                b BINARY({bin_bytes_cnt}) not null,
                ranked_genres_b BINARY({bin_bytes_cnt}) not null,
                subgenres_b BINARY({bin_bytes_cnt}) not null,
                constraint x__table_name__x_pk
                primary key (multi_id)
            )
            SELECT z1.multi_id,
                   BIT_OR(z1.b) as b,
                   BIT_OR(z1.ranked_genres_b) as ranked_genres_b,
                   BIT_OR(z1.subgenres_b) as subgenres_b
              FROM (
                    SELECT my_angles.multi_id,
                           d.b as b,
                           IF(my_tags.tag_rank IN ({TagsConstants.TAG_RANK_PRIMARY}, {TagsConstants.TAG_RANK_SECONDARY}) AND d_genres_bin.tag_id_int IS NOT NULL, d_genres_bin.b, zero.b) as ranked_genres_b,
                           IF(dd.subgenre = 1, d.b, zero.b) as subgenres_b
                      FROM platform_values.tmp_my_tags my_tags
                     INNER JOIN platform_values.tmp_my_angles_base my_angles
                        ON my_angles.multi_id = my_tags.multi_id
                     INNER JOIN platform.def_tags_bin d
                        ON d.tag_id_int = my_tags.tag_id_int
                     INNER JOIN platform.zero_bin_value zero
                     INNER JOIN app.def_sheet_platform_product dd
                        ON dd.tag_id_int = d.tag_id_int
                     LEFT JOIN platform.def_tags d_genres
                       ON d_genres.tag_id_int = my_tags.tag_id_int
                      AND d_genres.subcategory_int = {TagsConstants.SUBCATEGORY_GENRE_ID} 
                     LEFT JOIN platform.def_tags_bin d_genres_bin
                        ON d_genres_bin.tag_id_int = d_genres.tag_id_int
                 ) z1
             GROUP BY z1.multi_id
            """

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_tags_bin",
            query=query
        )

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_tags_bin_2",
            query=query
        )

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_tags_subc_per_audience",
            query="""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                subcategory_int int unsigned not null,
                cnt int unsigned not null,
                constraint x__table_name__x_pk
                primary key (multi_id, subcategory_int)
            )
            SELECT my_tags.multi_id,
                   p.subcategory_int, 
                   count(1) as cnt
              FROM platform_values.tmp_my_tags my_tags,
                   platform.def_tags_bin d,
                   app.def_sheet_platform_product p
             WHERE my_tags.tag_id_int = d.tag_id_int
               and d.tag_id_int = p.tag_id_int
             group by my_tags.multi_id, p.subcategory_int
            """
        )

        for i in range(2):
            PlatformValuesHelper.recreate_table(
                conn=conn,
                table_name=f"tmp_my_angle_{i}",
                query=f"""
                CREATE TABLE platform_values.x__table_name__x (
                    multi_id int not null,
                    angle_cnt int unsigned not null,
                    b BINARY({bin_bytes_cnt}) not null,
                    loved_cnt int unsigned not null,
                    total_cnt int unsigned not null,
                    constraint x__table_name__x_pk
                    primary key (multi_id)
                )
                SELECT m.multi_id, aa.angle_cnt, aa.b, aap.loved_cnt, aap.total_cnt
                  FROM platform.audience_angle aa,
                       platform.audience_angle_potential aap,
                       platform_values.tmp_my_angles_base m
                 WHERE aa.id = m.audience_angle_id
                   AND aa.id = aap.id
                """
            )

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_competitors_pool_w",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                subcategory_int int unsigned not null,
                competitors_pool_w int unsigned not null,
                constraint x__table_name__x_pk
                    primary key (subcategory_int)
            )
            SELECT DISTINCT 
                   d.subcategory_int,
                   d.competitors_pool_w
              FROM platform.def_tags d
            """
        )

        if tags_weights is not None:
            bulk_data = []
            for w in tags_weights:
                bulk_data.append((w["weight"], w['subcategory_int']))
            if len(bulk_data) > 0:
                conn.bulk("""
                UPDATE platform_values.tmp_competitors_pool_w w
                   SET w.competitors_pool_w = %s
                 WHERE w.subcategory_int = %s
                """, bulk_data)
                conn.commit()

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_survey_w",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                tag_id_int INT UNSIGNED NOT NULL,
                w INT UNSIGNED NOT NULL,
                CONSTRAINT x__table_name__x_pk
                PRIMARY KEY (multi_id, tag_id_int)
            )
            SELECT z1.multi_id, 
                   z1.tag_id_int,
                   ROUND(z1.loved_cnt / aap.loved_cnt * 100) as w
              FROM (
                    SELECT z1.multi_id,
                           d.tag_id_int, 
                           COUNT(1) AS loved_cnt
                      FROM (
                            SELECT my_angle.multi_id,
                                   my_angle.audience_angle_id,
                                   stb.survey_instance_int
                              FROM platform_values.tmp_my_angles_base my_angle,
                                   platform.audience_angle aa,
                                   platform.platform_values_survey_tags_bin stb,
                                   platform_values.tmp_my_tags_bin my_tags_bin
                             WHERE stb.survey_id = {survey_id}
                               AND BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
                               AND BIT_COUNT(stb.b_rejected & my_tags_bin.b) = 0
                               AND my_angle.multi_id = my_tags_bin.multi_id
                               AND my_angle.audience_angle_id = aa.id
                           ) z1,
                           platform.platform_values_survey_tags st,
                           platform.def_tags d
                     WHERE st.survey_meta_id = {survey_id}
                       AND st.survey_instance_int = z1.survey_instance_int
                       AND st.tag_id_int = d.tag_id_int
                       AND st.loved = 1
                     GROUP BY z1.multi_id, d.tag_id_int
                   ) z1,
                   platform.audience_angle_potential aap
                   WHERE aap.id = z1.tag_id_int
                     AND aap.survey_id = {survey_id}
            """
        )

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_tags_platform_bin",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                b BINARY({bin_bytes_cnt}) not null,
                cnt int not null,
                constraint x__table_name__x_pk
                primary key (multi_id)
            )
            SELECT z1.multi_id, BIT_OR(z1.b) as b, count(1) as cnt
             FROM (
            SELECT my_angles.multi_id, pb.b as b
              FROM platform.def_tags p,
                   platform.def_tags_bin pb,
                   platform_values.tmp_my_angles_base my_angles,
                   platform_values.tmp_my_tags my_tags
             WHERE p.subcategory_int = {TagsConstants.SUBCATEGORY_PLATFORMS_ID}
               AND p.tag_id_int = my_tags.tag_id_int
               AND p.tag_id_int = pb.tag_id_int
               AND my_angles.multi_id = my_tags.multi_id
               AND my_angles.audience_angle_id NOT IN ({TagsConstants.PLATFORM_PC}, {TagsConstants.PLATFORM_MOBILE})
            UNION
            SELECT my_angles.multi_id, pb.b
              FROM platform.def_tags p,
                   platform.def_tags_bin pb,
                   platform_values.tmp_my_angles_base_2 my_angles
             WHERE p.subcategory_int = {TagsConstants.SUBCATEGORY_PLATFORMS_ID}
               AND p.tag_id_int = pb.tag_id_int
               AND p.tag_id_int = my_angles.audience_angle_id
               AND my_angles.audience_angle_id IN ({TagsConstants.PLATFORM_PC}, {TagsConstants.PLATFORM_MOBILE})
            UNION
            SELECT my_angles.multi_id, z.b
              FROM platform.zero_bin_value z,
                   platform_values.tmp_my_angles_base_3 my_angles
            ) z1
            GROUP BY z1.multi_id
            """
        )

        competitors_pool_query = f"""
        FROM platform.platform_values_apps a
  INNER JOIN platform_values.tmp_my_angle_0 my_angle
  INNER JOIN platform.platform_values_tags_bin tb
          ON a.app_id_int = tb.app_id_int
         AND BIT_COUNT(tb.b & my_angle.b) = my_angle.angle_cnt
  INNER JOIN platform_values.tmp_my_tags_bin my_tags
          ON tb.ranked_genres_b & my_tags.ranked_genres_b = my_tags.ranked_genres_b
         AND (BIT_COUNT(tb.subgenres_b & my_tags.subgenres_b) > 0 OR BIT_COUNT(my_tags.subgenres_b) = 0)
         AND my_angle.multi_id = my_tags.multi_id
  INNER JOIN ###__POOL_TABLE__### cc
          ON cc.multi_id = my_angle.multi_id
         AND (
                 (a.tier = ({my_tier} - (cc.tier_val + 1)) AND a.growth >= (0.15 - (cc.growth_val / 100)) * {my_growth})
              OR (a.tier >= {my_tier} - cc.tier_val)
              OR (a.growth >= (0.15 - (cc.growth_val / 100)) * {my_growth})
             )
     INNER JOIN platform.platform_values_tags_platform_bin tpb
        ON a.app_id_int = tpb.app_id_int
     INNER JOIN platform_values.tmp_my_tags_platform_bin my_platform_bin
        ON (BIT_COUNT(my_platform_bin.b) = 0 OR BIT_COUNT(tpb.b & my_platform_bin.b) > 0)
        AND my_platform_bin.multi_id = my_angle.multi_id
  {exclude_apps_from_competitors_cnd}
        """

        competitors_cnt = 50
        iteration_cnt = 10
        iteration_tier_val = 0
        iteration_growth_val = 0

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_competitors_pool_cnt",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                competitors_cnt int UNSIGNED NOT NULL,
                tier_val int not null,
                growth_val int not null,
                CONSTRAINT x__table_name__x_pk 
                    PRIMARY KEY (multi_id)
            )
            SELECT 
                my_angle.multi_id, 
                0 as competitors_cnt,
                0 as tier_val,
                0 as growth_val
            FROM platform_values.tmp_my_angles_base my_angle
            """
        )

        while True:

            PlatformValuesHelper.recreate_table(
                conn=conn,
                table_name="tmp_competitors_pool_cnt_iteration",
                query=f"""
                CREATE TABLE platform_values.x__table_name__x (
                    multi_id int not null,
                    tier_val int not null ,
                    growth_val int not null,
                    CONSTRAINT x__table_name__x_pk 
                        PRIMARY KEY (multi_id)
                )
                SELECT multi_id, 
                {iteration_tier_val} as tier_val, 
                {iteration_growth_val} as growth_val
                  FROM platform_values.tmp_competitors_pool_cnt
                 WHERE competitors_cnt < {competitors_cnt}
                """
            )

            competitors_pool_query_iterations = competitors_pool_query

            competitors_pool_query_iterations = competitors_pool_query_iterations.replace(
                "###__POOL_TABLE__###", "platform_values.tmp_competitors_pool_cnt_iteration")

            conn.query(f"""
            UPDATE platform_values.tmp_competitors_pool_cnt pool
            INNER JOIN (
                SELECT my_angle.multi_id, count(1) as cnt
                {competitors_pool_query_iterations}
                GROUP BY my_angle.multi_id
            ) z1 ON z1.multi_id = pool.multi_id
            SET pool.competitors_cnt = z1.cnt,
                pool.tier_val = {iteration_tier_val},
                pool.growth_val = {iteration_growth_val}
            """)

            conn.commit()

            needs_it_cnt = conn.select_one(f"""
            SELECT count(1) as cnt FROM platform_values.tmp_competitors_pool_cnt
            WHERE competitors_cnt < {competitors_cnt}
            """)["cnt"]

            if needs_it_cnt == 0:
                break

            iteration_cnt -= 1
            if iteration_cnt <= 0:
                break

            iteration_growth_val += 1
            iteration_tier_val += 1

        competitors_pool_query_final = competitors_pool_query

        competitors_pool_query_final = competitors_pool_query_final.replace(
            "###__POOL_TABLE__###", "platform_values.tmp_competitors_pool_cnt")

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_competitors_pool",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                app_id_int int UNSIGNED NOT NULL,
                score int NOT NULL,
                CONSTRAINT x__table_name__x_pk 
                PRIMARY KEY (multi_id, app_id_int)
            )
            SELECT *
              FROM (
            SELECT z2.multi_id,
                   z2.app_id_int,
                   ROW_NUMBER() over (partition by z2.multi_id order by z2.score DESC) as row_num,
                   round(z2.score) as score
              FROM (
                    SELECT z1.multi_id,
                           z1.app_id_int, 
                           SUM(CAST(z1.score1 as SIGNED) - CAST((z1.score2 + z1.score3) as SIGNED)) AS score
                      FROM (
                            SELECT pool.multi_id,
                                   pool.app_id_int,   
                                   3 * BIT_COUNT(tb.b & my_tags.b & d.b) * pool_w.competitors_pool_w * survey_w.w AS score1,
                                   BIT_COUNT(tb.b & d.b) * pool_w.competitors_pool_w * survey_w.w AS score2,
                                   BIT_COUNT(my_tags.b & d.b) * pool_w.competitors_pool_w * survey_w.w AS score3
                              FROM (
                                    SELECT my_angle.multi_id, a.app_id_int
                                    {competitors_pool_query_final}                                    
                                    GROUP BY my_angle.multi_id, a.app_id_int
                                   ) pool,
                                   platform.platform_values_tags_bin tb,
                                   platform_values.tmp_my_tags_bin_2 my_tags,
                                   platform_values.tmp_survey_w AS survey_w,
                                   platform.def_tags_bin d,
                                   app.def_sheet_platform_product p,
                                   platform_values.tmp_competitors_pool_w pool_w
                             WHERE pool.app_id_int = tb.app_id_int
                               AND survey_w.tag_id_int = d.tag_id_int
                               AND p.tag_id_int = d.tag_id_int
                               AND pool_w.subcategory_int = p.subcategory_int
                               AND my_tags.multi_id = pool.multi_id
                               AND survey_w.multi_id = pool.multi_id
                   ) z1
               GROUP BY z1.multi_id, z1.app_id_int
             ) z2
             ) z3
         where z3.row_num <= 50
         ORDER BY z3.multi_id, z3.score DESC
        """
        )

        # threat score - similarity

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_ts_similarity",
            query=f"""
                    CREATE TABLE platform_values.x__table_name__x (
                        multi_id int not null,
                        app_id_int int unsigned NOT NULL,
                        w smallint unsigned not null,
                        color TINYINT NOT NULL,
                        constraint x__table_name__x_pk
                           primary key (multi_id, app_id_int)
                    )
                    SELECT z3.multi_id, 
                     z3.app_id_int, 
                     round(z3.w) as w, 
                     IF(tp.color = 'r', -1, (IF(tp.color = 'g', 1, 0))) as color 
                FROM (
                      SELECT z2.multi_id, 
                             z2.app_id_int, 
                             SUM(z2.w) AS w
                        FROM (
                              SELECT z1.multi_id, 
                                     z1.app_id_int, 
                                     (z1.same_cnt / GREATEST(m_all_cnt, c_all_cnt)) * z1.def_w AS w
                                FROM (
                                      SELECT pool.multi_id, 
                                             pool.app_id_int, 
                                             BIT_COUNT(t.b & def_subc.b) AS c_all_cnt, 
                                             my_tags_subc.cnt AS m_all_cnt, 
                                             BIT_COUNT(t.b & def_subc.b & my_tags_bin.b) AS same_cnt,
                                             subc_w.w AS def_w
                                        FROM platform_values.tmp_competitors_pool pool,
                                             platform.platform_values_tags_bin t,
                                             platform.def_tags_subc_bin def_subc,
                                             platform_values.tmp_my_tags_bin my_tags_bin,
                                             platform_values.tmp_my_tags_subc_per_audience my_tags_subc,
                                             (
                                              SELECT p.subcategory_int,
                                                     p.threatscore_similarity_w AS w
                                                FROM app.def_sheet_platform_product p
                                               WHERE p.threatscore_similarity_w != 0
                                               GROUP BY 
                                                     p.subcategory_int, 
                                                     p.threatscore_similarity_w
                                             ) subc_w
                                       WHERE my_tags_bin.multi_id = pool.multi_id
                                         AND pool.app_id_int = t.app_id_int
                                         AND my_tags_bin.multi_id = my_tags_subc.multi_id
                                         AND my_tags_subc.subcategory_int = subc_w.subcategory_int
                                         AND def_subc.subcategory_int = my_tags_subc.subcategory_int
                                     ) z1
                               WHERE z1.m_all_cnt + z1.c_all_cnt > 0
                             ) z2
                         GROUP BY 
                               z2.multi_id, 
                               z2.app_id_int 
                      ) z3,
                      app.def_sheet_platform_threats_params tp
                WHERE tp.param = 'similar_color'
                  AND CAST(tp.v1 AS UNSIGNED) <= round(z3.w)
                  AND round(z3.w) <= CAST(tp.v2 AS UNSIGNED)   
                                """
        )

        # threat score - trend, size, quality
        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_ts_tqs",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int NOT NULL,
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
                primary key (multi_id, app_id_int)
            )
            SELECT z1.multi_id, 
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
                    SELECT pool.multi_id, 
                           pool.app_id_int,
                           CAST(trend.v3 AS UNSIGNED ) AS trend_w,
                           IF(trend.color = 'r', -1, IF(trend.color = 'g', 1, 0)) AS trend_color,
                           CAST(quality.v3 AS UNSIGNED ) AS quality_w,
                           IF(quality.color = 'r', -1, IF(quality.color = 'g', 1, 0)) AS quality_color,
                           CAST(size.v3 AS UNSIGNED ) AS size_w,
                           IF(size.color = 'r', -1, IF(size.color = 'g', 1, 0)) AS size_color,
                           round(IF({my_growth} = 0, -1, ac.growth / {my_growth} * 100)) AS growth_ratio
                      FROM platform_values.tmp_competitors_pool pool,
                           platform.platform_values_apps ac,
                           app.def_sheet_platform_threats_params trend,
                           app.def_sheet_platform_threats_params quality,
                           app.def_sheet_platform_threats_params size
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
                   app.def_sheet_platform_threats_params growth
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
        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_rejected_per_app_per_angle",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id bigint unsigned not null,
                app_id_int int unsigned not null,
                rejected_cnt int unsigned not null,
                constraint x__table_name__x_pk
                primary key (multi_id, app_id_int)
            )
            SELECT pool.multi_id, 
                   pool.app_id_int, 
                   count(1) as rejected_cnt
              FROM platform_values.tmp_competitors_pool pool,
                   platform_values.tmp_my_angle_0 my_angle,
                   platform.platform_values_tags_bin tb,
                   platform.platform_values_survey_tags_bin stb
             WHERE tb.app_id_int = pool.app_id_int
               AND my_angle.multi_id = pool.multi_id
               AND stb.survey_id = {survey_id}
               AND BIT_COUNT(tb.b & my_angle.b) = my_angle.angle_cnt
               AND BIT_COUNT(stb.b_loved & my_angle.b) = my_angle.angle_cnt
               AND BIT_COUNT(stb.b_rejected & tb.b) > 0
             group by pool.multi_id, pool.app_id_int
            """
        )

        # threat score - tam
        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_ts_competitor_tam",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                app_id_int int unsigned not null,
                tam bigint unsigned not null,
                constraint x__table_name__x_pk
                primary key (multi_id, app_id_int)
            )
            SELECT pool.multi_id, pool.app_id_int,
                   platform.calc_tam(
                       platform.calc_total_audience(
                           p.loved_cnt,
                           p.total_cnt,
                           IF(r.rejected_cnt IS NULL, 0, r.rejected_cnt),
                           tb.potential_downloads,
                           platform.get_loyalty_installs(p.loyalty_installs, tb.potential_downloads),
                           p.loved_ratio_ext,
                           p.rejected_ratio_ext
                       ),
                       aac.arpu
                   ) AS tam
              FROM platform_values.tmp_my_angles_base my_angles
             INNER JOIN platform.audience_angle_potential p
                ON p.id = my_angles.audience_angle_id
                AND p.survey_id = {survey_id}
             INNER JOIN platform.audience_angle aac
                ON aac.id = p.id
             INNER JOIN platform_values.tmp_competitors_pool pool
               ON my_angles.multi_id = pool.multi_id
              LEFT JOIN platform_values.tmp_rejected_per_app_per_angle r
                ON r.app_id_int = pool.app_id_int
               AND r.multi_id = pool.multi_id
             INNER JOIN platform.platform_values_tags_platform_bin tb
                ON tb.app_id_int = pool.app_id_int
            """
        )
        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_ts_tam",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                app_id_int MEDIUMINT NOT NULL,
                tam_w SMALLINT UNSIGNED NOT NULL,
                tam_color TINYINT NOT NULL,
                tam_raw bigint unsigned not null,
                no_data TINYINT DEFAULT 0 NOT NULL,
                constraint x__table_name__x_pk
                    primary key (multi_id, app_id_int)
            )
            SELECT z2.multi_id, 
                   z2.app_id_int,
                   CAST(tp.v3 AS UNSIGNED) AS tam_w,
                   IF (tp.color = 'r', -1, IF (tp.color = 'g', 1, 0)) AS tam_color,
                   z2.no_data,
                   z2.c_tam as tam_raw
              FROM (
                    SELECT z1.multi_id, 
                           z1.app_id_int,
                           z1.c_tam,
                           round(IF(z1.my_tam = 0, -1, z1.c_tam / z1.my_tam * 100)) AS tam_ratio,
                           IF(z1.my_tam = 0, 1, 0) as no_data
                      FROM (
                            SELECT pool.multi_id, 
                                   pool.app_id_int, 
                                   tam_per_app.tam as c_tam,
                                   platform.calc_tam(
                                       my_tam.total_audience,
                                       my_tam.arpu
                                   ) as my_tam
                              FROM platform_values.tmp_competitors_pool pool,
                                   platform_values.tmp_ts_competitor_tam tam_per_app,
                                   platform_values.tmp_my_angles_base my_angles,
                                   platform_values.results_audience_angles__final my_tam
                             WHERE my_tam.row_id = my_angles.audience_angle_row_id
                               AND my_angles.multi_id = pool.multi_id
                               AND tam_per_app.multi_id = pool.multi_id
                               AND pool.app_id_int = tam_per_app.app_id_int
                           ) z1
                   ) z2,
                   app.def_sheet_platform_threats_params tp
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

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_ts_final",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                audience_angle_row_id int not null,
                app_id_int int not null,
                competitor_score smallint not null,
                threat_score smallint unsigned not null,
                similarity_w smallint unsigned not null,
                similarity_c tinyint not null,
                trend_w smallint unsigned not null,
                trend_c tinyint not null,
                quality_w smallint unsigned not null,
                quality_c tinyint not null,
                size_w smallint unsigned not null,
                size_c tinyint not null,
                growth_w smallint unsigned not null,
                growth_c tinyint not null,
                tam_w smallint unsigned not null,
                tam_c tinyint not null,
                tam_raw bigint unsigned not null,
                no_data TINYINT DEFAULT 0 NOT NULL,
                constraint x__table_name__x_pk
                    primary key (multi_id, app_id_int)
            )
            SELECT pool.multi_id, 
                   my_angles.audience_angle_row_id,
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
              FROM platform_values.tmp_competitors_pool pool
              inner join platform_values.tmp_my_angles_base my_angles
                ON my_angles.multi_id = pool.multi_id
              LEFT JOIN platform_values.tmp_ts_similarity s 
                ON s.multi_id = pool.multi_id
               AND s.app_id_int = pool.app_id_int
              LEFT JOIN platform_values.tmp_ts_tqs tqs 
                ON tqs.multi_id = pool.multi_id
               AND tqs.app_id_int = pool.app_id_int
              LEFT JOIN platform_values.tmp_ts_tam tam 
                ON tam.multi_id = pool.multi_id
               AND tam.app_id_int = pool.app_id_int
             INNER JOIN app.def_sheet_platform_threats_params p_similar
                ON p_similar.param = 'final_score_weight'
               AND p_similar.v1 = 'similar' 
             INNER JOIN app.def_sheet_platform_threats_params p_size
                ON p_size.param = 'final_score_weight'
               AND p_size.v1 = 'size' 
             INNER JOIN app.def_sheet_platform_threats_params p_growth
                ON p_growth.param = 'final_score_weight'
               AND p_growth.v1 = 'growth' 
             INNER JOIN app.def_sheet_platform_threats_params p_tam
                ON p_tam.param = 'final_score_weight'
               AND p_tam.v1 = 'tam' 
             INNER JOIN app.def_sheet_platform_threats_params p_quality
                ON p_quality.param = 'final_score_weight'
               AND p_quality.v1 = 'quality' 
             INNER JOIN app.def_sheet_platform_threats_params p_trend
                ON p_trend.param = 'final_score_weight'
               AND p_trend.v1 = 'trend'
            """
        )

        if not skip_results_copy:
            PlatformValuesCompetitorsCalc.__copy_data_to_results(
                conn=conn,
                platform_id=platform_id,
                dev_id_int=dev_id_int,
                audience_angle_row_id=audience_angle_row_id
            )

    @staticmethod
    def clear_results(
            conn: DbConnection,
            platform_id: int
    ):
        conn.query("""
        DELETE FROM platform_values.results_audience_angles__competitors
         WHERE platform_id = %s 
        """, [platform_id])
        conn.commit()

        conn.query("""
        DELETE FROM platform_values.results_competitors_cnt
         WHERE platform_id = %s
        """, [platform_id])
        conn.commit()

        conn.query("""
        DELETE FROM platform_values.results_affinities
         WHERE platform_id = %s
        """, [platform_id])
        conn.commit()

    @staticmethod
    def __copy_data_to_results(
            conn: DbConnection,
            platform_id: int,
            dev_id_int: int,
            audience_angle_row_id: int
    ):
        conn.query("""
        INSERT INTO platform_values.results_audience_angles__competitors
        (platform_id, multi_id, dev_id_int, audience_angle_row_id, app_id_int, competitor_score, 
        threat_score, similarity_w, similarity_c, trend_w, trend_c, quality_w, quality_c, tam_w, 
        tam_c, size_w, size_c, growth_w, growth_c, tam_raw)
        SELECT %s as platform_id, f.multi_id, %s as dev_id_int, f.audience_angle_row_id,
               f.app_id_int, f.competitor_score, f.threat_score, f.similarity_w, f.similarity_c,
               f.trend_w, f.trend_c, f.quality_w, f.quality_c, f.tam_w, f.tam_c, f.size_w, f.size_c,
               f.growth_w, f.growth_c, f.tam_raw
          FROM platform_values.tmp_ts_final f
        """, [platform_id, dev_id_int])

        conn.commit()

        conn.query("""
        INSERT INTO platform_values.results_competitors_cnt (platform_id, multi_id, competitors_cnt)
        SELECT %s as platform_id, multi_id, competitors_cnt
          FROM platform_values.tmp_competitors_pool_cnt 
        """, [platform_id])
        
        conn.commit()

        conn.query("""
        INSERT INTO platform_values.results_affinities
        (platform_id, affinity_tag_id_int, ts, audience_angle_row_id) 
        SELECT %s, a.multi_id, -1, a.audience_angle_row_id
        FROM platform_values.tmp_my_angles_base a
        """, [platform_id])
        conn.commit()

    @staticmethod
    def generate_client_data(
            conn: DbConnection,
            platform_id: int,
            is_admin=False
    ) -> {}:

        rows = conn.select_all("""
        SELECT r.multi_id, r.audience_angle_row_id, r.app_id_int, r.competitor_score, 
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
               IF (cc.competitors_cnt IS NULL, 0, cc.competitors_cnt) as competitors_pool_cnt,
               IF(a.removed_from_store IS NULL, 0, 1) as removed_from_store,
               platform.calc_ts_final(
                   r.threat_score,
                   af.discount
               ) as ts_final
          FROM platform_values.results_audience_angles__competitors r
         inner join platform_values.results_audience_angles__final af
            on af.row_id = r.audience_angle_row_id 
         INNER JOIN platform.platform_values_apps p
            ON p.app_id_int = r.app_id_int
         INNER JOIN scraped_data.apps_valid a 
            ON a.app_id_int = r.app_id_int
         INNER JOIN app.map_app_id_to_store_id m
            ON m.app_id_int = a.app_id_int
          LEFT JOIN app.def_sheet_platform_threats_params p_size
            ON p_size.param = 'size'
           AND CAST(p_size.v3 as UNSIGNED) = r.size_w
          LEFT JOIN app.def_sheet_platform_threats_params p_growth
            ON p_growth.param = 'growth'
           AND CAST(p_growth.v3 as UNSIGNED) = r.growth_w
          LEFT JOIN app.def_sheet_platform_threats_params p_quality
            ON p_quality.param = 'quality'
           AND CAST(p_quality.v3 as UNSIGNED) = r.quality_w
          LEFT JOIN app.def_sheet_platform_threats_params p_trend
            ON p_trend.param = 'trend'
           AND CAST(p_trend.v3 as UNSIGNED) = r.trend_w
          LEFT JOIN app.def_sheet_platform_threats_params p_tam
            ON p_tam.param = 'tam'
           AND CAST(p_tam.v3 as UNSIGNED) = r.tam_w
          LEFT JOIN platform_values.results_competitors_cnt cc
            ON cc.platform_id = r.platform_id
         WHERE r.platform_id = %s 
         ORDER BY r.threat_score DESC, r.competitor_score DESC, a.loyalty_installs DESC
        """, [platform_id])

        rows_input_platform_tags = []

        rows_audiences = conn.select_all(f"""
        SELECT af.*,
               platform.calc_discount_experience(af.exp_d) as calc_exp,
               platform.calc_discount_quality(af.qua_d) as calc_qua_angle,
               platform.calc_discount_quality(af.quality_full) as calc_qua_full,
               platform.calc_discount_revenues(af.rev_d) as calc_rev,
               d.title, aa.installs
          FROM platform_values.results_affinities f
         INNER JOIN platform_values.results_audience_angles__final af
            ON af.row_id = f.audience_angle_row_id
         INNER JOIN platform.audience_angle aa 
            ON aa.id = af.audience_angle_id
          LEFT JOIN scraped_data.devs d 
            ON d.dev_id_int = af.dev_id_int
         WHERE f.platform_id = %s
        """, [platform_id])

        rows_angle_tags = conn.select_all("""
        SELECT DISTINCT f.affinity_tag_id_int, aat.tag_id_int
          FROM platform_values.results_affinities f,
               platform_values.results_audience_angles__final af,
               platform.audience_angle_tags aat
         WHERE f.platform_id = %s
           AND f.audience_angle_row_id = af.row_id
           AND af.audience_angle_id = aat.id
        """, [platform_id])

        angle_tags_per_multi = {}
        for row in rows_angle_tags:
            if row["affinity_tag_id_int"] not in angle_tags_per_multi:
                angle_tags_per_multi[row["affinity_tag_id_int"]] = []
            angle_tags_per_multi[row["affinity_tag_id_int"]].append(row["tag_id_int"])

        if len(rows_audiences) > 0:

            audience_platforms = []
            for row in rows_audiences:
                if row["platform_id"] not in audience_platforms:
                    audience_platforms.append(row["platform_id"])

            rows_input_platform_tags = conn.select_all(f"""
            SELECT t.multi_id, t.tag_id_int, t.tag_rank
            FROM platform_values.results_audience_angle__input_tags t,
                 platform.def_tags d
            WHERE t.platform_id in ({conn.values_arr_to_db_in(audience_platforms, int_values=True)})
            AND t.tag_id_int = d.tag_id_int
            AND d.subcategory_int = %s
            """, [TagsConstants.SUBCATEGORY_PLATFORMS_ID])

        rows_tags = conn.select_all("""
        SELECT z1.app_id_int, t.tag_id_int
          FROM (
                SELECT DISTINCT r.app_id_int
                  FROM platform_values.results_audience_angles__competitors r
                 WHERE r.platform_id = %s
               ) z1,
               platform.platform_values_tags t
         WHERE t.app_id_int = z1.app_id_int
        """, [platform_id])

        rows_def_w = conn.select_all("""
        SELECT p.v1 as group_id, CAST(p.v2 as UNSIGNED) as w
          FROM app.def_sheet_platform_threats_params p
         WHERE p.param = 'final_score_weight'
        """)
        threat_score_def_w = {}
        for row in rows_def_w:
            threat_score_def_w[row["group_id"]] = row["w"]

        rows_ts_params = conn.select_all("""
        SELECT p.param, p.v3, p.name
          FROM app.def_sheet_platform_threats_params p
         WHERE p.param IN ('similar', 'size', 'growth', 'quality', 'trend', 'tam')
        """)

        tags_per_app_id = {}
        for row in rows_tags:
            app_id_int = row[UserObfuscator.APP_ID_INT]
            if app_id_int not in tags_per_app_id:
                tags_per_app_id[app_id_int] = []
            tags_per_app_id[app_id_int].append(row[UserObfuscator.TAG_ID_INT])

        res_final = {
            "app_details": [],
            "data": []
        }

        discount_groups = ["calc_qua_angle", "calc_exp", "calc_rev"]

        ts_items_per_gr = {}

        rows_per_multi_id = {}
        for row in rows_audiences:
            multi_id = row["multi_id"]
            rows_per_multi_id[multi_id] = {
                "aud": row,
                "c": []
            }
        for row in rows:
            multi_id = row["multi_id"]
            if multi_id in rows_per_multi_id:
                rows_per_multi_id[multi_id]["c"].append(row)

        platform_tags_per_multi_id = {}
        for row in rows_input_platform_tags:
            multi_id = row["multi_id"]
            if multi_id not in platform_tags_per_multi_id:
                platform_tags_per_multi_id[multi_id] = []
            platform_tags_per_multi_id[multi_id].append(row["tag_id_int"])

        input_tags_per_multi_id = {}
        for row in rows_input_platform_tags:
            multi_id = row["multi_id"]
            if multi_id not in input_tags_per_multi_id:
                input_tags_per_multi_id[multi_id] = []
            input_tags_per_multi_id[multi_id].append({
                "tag_id_int": row["tag_id_int"],
                "tag_rank": row["tag_rank"]
            })

        helper = PlatformValuesHelper(conn=conn)

        app_details = {}

        for multi_id in rows_per_multi_id:

            res = {
                "multi_id": multi_id,
                "ts_items": []
            }

            res_final["data"].append(res)

            ts_arr = []
            if multi_id in input_tags_per_multi_id:
                res["input_tags"] = input_tags_per_multi_id[multi_id]

            row_audience = rows_per_multi_id[multi_id]["aud"]

            app_platforms = []
            if multi_id in platform_tags_per_multi_id:
                app_platforms = platform_tags_per_multi_id[multi_id]
            res["audience_detail"] = helper.audience_to_client_data_2(
                data=row_audience,
                platform_id=platform_id,
                app_platforms=app_platforms,
                is_admin=False,
                angle_tags=angle_tags_per_multi[multi_id] if multi_id in angle_tags_per_multi else None
            )

            quality_portfolio = row_audience["quality_portfolio"]

            res["company_title"] = row_audience["title"]

            def discountColor(k: str):
                v = row_audience[k]
                if k == "qua_angle":
                    return "r" if v < 6 else "g" if v > 8 else ""
                if k == "exp":
                    return "r" if v < 2 else "g" if v > 6 else ""
                if k == "rev":
                    return "r" if v < 3 else "g" if v > 10 else ""
                return ""
            res["discounts"] = [{
                "gr": k,
                "ts_perc": round(row_audience[k]),
                "ts_raw": row_audience[k],
                "c": discountColor(k)
            } for k in discount_groups]

            res["qua_full"] = row_audience["calc_qua_full"]

            res["competitor_apps_details"] = []

            rows = rows_per_multi_id[multi_id]["c"]
            for row in rows:
                res["competitors_pool_cnt"] = row["competitors_pool_cnt"]

                app_id_int = row[UserObfuscator.APP_ID_INT]

                if app_id_int not in app_details:
                    app_details[app_id_int] = {
                        UserObfuscator.APP_ID_INT: app_id_int,
                        "title": row["title"],
                        "app_id_in_store": row["app_id_in_store"],
                        "app_store_url": AppModel.get_store_url(
                            url=row["url"],
                            app_id_in_store=row["app_id_in_store_raw"],
                            app_type=AppModel.APP_TYPE__STORE,
                            store=row["store"]
                        ),
                        "icon": row["icon"],
                        "platform": row["platform"],
                        "store": row["store"],
                        "locked": False,
                        "removed_from_store": row["removed_from_store"]
                    }

                    if app_id_int in tags_per_app_id:
                        app_details[app_id_int][UserObfuscator.TAG_IDS_INT] = tags_per_app_id[app_id_int]

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
                        "released": datetime.now().year - row["released_years"],
                        "raw_val": row['release_year'],
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

                ts_data["ts"] = row["ts_final"]
                ts_arr.append(row["ts_final"])

            if quality_portfolio != 0:
                res["quality_portfolio"] = quality_portfolio

            res["ts_items"].sort(key=lambda x: x["ts"], reverse=True)
            res["ts_items"] = res["ts_items"][:50]

            res["competitors_count"] = len(res["ts_items"])

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
            res["ts"] = PlatformValuesHelper.calc_ts(ts_arr=ts_arr)

        for app_id_int in app_details:
            res_final["app_details"].append(app_details[app_id_int])

        return res_final
