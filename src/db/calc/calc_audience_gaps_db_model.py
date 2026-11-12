from server_core.db.db_connection import DbConnection
from src.db.base.base_db_model import BaseDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.calc.helpers.audience_stats import AudienceStats


class CalcAudienceGapsDbModel(BaseDbModel):

    def __init__(self, conn: DbConnection):
        super(CalcAudienceGapsDbModel, self).__init__(conn=conn)
        self.calc_utils = CalcUtils(conn=conn)
        
    def calc(
            self,
            platform_id: int,
            survey_id: int,
            dev_id_int: int,
            selected_tags_details: list[dict],
            update_progress_data=None
    ):
        selected_tags_ids = []
        for tag_detail in selected_tags_details:
            selected_tags_ids.append(tag_detail["tag_id_int"])
        selected_tags_ids_db = self.conn.values_arr_to_db_in(selected_tags_ids, int_values=True)

        bin_bytes_cnt = self.calc_utils.get_bin_bytes_cnt()

        row_yearly_revenues = self.conn.select_one_or_none(f"""
                SELECT d.yearly_revenues
                  FROM gb_platform.platform_values_devs d
                 WHERE d.dev_id_int = {dev_id_int}
                """)

        yearly_revenues = 0
        if row_yearly_revenues is not None:
            yearly_revenues = row_yearly_revenues["yearly_revenues"]

        # developer app with most installs covering node
        self.calc_utils.recreate_table(
            table_name="tmp_gaps__ts_tags_per_tag",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                tag_id_int int unsigned not null,
                b BINARY({bin_bytes_cnt}) not null,
                tier int unsigned default 0 not null,
                tam bigint unsigned not null,
                constraint x__table_name__x_pk
                primary key (tag_id_int)
            )
            SELECT z2.tag_id_int, tb.b, a.tier, tam.tam
              FROM (
                    SELECT z1.tag_id_int,
                           z1.app_id_int
                      FROM (
                            SELECT p.tag_id_int,
                                   FIRST_VALUE(a.app_id_int) OVER (PARTITION BY p.tag_id_int ORDER BY a.installs DESC) AS app_id_int
                              FROM gb_platform.platform_values_devs_apps da,
                                   gb_platform.platform_values_apps a,
                                   gb_platform.platform_values_tags t,
                                   gb_platform.def_tags p
                             WHERE da.dev_id_int = {dev_id_int}
                               AND da.app_id_int = a.app_id_int
                               AND a.app_id_int = t.app_id_int
                               AND p.tag_id_int = t.tag_id_int
                               AND p.is_survey = 1
                           ) z1
                     GROUP BY 
                           z1.tag_id_int, 
                           z1.app_id_int
                   ) z2,
                   gb_platform.platform_values_apps a,
                   gb_platform.platform_values_tags_bin tb,
                   gb_platform.audience_angle_tam_per_app tam
             WHERE z2.app_id_int = a.app_id_int
               AND tb.app_id_int = a.app_id_int
               AND tam.app_id_int = a.app_id_int
               AND tam.survey_id = {survey_id}
               AND tam.angle_cnt = 2
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_gaps__ts_tags_live_apps",
            query="""
            CREATE TABLE gb_calc.x__table_name__x (
                tag_id_int int unsigned not null
            )
            select tag_id_int from gb_calc.tmp_gaps__ts_tags_per_tag
            """
        )

        # fake concepts where nodes are not covered
        query = f"""
        INSERT INTO gb_calc.tmp_gaps__ts_tags_per_tag (tag_id_int, b, tam)
        SELECT f.tag_id_int, 
               f.b,
               tam.tam
          FROM gb_platform.platform_values_tags_fake_concepts_bin f,
               gb_platform.platform_values_tags_fake_concepts_tam tam
         WHERE f.tag_id_int NOT IN (
             SELECT t.tag_id_int FROM gb_calc.tmp_gaps__ts_tags_live_apps t
         )
         AND tam.survey_id = {survey_id}
         AND tam.tag_id_int = f.tag_id_int
         AND f.survey_id = tam.survey_id
        """
        self.conn.query(query)
        self.conn.commit()
        self.conn.analyze("platform_values.tmp_gaps__ts_tags_per_tag")

        self.calc_utils.recreate_table(
            table_name="tmp_gaps__ts_tags_subc_per_tag",
            query="""
            CREATE TABLE gb_calc.x__table_name__x (
                tag_id_int int unsigned not null,
                subcategory_int int unsigned not null,
                cnt int unsigned not null,
                constraint x__table_name__x_pk
                    primary key (tag_id_int, subcategory_int)
            )
            SELECT t.tag_id_int, p.subcategory_int, count(1) as cnt
              FROM gb_calc.tmp_gaps__ts_tags_per_tag t,
                   gb_platform.def_tags_bin d,
                   gb_app.def_sheet_platform_product p
             WHERE bit_count(t.b & d.b) = 1
               and d.tag_id_int = p.tag_id_int
             group by t.tag_id_int, p.subcategory_int
            """
        )

        if update_progress_data is not None:
            update_progress_data({
                "state": "aud"
            })

        self.conn.query(f"""
               DELETE FROM gb_calc.results_audience_angle__rejected r
               WHERE r.platform_id = {platform_id}
               """)
        self.conn.commit()

        if len(selected_tags_ids) > 0:

            bin_bytes_cnt = self.calc_utils.get_bin_bytes_cnt()
            self.calc_utils.recreate_table(
                table_name="tmp_gaps__selected_tags_bin",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    b BINARY({bin_bytes_cnt}) not null
                )
                SELECT BIT_OR(d.b) as b
                  FROM gb_platform.def_tags_bin d
                 WHERE d.tag_id_int in ({selected_tags_ids_db})
                """
            )

            # rejected per node
            self.calc_utils.recreate_table(
                table_name="tmp_rejected_per_node",
                query=f"""
                INSERT INTO gb_calc.results_audience_angle__rejected 
                (platform_id, audience_angle_id, rejected_cnt) 
                SELECT {platform_id} as platform_id, 
                       d.tag_id_int as audience_angle_id, 
                       count(1) as rejected_cnt
                  FROM gb_platform.def_tags_bin d,
                       gb_platform.platform_values_survey_tags_bin stb,
                       gb_calc.tmp_gaps__selected_tags_bin my_tags
                 WHERE bit_count(stb.b_loved & d.b) = 1
                   AND bit_count(stb.b_hated & my_tags.b) > 0
                   AND stb.survey_id = {survey_id}
                 GROUP BY d.tag_id_int
                """
            )

        # discount per node

        self.calc_utils.recreate_table(
            table_name="tmp_gaps__discount_per_node",
            query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    tag_id_int int unsigned not null,
                    quality int unsigned default 40 not null,
                    released_years int unsigned default 0 not null,
                    constraint x__table_name__x_pk
                    primary key (tag_id_int)
                )
                select tag_id_int FROM gb_platform.def_tags
                """
        )

        self.conn.query(f"""
            UPDATE gb_calc.tmp_gaps__discount_per_node aa
            INNER JOIN (
            SELECT z3.tag_id_int, ROUND(AVG(z3.quality)) AS quality
      FROM (
    SELECT z2.tag_id_int, 
           z2.quality,
           ROW_NUMBER() OVER (PARTITION BY z2.tag_id_int ORDER BY z2.quality) AS row_num,
           COUNT(1) OVER (PARTITION BY z2.tag_id_int) AS total_num
      FROM (
    SELECT d.tag_id_int,
           apps.rating * IF(BIT_COUNT(tb.b & d.b) = 1, 1, 0.75) AS quality
      FROM (
    SELECT a.app_id_int, a.rating
      FROM gb_platform.platform_values_devs_apps da,
           gb_platform.platform_values_apps a
     WHERE da.dev_id_int = {dev_id_int}
       AND da.app_id_int = a.app_id_int
       AND a.tier >= 3
     ) apps,
     gb_platform.platform_values_tags_bin tb,
     gb_platform.def_tags_bin d
     WHERE tb.app_id_int = apps.app_id_int
     ) z2
     ) z3
     WHERE z3.row_num IN (FLOOR((z3.total_num + 1) / 2), FLOOR((z3.total_num + 2) / 2) ) 
            GROUP BY z3.tag_id_int ) z4
            ON z4.tag_id_int = aa.tag_id_int
            SET aa.quality = z4.quality
            """)
        self.conn.commit()

        self.conn.query(f"""
                    UPDATE gb_calc.tmp_gaps__discount_per_node aa
                    INNER JOIN (
                    SELECT z3.tag_id_int, MAX(z3.released_years) AS released_years
      FROM (
    SELECT z2.tag_id_int, 
           z2.released_years
      FROM (
    SELECT d.tag_id_int,
           apps.released_years
      FROM (
    SELECT a.app_id_int, a.released_years
      FROM gb_platform.platform_values_devs_apps da,
           gb_platform.platform_values_apps a
     WHERE da.dev_id_int = {dev_id_int}
       AND da.app_id_int = a.app_id_int
       AND a.tier >= 3
     ) apps,
     gb_platform.platform_values_tags_bin tb,
     gb_platform.def_tags_bin d
     WHERE tb.app_id_int = apps.app_id_int
       AND BIT_COUNT(tb.b & d.b) = 1
     ) z2
     ) z3
            GROUP BY z3.tag_id_int
      ) z4
                    ON z4.tag_id_int = aa.tag_id_int
                    SET aa.released_years = z4.released_years
                    """)
        self.conn.commit()

        # competitors per node

        self.conn.query("""
        delete from gb_calc.results_gaps__nodes_ts
        where platform_id = %s
        """, [platform_id])

        page = 0
        limit = 20
        total = int(self.conn.select_one("""
        SELECT count(1) as cnt
              FROM gb_platform.audience_angle_potential aap,
                   gb_platform.audience_angle aa
             where aa.angle_cnt = 1
               and aap.id = aa.id
        """)["cnt"] / limit)

        if update_progress_data is not None:
            update_progress_data({
                "state": "ts",
                "p": 0,
                "t": total,
                "l": limit
            })

        while True:

            print(f"Page: {page}")

            rows_tags = self.conn.select_all(f"""
            SELECT p.id
              FROM gb_platform.audience_angle_potential p,
                   gb_platform.audience_angle aa
             where aa.id = p.id
               and aa.angle_cnt = 1
             ORDER BY p.id
             LIMIT {page * limit}, {limit}
            """)

            if len(rows_tags) == 0:
                break

            page += 1

            tag_ids = []
            for row in rows_tags:
                tag_ids.append(row["id"])

            tag_ids_db = self.conn.values_arr_to_db_in(tag_ids, int_values=True)

            self.calc_utils.recreate_table(
                table_name="tmp_gaps__competitors_pool",
                query=f"""
                    CREATE TABLE gb_calc.x__table_name__x (
                    id int unsigned not null,
                    app_id_int int unsigned not null,
                    constraint x__table_name__x_pk
                    primary key (id, app_id_int)
                    )
                    SELECT z3.id, z3.app_id_int
                      FROM (
                    SELECT z2.id,
                           z2.app_id_int,
                           ROW_NUMBER() OVER (PARTITION BY z2.id ORDER BY z2.score DESC) AS row_num
                      FROM (
                   SELECT z1.id, z1.app_id_int, SUM(CAST(z1.score1 as SIGNED) - CAST((z1.score2 + z1.score3) as SIGNED)) AS score
                      FROM (
                    SELECT d.tag_id_int as id, 
                           t.app_id_int, 
                            3 * BIT_COUNT(t.b & my_tags.b & survey_w.w_b & tags_subc.b) * tags_subc.competitors_pool_w * survey_w.w AS score1,
                            BIT_COUNT(t.b & survey_w.w_b & tags_subc.b) * tags_subc.competitors_pool_w * survey_w.w AS score2,
                            BIT_COUNT(my_tags.b & survey_w.w_b & tags_subc.b) * tags_subc.competitors_pool_w * survey_w.w AS score3
                     FROM gb_platform.platform_values_tags_bin t,
                          gb_platform.def_tags_bin d,
                          gb_platform.def_tags_subc_bin tags_subc,
                          gb_platform.audience_angle_tags_w survey_w,
                          gb_calc.tmp_gaps__ts_tags_per_tag my_tags
                    WHERE BIT_COUNT(t.b & d.b) = 1
                      AND survey_w.survey_id = {survey_id}
                      AND survey_w.id = d.tag_id_int
                      AND d.tag_id_int IN ({tag_ids_db})
                      AND my_tags.tag_id_int = d.tag_id_int
                    ) z1
                    GROUP BY z1.id, z1.app_id_int
                    ) z2
                    ) z3
                    WHERE z3.row_num <= 20
                """
            )

            # threat score - similarity

            self.calc_utils.recreate_table(
                table_name="tmp_gaps__ts_similarity",
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
                                    FROM gb_calc.tmp_gaps__competitors_pool pool,
                                         gb_platform.platform_values_tags_bin t,
                                         gb_platform.def_tags_subc_bin def_subc,
                                         gb_calc.tmp_gaps__ts_tags_per_tag my_tags,
                                         gb_calc.tmp_gaps__ts_tags_subc_per_tag my_tags_subc,
                                         (
                                          SELECT p.subcategory_int,
                                                 p.threatscore_similarity_w AS w
                                            FROM gb_app.def_sheet_platform_product p
                                           WHERE p.threatscore_similarity_w != 0
                                           GROUP BY 
                                                 p.subcategory_int, 
                                                 p.threatscore_similarity_w
                                         ) subc_w
                                   WHERE my_tags.tag_id_int = pool.id
                                     AND pool.app_id_int = t.app_id_int
                                     AND my_tags.tag_id_int = my_tags_subc.tag_id_int
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
                table_name="tmp_gaps__ts_tqs",
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
                                constraint x__table_name__x_pk
                                   primary key (id, app_id_int)
                            )
                            SELECT pool.id, pool.app_id_int,
                                   CAST(trend.v3 AS UNSIGNED ) AS trend_w,
                                   IF(trend.color = 'r', -1, IF(trend.color = 'g', 1, 0)) AS trend_color,
                                   CAST(quality.v3 AS UNSIGNED ) AS quality_w,
                                   IF(quality.color = 'r', -1, IF(quality.color = 'g', 1, 0)) AS quality_color,
                                   CAST(size.v3 AS UNSIGNED ) AS size_w,
                                   IF(size.color = 'r', -1, IF(size.color = 'g', 1, 0)) AS size_color
                              FROM gb_calc.tmp_gaps__competitors_pool pool,
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
                               AND CAST(ac.tier as SIGNED) >= CAST(size.v1 as SIGNED)
                               AND CAST(ac.tier as SIGNED) <= CAST(size.v2 as SIGNED)
                            """
            )

            # threat score - tam
            self.calc_utils.recreate_table(
                table_name="tmp_gaps__ts_tam",
                query=f"""
                            CREATE TABLE gb_calc.x__table_name__x (
                                id bigint unsigned not null,
                                app_id_int MEDIUMINT NOT NULL,
                                tam_w SMALLINT UNSIGNED NOT NULL,
                                tam_color TINYINT NOT NULL,
                                constraint x__table_name__x_pk
                                    primary key (id, app_id_int)
                            )
                            SELECT z2.id, 
                                   z2.app_id_int,
                                   CAST(tp.v3 AS UNSIGNED) AS tam_w,
                                   IF (tp.color = 'r', -1, IF (tp.color = 'g', 1, 0)) AS tam_color
                              FROM (
                                    SELECT z1.id, 
                                           z1.app_id_int, 
                                           round(IF(z1.my_tam = 0, -1, z1.c_tam / z1.my_tam * 100)) AS tam_ratio
                                      FROM (
                                            SELECT pool.id, 
                                                   pool.app_id_int, 
                                                   tam_per_app.tam as c_tam,
                                                   my_tags.tam as my_tam
                                              FROM gb_calc.tmp_gaps__competitors_pool pool,
                                                   gb_platform.audience_angle_tam_per_app tam_per_app,
                                                   gb_calc.tmp_gaps__ts_tags_per_tag my_tags
                                             WHERE my_tags.tag_id_int = pool.id
                                               AND pool.app_id_int = tam_per_app.app_id_int
                                               AND tam_per_app.survey_id ={survey_id}
                                               AND tam_per_app.angle_cnt = 2
                                           ) z1
                                   ) z2,
                                   gb_app.def_sheet_platform_threats_params tp
                             WHERE tp.param = 'tam'     
                             AND (
                                        (
                                         z2.tam_ratio < 0 AND tp.v2 IS NULL
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
                table_name="tmp_gaps__ts_final",
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
                                size_w smallint unsigned not null,
                                size_c tinyint not null,
                                tam_w smallint unsigned not null,
                                tam_c tinyint not null,
                                constraint x__table_name__x_pk
                                    primary key (id, app_id_int)
                            )
                            SELECT pool.id, 
                                   pool.app_id_int,
                                   round(0) AS competitor_score,
                                   ROUND((
                                           CAST(p_similar.v2 AS UNSIGNED) * IF(s.w IS NULL, 0, s.w) +
                                           CAST(p_size.v2 AS UNSIGNED) * IF(tqs.size_w IS NULL, 0, tqs.size_w) +
                                           CAST(p_tam.v2 AS UNSIGNED) * IF(tam.tam_w IS NULL, 0, tam.tam_w) +
                                           CAST(p_quality.v2 AS UNSIGNED) * IF(tqs.quality_w IS NULL, 0, tqs.quality_w) +
                                           CAST(p_trend.v2 AS UNSIGNED) * IF(tqs.trend_w IS NULL, 0, tqs.trend_w)) / 100, 0)
                                           AS threat_score,
                                   gb_platform.calc_discount(
                                       {yearly_revenues},
                                       IF(ds.quality is null, 0, ds.quality),
                                       IF(ds.released_years is null, 0, ds.released_years)
                                   ) as threat_score_discount,
                                   IF(s.w IS NULL, 0, s.w) AS similarity_w,
                                   IF(s.color IS NULL, 0, s.color) AS similarity_c,
                                   IF(tqs.trend_w IS NULL, 0, tqs.trend_w) AS trend_w,
                                   IF(tqs.trend_color IS NULL, 0, tqs.trend_color)  AS trend_c,
                                   IF(tqs.quality_w IS NULL, 0, tqs.quality_w)  AS quality_w,
                                   IF(tqs.quality_color IS NULL, 0, tqs.quality_color)  AS quality_c,
                                   IF(tqs.size_w IS NULL, 0, tqs.size_w)  AS size_w,
                                   IF(tqs.size_color IS NULL, 0, tqs.size_color) AS size_c,
                                   IF(tam.tam_w IS NULL, 0, tam.tam_w) AS tam_w,
                                   IF(tam.tam_color IS NULL, 0, tam.tam_color) AS tam_c
                              FROM gb_calc.tmp_gaps__competitors_pool pool
                              LEFT JOIN gb_calc.tmp_gaps__discount_per_node ds
                              on ds.tag_id_int = pool.id
                              LEFT JOIN gb_calc.tmp_gaps__ts_similarity s 
                                ON s.id = pool.id
                               AND s.app_id_int = pool.app_id_int
                              LEFT JOIN gb_calc.tmp_gaps__ts_tqs tqs 
                                ON tqs.id = pool.id
                               AND tqs.app_id_int = pool.app_id_int
                              LEFT JOIN gb_calc.tmp_gaps__ts_tam tam 
                                ON tam.id = pool.id
                               AND tam.app_id_int = pool.app_id_int
                             INNER JOIN gb_app.def_sheet_platform_threats_params p_similar
                                ON p_similar.param = 'final_score_weight'
                               AND p_similar.v1 = 'similar' 
                             INNER JOIN gb_app.def_sheet_platform_threats_params p_size
                                ON p_size.param = 'final_score_weight'
                               AND p_size.v1 = 'size' 
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

            # results
            rows_ts = self.conn.select_all("""
            SELECT tf.id,
                   greatest(0, cast(tf.threat_score as signed) - cast(tf.threat_score_discount as signed)) AS ts
              FROM gb_calc.tmp_gaps__ts_final tf
            """)

            ts_per_angle = {}
            for row in rows_ts:
                angle_id = row["id"]
                if angle_id not in ts_per_angle:
                    ts_per_angle[angle_id] = []
                ts_per_angle[angle_id].append(int(row["ts"]))

            bulk_data = []
            for angle_id in ts_per_angle:
                ts_final = self.calc_utils.calc_ts(ts_arr=ts_per_angle[angle_id])
                bulk_data.append((platform_id, angle_id, ts_final))

            self.conn.bulk("""
            insert into gb_calc.results_gaps__nodes_ts (platform_id, tag_id_int, ts)
            values (%s, %s, %s)
            """, bulk_data)
            self.conn.commit()

            if update_progress_data is not None:
                update_progress_data({
                    "state": "ts",
                    "p": page,
                    "t": total,
                    "l": limit
                })

    def generate_client_data(
            self,
            platform_id: int,
    ) -> dict:

        rows = self.conn.select_all(f"""
        SELECT aa.id,
               gb_platform.calc_total_audience(
                   aap.loved_cnt,
                   aap.total_cnt,
                   IF(r.rejected_cnt IS NULL, 0, r.rejected_cnt),
                   CAST(c.v1 as UNSIGNED),
                   aa.loyalty_installs, 0, 0
               ) as total_audience,
               aap.loved_cnt,
               aap.total_cnt,
               IF(r.rejected_cnt IS NULL, 0, r.rejected_cnt) as rejected_cnt,
               CAST(c.v1 as UNSIGNED) as potential_downloads,
               aa.loyalty_installs,
               ts.ts
          FROM gb_platform.audience_angle aa
         INNER JOIN gb_app.def_sheet_platform_values_constants c
            ON c.param = '{CalcUtils.POTENTIAL_DOWNLOADS_MOBILE_WW}'
         INNER JOIN gb_platform.audience_angle_potential aap
            ON aap.id = aa.id
          LEFT JOIN gb_calc.results_audience_angle__rejected r
            ON r.platform_id = %s
           AND r.audience_angle_id = aa.id
         LEFT JOIN gb_calc.results_gaps__nodes_ts ts
         ON ts.platform_id = %s
         AND ts.tag_id_int = aa.id
         WHERE aa.angle_cnt = 1
        """, [platform_id, platform_id])

        items = []
        for row in rows:

            audience_stats = AudienceStats(
                loved_survey_cnt=row["loved_cnt"],
                total_survey_cnt=row["total_cnt"],
                rejected_survey_cnt=row["rejected_cnt"],
                potential_downloads=row["potential_downloads"],
                loyalty_installs=row["loyalty_installs"]
            )

            d = {
                "tag_id_int": row["id"],
                "audience_stats": audience_stats.generate_client_data(),
                "ts": 0
            }

            if row["ts"] is not None:
                d["ts"] = row["ts"]
            else:
                d["no_data"] = True

            items.append(d)

        return {
            "platform_id": platform_id,
            "items": items
        }
