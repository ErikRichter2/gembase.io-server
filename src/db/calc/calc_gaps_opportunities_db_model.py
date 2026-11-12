import json
from typing import Optional

from server_core.db.db_connection import DbConnection
from src.db.base.base_db_model import BaseDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.games_db_model import GamesDbModel
from src.models.user_obfuscator import UserObfuscator


class CalcGapsOpportunitiesDbModel(BaseDbModel):

    def __init__(self, conn: DbConnection):
        super(CalcGapsOpportunitiesDbModel, self).__init__(conn=conn)
        self.calc_utils = CalcUtils(conn=conn)

    def calc(
            self,
            platform_id: int,
            survey_id: int,
            dev_id_int: int,
            selected_tags_details: list[dict],
            update_progress_data=None
    ):
        progress_data = {
            "state": "init",
            "p": 0,
            "t": 10
        }

        def update_progress():
            if update_progress_data is not None:
                update_progress_data(progress_data)

        update_progress()

        self.conn.query(f"""
        DELETE FROM gb_calc.results_gaps__opportunities r
        WHERE r.platform_id = {platform_id}
        """)
        self.conn.query(f"""
        DELETE FROM gb_calc.results_gaps__opportunities_tags r
        WHERE r.platform_id = {platform_id}
        """)
        self.conn.commit()

        bin_bytes_cnt = self.calc_utils.get_bin_bytes_cnt()

        row_yearly_revenues = self.conn.select_one_or_none(f"""
        SELECT d.yearly_revenues
          FROM gb_platform.platform_values_devs d
         WHERE d.dev_id_int = {dev_id_int}
        """)

        yearly_revenues = 0
        if row_yearly_revenues is not None:
            yearly_revenues = row_yearly_revenues["yearly_revenues"]

        selected_tags_ids = []
        for tag_detail in selected_tags_details:
            selected_tags_ids.append(tag_detail["tag_id_int"])

        selected_tags_ids.sort()
        selected_tags_ids_db = self.conn.values_arr_to_db_in(selected_tags_ids, int_values=True)

        rows_affinity_tags = self.conn.select_all(f"""
        SELECT d.tag_id_int
          FROM gb_platform.audience_angle aa,
               gb_platform.audience_angle_potential aap,
               gb_platform.def_tags_bin d
         WHERE aa.angle_cnt = 1
           AND d.tag_id_int NOT IN ({selected_tags_ids_db})
           AND aa.id = aap.id
           AND BIT_COUNT(aa.b & d.b) = 1
          ORDER BY d.tag_id_int
        """)

        bulk_data = []
        for row in rows_affinity_tags:
            affinity_tag_id_int = row["tag_id_int"]
            for i in range(len(selected_tags_ids)):
                tag_id_int_1 = selected_tags_ids[i]
                audience_angle_id = CalcUtils.create_audience_angle_2_comb_id(affinity_tag_id_int, tag_id_int_1)
                bulk_data.append((audience_angle_id, affinity_tag_id_int))
                for j in range(i + 1, len(selected_tags_ids)):
                    tag_id_int_2 = selected_tags_ids[j]
                    audience_angle_id = CalcUtils.create_audience_angle_2_comb_id(tag_id_int_1, tag_id_int_2)
                    bulk_data.append((audience_angle_id, affinity_tag_id_int))

        self.calc_utils.recreate_table(
            table_name="tmp_gaps__audience_angles",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                affinity_tag_id_int int unsigned not null,
                rejected_cnt int unsigned default 0 not null,
                total_audience bigint unsigned default 0 not null,
                tam bigint unsigned default 0 not null,
                arpu decimal(8,4) default 0 not null,
                quality int unsigned default 40 not null,
                released_years int unsigned default 0 not null,
                constraint x__table_name__x_pk
                primary key (id, affinity_tag_id_int)
            )
            """
        )
        self.conn.bulk("""
        INSERT INTO gb_calc.tmp_gaps__audience_angles 
        (id, affinity_tag_id_int)
        VALUES (%s, %s)
        """, bulk_data)
        self.conn.commit()

        self.calc_utils.recreate_table(
            table_name="tmp_gaps__audience_angles_list",
            query="""
            CREATE TABLE gb_calc.x__table_name__x (
            id bigint unsigned not null,
                affinity_tag_id_int int unsigned not null,
                constraint x__table_name__x_pk
                primary key (id, affinity_tag_id_int)
            )
            SELECT id, affinity_tag_id_int
            FROM gb_calc.tmp_gaps__audience_angles
            """
        )

        self.conn.query(f"""
        UPDATE gb_calc.tmp_gaps__audience_angles aa
        INNER JOIN (
        SELECT z3.id, ROUND(AVG(z3.quality)) AS quality
  FROM (
SELECT z2.id, 
       z2.quality,
       ROW_NUMBER() OVER (PARTITION BY z2.id ORDER BY z2.quality) AS row_num,
       COUNT(1) OVER (PARTITION BY z2.id) AS total_num
  FROM (
SELECT aa.id,
       apps.rating * IF(BIT_COUNT(tb.b & aa.b) = aa.angle_cnt, 1, 0.75) AS quality
  FROM (
SELECT a.app_id_int, a.rating
  FROM gb_platform.platform_values_devs_apps da,
       gb_platform.platform_values_apps a
 WHERE da.dev_id_int = {dev_id_int}
   AND da.app_id_int = a.app_id_int
 ORDER BY a.installs DESC
 LIMIT 3
 ) apps,
 gb_platform.platform_values_tags_bin tb,
 gb_platform.audience_angle aa
 WHERE tb.app_id_int = apps.app_id_int
   AND aa.angle_cnt = 2
 ) z2
 ) z3
 WHERE z3.row_num IN (FLOOR((z3.total_num + 1) / 2), FLOOR((z3.total_num + 2) / 2) ) 
        GROUP BY z3.id ) z4
        ON z4.id = aa.id
        SET aa.quality = z4.quality
        """)
        self.conn.commit()

        self.conn.query(f"""
                UPDATE gb_calc.tmp_gaps__audience_angles aa
                INNER JOIN (
                SELECT z3.id, MAX(z3.released_years) AS released_years
  FROM (
SELECT z2.id, 
       z2.released_years
  FROM (
SELECT aa.id,
       apps.released_years
  FROM (
SELECT a.app_id_int, a.released_years
  FROM gb_platform.platform_values_devs_apps da,
       gb_platform.platform_values_apps a
 WHERE da.dev_id_int = {dev_id_int}
   AND da.app_id_int = a.app_id_int
 ) apps,
 gb_platform.platform_values_tags_bin tb,
 gb_platform.audience_angle aa
 WHERE tb.app_id_int = apps.app_id_int
   AND aa.angle_cnt = 2
   AND BIT_COUNT(tb.b & aa.b) = aa.angle_cnt
 ) z2
 ) z3
        GROUP BY z3.id
  ) z4
                ON z4.id = aa.id
                SET aa.released_years = z4.released_years
                """)
        self.conn.commit()

        self.calc_utils.recreate_table(
            table_name="tmp_gaps__selected_tags_bin",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                b BINARY({bin_bytes_cnt})
            )
            SELECT BIT_OR(d.b) as b
              FROM gb_platform.def_tags_bin d
             WHERE d.tag_id_int in ({selected_tags_ids_db})
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_gaps__selected_tags_per_angle",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                affinity_tag_id_int int unsigned not null,
                b BINARY({bin_bytes_cnt}) not null,
                constraint x__table_name__x_pk
                primary key (id, affinity_tag_id_int)
            )
            SELECT aa.id,
                   aa.affinity_tag_id_int,
                   d.b | my_tags.b as b
              FROM gb_calc.tmp_gaps__audience_angles aa,
                   gb_calc.tmp_gaps__selected_tags_bin my_tags,
                   gb_platform.def_tags_bin d
             WHERE d.tag_id_int = aa.affinity_tag_id_int
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_gaps__selected_tags_subc_count_per_angle",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                affinity_tag_id_int int unsigned not null,
                subcategory_int int unsigned not null,
                cnt int unsigned not null,
                constraint x__table_name__x_pk
                primary key (id, affinity_tag_id_int, subcategory_int)
            )
            SELECT sta.id,
                   sta.affinity_tag_id_int,
                   p.subcategory_int, 
                   COUNT(1) AS cnt
              FROM gb_calc.tmp_gaps__selected_tags_per_angle sta,
                   gb_app.def_sheet_platform_product p,
                   gb_platform.def_tags_bin d
             WHERE bit_count(sta.b & d.b) = 1
               AND d.tag_id_int = p.tag_id_int
             GROUP BY sta.id, sta.affinity_tag_id_int, p.subcategory_int
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_gaps__audience_angles_rejected",
            query="""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                affinity_tag_id_int int unsigned not null,
                rejected_cnt int unsigned not null,
                constraint x__table_name__x_pk
                primary key (id, affinity_tag_id_int)
            )
            """
        )

        opportunities_pages_cnt = round(self.conn.select_one("""
        SELECT count(1) as cnt FROM gb_calc.tmp_gaps__audience_angles
        """)["cnt"] / 500) + 1

        progress_data["state"] = "gaps"
        progress_data["t"] = opportunities_pages_cnt * 2

        page = 0
        limit = 500
        while True:

            print(f"Page: {page}")

            progress_data["p"] += 1
            update_progress()

            self.calc_utils.recreate_table(
                table_name="tmp_gaps__audience_angles_part",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    id bigint unsigned not null,
                    affinity_tag_id_int int unsigned not null,
                    constraint x__table_name__x_pk
                    primary key (id, affinity_tag_id_int)
                )
                SELECT aat.id, aat.affinity_tag_id_int
                  FROM gb_calc.tmp_gaps__audience_angles aat
                 ORDER BY aat.affinity_tag_id_int, aat.id
                 LIMIT {page * limit}, {limit}
                """
            )

            cnt = self.conn.select_one("""
            SELECT count(1) as cnt FROM gb_calc.tmp_gaps__audience_angles_part
            """)["cnt"]

            if cnt == 0:
                break

            page += 1

            self.conn.query(f"""
            INSERT INTO gb_calc.tmp_gaps__audience_angles_rejected
            (id, affinity_tag_id_int, rejected_cnt)
                SELECT part.id,
                       part.affinity_tag_id_int,
                       COUNT(1) AS rejected_cnt
                  FROM gb_calc.tmp_gaps__audience_angles_part part,
                       gb_platform.audience_angle aa,
                       gb_calc.tmp_gaps__selected_tags_per_angle my_tags,
                       gb_platform.platform_values_survey_tags_bin stb
                 WHERE stb.survey_id = {survey_id}
                   AND part.affinity_tag_id_int = my_tags.affinity_tag_id_int
                   AND part.id = my_tags.id
                   AND part.id = aa.id
                   AND BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
                   AND BIT_COUNT(stb.b_hated & my_tags.b) > 0
                 GROUP BY part.id,
                       part.affinity_tag_id_int
            """)
            self.conn.commit()

        # calc tam for angle
        self.conn.query(f"""
                UPDATE gb_calc.tmp_gaps__audience_angles aa
        INNER JOIN (
SELECT z1.id,
       z1.total_audience,
       z1.arpu,
       z1.affinity_tag_id_int,
       gb_platform.calc_tam(z1.total_audience, z1.arpu) AS tam
  FROM (
SELECT list.id,
       aa.arpu,
       list.affinity_tag_id_int,
       gb_platform.calc_total_audience(
		 	aap.loved_cnt, 
			aap.total_cnt, 
			r.rejected_cnt, 
			CAST(c.v1 AS UNSIGNED), 
			aa.loyalty_installs, 0, 0
		 ) AS total_audience
  FROM gb_calc.tmp_gaps__audience_angles_list list
  inner join gb_platform.audience_angle aa
  on list.id = aa.id
  inner join gb_platform.audience_angle_potential aap
  on list.id = aap.id
  AND aap.survey_id = {survey_id}
  inner JOIN gb_app.def_sheet_platform_values_constants c
  on c.param = '{CalcUtils.POTENTIAL_DOWNLOADS_MOBILE_WW}'
  left join gb_calc.tmp_gaps__audience_angles_rejected r
  on r.id = list.id
  and r.affinity_tag_id_int = list.affinity_tag_id_int
   ) z1
   ) z2
   ON z2.id = aa.id AND z2.affinity_tag_id_int = aa.affinity_tag_id_int
   SET aa.total_audience = z2.total_audience, aa.tam = z2.tam, aa.arpu = z2.arpu
                """)
        self.conn.commit()

        self.calc_utils.recreate_table(
            table_name="tmp_gaps__audience_angles_final",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
            affinity_tag_id_int int unsigned not null,
            id bigint unsigned not null,
            constraint x__table_name__x_pk
            primary key (affinity_tag_id_int)
            )
            SELECT DISTINCT aa.affinity_tag_id_int,
                   FIRST_VALUE(aa.id) OVER (PARTITION BY aa.affinity_tag_id_int ORDER BY aa.tam DESC) AS id
              FROM gb_calc.tmp_gaps__audience_angles aa
             WHERE aa.tam >= 0
            """
        )
        self.conn.commit()

        self.conn.query(f"""
        INSERT INTO gb_calc.results_gaps__opportunities 
        (platform_id, audience_angle_id, affinity_tag_id_int, age, 
        female, female_cnt, ltv, potential_cnt, total_cnt, rejected_cnt, threat_score,
        total_audience, tam, arpu, guid) 
        SELECT {platform_id} as platform_id, af.id as audience_angle_id,
               af.affinity_tag_id_int, 0 as age, 0 as female, 0 as female_cnt, 0 as ltv,
               aap.loved_cnt as potential_cnt,
               aap.total_cnt,
               aa.rejected_cnt,
               0 as threat_score,
               aa.total_audience,
               aa.tam,
               aa.arpu,
               uuid() as guid
          FROM gb_calc.tmp_gaps__audience_angles_final af,
               gb_platform.audience_angle_potential aap,
               gb_calc.tmp_gaps__audience_angles aa
         WHERE aap.id = af.id
           AND aa.id = af.id
           AND aa.affinity_tag_id_int = af.affinity_tag_id_int
        """)
        self.conn.commit()

        self.conn.query(f"""
        UPDATE gb_calc.results_gaps__opportunities  r
        INNER JOIN (
        SELECT z3.audience_angle_id, z3.affinity_tag_id_int,
                           MAX(z3.largest_age_group) as age, 
                           MAX(z3.largest_female_group) as female, 
                           MAX(z3.female_cnt) as female_cnt,
                           round(AVG(z3.spending)) AS ltv, 
                           MAX(z3.spending_row_total) AS filtered_rows,
                           MAX(total_rows.cnt) AS total_rows
                      FROM (
                            SELECT z2.*,
                                   SUM(z2.female) OVER (PARTITION BY z2.audience_angle_id) as female_cnt,
                                   FIRST_VALUE(z2.age) OVER (PARTITION BY z2.audience_angle_id ORDER BY z2.age_count DESC) AS largest_age_group,
                                   FIRST_VALUE(z2.female) OVER (PARTITION BY z2.audience_angle_id ORDER BY z2.female_count DESC) AS largest_female_group
                              FROM (
                                    SELECT z1.audience_angle_id,
                                           z1.affinity_tag_id_int,
                                           si.spending,
                                           si.age,
                                           si.female,
                                           ROW_NUMBER() OVER (PARTITION BY z1.audience_angle_id ORDER BY si.spending) AS spending_row_num,
                                           COUNT(1) OVER (PARTITION BY z1.audience_angle_id) AS spending_row_total,
                                           COUNT(si.age) OVER (PARTITION BY z1.audience_angle_id, si.age) AS age_count,
                                           COUNT(si.female) OVER (PARTITION BY z1.audience_angle_id, si.female) AS female_count
                                      FROM (
                                            SELECT r.audience_angle_id,
														         r.affinity_tag_id_int,
                                                   stb.survey_instance_int
                                              FROM gb_calc.results_gaps__opportunities r,
                                                   gb_platform.audience_angle aa,
                                                   gb_calc.tmp_gaps__selected_tags_per_angle my_tags,
                                                   gb_platform.platform_values_survey_tags_bin stb
                                             WHERE r.platform_id = {platform_id}
                                               AND r.audience_angle_id = aa.id
                                               AND aa.id = my_tags.id
                                               AND r.affinity_tag_id_int = my_tags.affinity_tag_id_int
                                               AND stb.survey_id = {survey_id}
                                               AND bit_count(stb.b_loved & aa.b) = aa.angle_cnt
                                               AND bit_count(stb.b_hated & my_tags.b) = 0
                                             GROUP BY 
                                                   r.audience_angle_id, 
                                                   r.affinity_tag_id_int,
                                                   stb.survey_instance_int
                                           ) z1,
                                           gb_platform.platform_values_survey_info si
                                     WHERE si.survey_meta_id = {survey_id}
                                       AND si.survey_instance_int = z1.survey_instance_int
                                   ) z2
                           ) z3,
                           (
                            SELECT COUNT(1) AS cnt 
                              FROM gb_platform.platform_values_survey_info si 
                             WHERE si.survey_meta_id = {survey_id}
                           ) AS total_rows
                     WHERE z3.spending_row_num IN (FLOOR((z3.spending_row_total + 1) / 2), FLOOR((z3.spending_row_total + 2) / 2) )
                     GROUP BY z3.audience_angle_id, z3.affinity_tag_id_int
                    ) z4
                    ON z4.audience_angle_id = r.audience_angle_id
                    AND z4.affinity_tag_id_int = r.affinity_tag_id_int
            SET r.age = z4.age, r.female = z4.female, r.female_cnt = z4.female_cnt, r.ltv = z4.ltv
        """)

        self.conn.query(f"""
        INSERT INTO gb_calc.results_gaps__opportunities_tags 
        (platform_id, audience_angle_id, tag_id_int) 
        SELECT {platform_id} as platform_id, af.id as audience_angle_id,
               d.tag_id_int
          FROM (
          select distinct af.id 
			 from gb_calc.tmp_gaps__audience_angles_final af
			 ) af,
            gb_platform.audience_angle aa,
            gb_platform.def_tags_bin d
         WHERE aa.id = af.id
           AND BIT_COUNT(aa.b & d.b) = 1
        """)
        self.conn.commit()

        cards_pages_cnt = round(self.conn.select_one("""
                SELECT count(1) as cnt FROM gb_calc.tmp_gaps__audience_angles_final aat
                """)["cnt"] / 50) + 1

        progress_data["state"] = "cards"
        progress_data["t"] = opportunities_pages_cnt + cards_pages_cnt

        # threat score

        page = 0
        limit = 50
        while True:

            print(f"Page: {page}")

            progress_data["p"] += 1
            update_progress()

            self.calc_utils.recreate_table(
                table_name="tmp_gaps__audience_angles_part",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    affinity_tag_id_int int unsigned not null,
                    constraint x__table_name__x_pk
                    primary key (affinity_tag_id_int)
                )
                SELECT aat.affinity_tag_id_int
                  FROM gb_calc.tmp_gaps__audience_angles_final aat
                 ORDER BY aat.affinity_tag_id_int
                 LIMIT {page * limit}, {limit}
                """
            )

            cnt = self.conn.select_one("""
            SELECT count(1) as cnt FROM gb_calc.tmp_gaps__audience_angles_part
            """)["cnt"]

            if cnt == 0:
                break

            page += 1

            # top 20 competitors per angle

            self.calc_utils.recreate_table(
                table_name="tmp_gaps__competitors_pool",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
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
                SELECT z1.id, z1.app_id_int, SUM(z1.score1 + z1.score2 + z1.score3) AS score
                  FROM (
                SELECT aaf.id,
                       t.app_id_int,
                       3 * BIT_COUNT(t.b & my_tags.b & survey_w.w_b & tags_subc.b) * tags_subc.competitors_pool_w * survey_w.w AS score1,
                         BIT_COUNT(t.b & survey_w.w_b & tags_subc.b) * tags_subc.competitors_pool_w * survey_w.w AS score2,
                         BIT_COUNT(my_tags.b & survey_w.w_b & tags_subc.b) * tags_subc.competitors_pool_w * survey_w.w AS score3
                  FROM gb_calc.tmp_gaps__audience_angles_part part,
                       gb_calc.tmp_gaps__audience_angles_final aaf,
                       gb_platform.audience_angle aa,
                       gb_platform.platform_values_tags_bin t,
                       gb_platform.audience_angle_tags_w survey_w,
                       gb_calc.tmp_gaps__selected_tags_per_angle my_tags,
                       gb_platform.def_tags_subc_bin tags_subc
                 WHERE part.affinity_tag_id_int = aaf.affinity_tag_id_int
                   AND my_tags.affinity_tag_id_int = aaf.affinity_tag_id_int
                   AND my_tags.id = aa.id
                   AND aaf.id = aa.id
                   AND aaf.id = survey_w.id
                   AND survey_w.survey_id = {survey_id}
                   AND BIT_COUNT(t.b & aa.b) = aa.angle_cnt
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
                    affinity_tag_id_int smallint unsigned not null,
                    app_id_int MEDIUMINT NOT NULL,
                    w smallint unsigned not null,
                    color TINYINT NOT NULL,
                    constraint x__table_name__x_pk
                       primary key (affinity_tag_id_int, app_id_int)
                )
                SELECT z3.affinity_tag_id_int, 
     z3.app_id_int, 
     round(z3.w) as w, 
     IF(tp.color = 'r', -1, (IF(tp.color = 'g', 1, 0))) as color 
FROM (
      SELECT z2.affinity_tag_id_int, 
             z2.app_id_int, 
             SUM(z2.w) AS w
        FROM (
              SELECT z1.affinity_tag_id_int, 
                     z1.app_id_int, 
                     (z1.same_cnt / GREATEST(m_all_cnt, c_all_cnt)) * z1.def_w AS w
                FROM (
                      SELECT part.affinity_tag_id_int, 
                             pool.app_id_int, 
                             BIT_COUNT(t.b & def_subc.b) AS c_all_cnt, 
                             my_tags_subc.cnt AS m_all_cnt, 
                             BIT_COUNT(t.b & def_subc.b & my_tags.b) AS same_cnt,
                             subc_w.w AS def_w
                        FROM gb_calc.tmp_gaps__audience_angles_part part,
                             gb_calc.tmp_gaps__audience_angles_final aa,
								     gb_calc.tmp_gaps__competitors_pool pool,
                             gb_platform.platform_values_tags_bin t,
                             gb_platform.def_tags_subc_bin def_subc,
                             gb_calc.tmp_gaps__selected_tags_per_angle my_tags,
                             gb_calc.tmp_gaps__selected_tags_subc_count_per_angle my_tags_subc,
                             (
                              SELECT p.subcategory_int,
                                     p.threatscore_similarity_w AS w
                                FROM gb_app.def_sheet_platform_product p
                               WHERE p.threatscore_similarity_w != 0
                               GROUP BY 
                                     p.subcategory_int, 
                                     p.threatscore_similarity_w
                             ) subc_w
                       WHERE part.affinity_tag_id_int = aa.affinity_tag_id_int
                         AND pool.id = aa.id
                         AND my_tags.id = aa.id
                         AND my_tags.affinity_tag_id_int = aa.affinity_tag_id_int
                         AND pool.app_id_int = t.app_id_int
                         AND my_tags.id = my_tags_subc.id
                         AND my_tags.affinity_tag_id_int = my_tags_subc.affinity_tag_id_int
                         AND my_tags_subc.subcategory_int = subc_w.subcategory_int
                         AND def_subc.subcategory_int = my_tags_subc.subcategory_int
                     ) z1
               WHERE z1.m_all_cnt + z1.c_all_cnt > 0
             ) z2
         GROUP BY 
               z2.affinity_tag_id_int, 
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
                    affinity_tag_id_int smallint UNSIGNED NOT NULL,
                    app_id_int MEDIUMINT UNSIGNED NOT NULL,
                    trend_w SMALLINT UNSIGNED NOT NULL,
                    trend_color TINYINT NOT NULL,
                    quality_w SMALLINT UNSIGNED NOT NULL,
                    quality_color TINYINT NOT NULL,
                    size_w SMALLINT UNSIGNED NOT NULL,
                    size_color TINYINT NOT NULL,
                    constraint x__table_name__x_pk
                       primary key (affinity_tag_id_int, app_id_int)
                )
                SELECT part.affinity_tag_id_int, pool.app_id_int,
                       CAST(trend.v3 AS UNSIGNED ) AS trend_w,
                       IF(trend.color = 'r', -1, IF(trend.color = 'g', 1, 0)) AS trend_color,
                       CAST(quality.v3 AS UNSIGNED ) AS quality_w,
                       IF(quality.color = 'r', -1, IF(quality.color = 'g', 1, 0)) AS quality_color,
                       CAST(size.v3 AS UNSIGNED ) AS size_w,
                       IF(size.color = 'r', -1, IF(size.color = 'g', 1, 0)) AS size_color
                  FROM gb_calc.tmp_gaps__audience_angles_part part,
                       gb_calc.tmp_gaps__audience_angles_final aa,
                       gb_calc.tmp_gaps__competitors_pool pool,
                       gb_platform.platform_values_apps ac,
                       gb_app.def_sheet_platform_threats_params trend,
                       gb_app.def_sheet_platform_threats_params quality,
                       gb_app.def_sheet_platform_threats_params size
                 WHERE part.affinity_tag_id_int = aa.affinity_tag_id_int
                   AND pool.id = aa.id     
                   AND pool.app_id_int = ac.app_id_int
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
                    affinity_tag_id_int smallint unsigned not null,
                    app_id_int MEDIUMINT NOT NULL,
                    tam_w SMALLINT UNSIGNED NOT NULL,
                    tam_color TINYINT NOT NULL,
                    constraint x__table_name__x_pk
                        primary key (affinity_tag_id_int, app_id_int)
                )
                SELECT z2.affinity_tag_id_int, 
                       z2.app_id_int,
                       CAST(tp.v3 AS UNSIGNED) AS tam_w,
                       IF (tp.color = 'r', -1, IF (tp.color = 'g', 1, 0)) AS tam_color
                  FROM (
                        SELECT z1.affinity_tag_id_int, 
                               z1.app_id_int, 
                               round(IF(z1.my_tam = 0, -1, z1.c_tam / z1.my_tam * 100)) AS tam_ratio
                          FROM (
                                SELECT part.affinity_tag_id_int, 
                                       pool.app_id_int, 
                                       tam_per_app.tam as c_tam,
                                       aa.tam as my_tam
                                  FROM gb_calc.tmp_gaps__audience_angles_part part,
                                       gb_calc.tmp_gaps__audience_angles_final aaf,
                                       gb_calc.tmp_gaps__audience_angles aa,
                                       gb_calc.tmp_gaps__competitors_pool pool,
                                       gb_platform.audience_angle_tam_per_app tam_per_app
                                 WHERE part.affinity_tag_id_int = aaf.affinity_tag_id_int
                                   AND pool.id = aaf.id
                                   AND aa.id = aaf.id
                                   AND aa.affinity_tag_id_int = aaf.affinity_tag_id_int     
                                   AND pool.app_id_int = tam_per_app.app_id_int
                                   AND tam_per_app.survey_id = {survey_id}
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
                    affinity_tag_id_int smallint unsigned not null,
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
                        primary key (affinity_tag_id_int, app_id_int)
                )
                SELECT part.affinity_tag_id_int, 
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
                           aa.quality,
                           aa.released_years
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
                  FROM gb_calc.tmp_gaps__audience_angles_part part
                  INNER join gb_calc.tmp_gaps__audience_angles_final aaf
                 ON aaf.affinity_tag_id_int = part.affinity_tag_id_int
                 INNER join gb_calc.tmp_gaps__audience_angles aa
                 ON aa.affinity_tag_id_int = aaf.affinity_tag_id_int
                 AND aa.id = aaf.id
                 INNER join gb_calc.tmp_gaps__competitors_pool pool
                 ON pool.id = aaf.id
                  LEFT JOIN gb_calc.tmp_gaps__ts_similarity s 
                    ON s.affinity_tag_id_int = part.affinity_tag_id_int 
                   AND s.app_id_int = pool.app_id_int
                  LEFT JOIN gb_calc.tmp_gaps__ts_tqs tqs 
                    ON tqs.affinity_tag_id_int = part.affinity_tag_id_int 
                   AND tqs.app_id_int = pool.app_id_int
                  LEFT JOIN gb_calc.tmp_gaps__ts_tam tam 
                    ON tam.affinity_tag_id_int = part.affinity_tag_id_int 
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
            self.conn.query(f"""
            UPDATE gb_calc.results_gaps__opportunities o
            INNER JOIN (
           SELECT z3.affinity_tag_id_int, 
       af.id,
       round(AVG(z3.threat_score)) AS threat_score
  FROM (
  SELECT z2.affinity_tag_id_int,
  z2.threat_score,
  z2.row_num,
  COUNT(1) OVER (PARTITION BY z2.affinity_tag_id_int) AS total_num
    FROM 
  (
        SELECT z1.affinity_tag_id_int,
               z1.threat_score,
               ROW_NUMBER() OVER (PARTITION BY z1.affinity_tag_id_int ORDER BY z1.threat_score DESC) AS row_num
          FROM (
			        SELECT tf.affinity_tag_id_int,
			               GREATEST(0, CAST(tf.threat_score as SIGNED) - CAST(tf.threat_score_discount as SIGNED)) AS threat_score
			          FROM gb_calc.tmp_gaps__ts_final tf
			         ORDER BY tf.threat_score - tf.threat_score_discount DESC
			      ) z1
         ) z2
         WHERE z2.row_num <= 5
         ) z3,
         gb_calc.tmp_gaps__audience_angles_final af
         WHERE z3.row_num IN (FLOOR((z3.total_num + 1) / 2), FLOOR((z3.total_num + 2) / 2) )
         AND af.affinity_tag_id_int = z3.affinity_tag_id_int 
                GROUP BY z3.affinity_tag_id_int, af.id
                                ) z4
                                ON z4.id = o.audience_angle_id
                                AND z4.affinity_tag_id_int = o.affinity_tag_id_int
                                SET o.threat_score = z4.threat_score
                                WHERE o.platform_id = {platform_id}
            """)
            self.conn.commit()

    def generate_client_data(
            self,
            platform_id: int | None = None,
            opportunity_guid: str | None = None
    ) -> Optional[dict]:

        if platform_id is None and opportunity_guid is None:
            raise Exception(f"platform_id or opportunity_guid must be set")

        if platform_id is None:
            row_platform_id = self.conn.select_one_or_none("""
            SELECT o.platform_id
              FROM gb_calc.results_gaps__opportunities o
             WHERE o.guid = %s
            """, [opportunity_guid])

            if row_platform_id is None:
                return None

            platform_id = row_platform_id["platform_id"]
            
        if platform_id is None:
            raise Exception('platform_id is None')

        row_request = self.conn.select_one("""
        SELECT r.input_data
          FROM gb_calc.requests r
         WHERE r.platform_id = %s
        """, [platform_id])

        input_data = json.loads(row_request["input_data"])

        rows_audiences = self.conn.select_all("""
        SELECT r.platform_id, r.audience_angle_id, r.affinity_tag_id_int, r.age, r.female, r.female_cnt, r.ltv,
        r.total_audience, r.tam, r.potential_cnt, r.threat_score, p.unlocked, r.guid,
        r.total_cnt, r.rejected_cnt, aa.loyalty_installs, aa.arpu
          FROM gb_calc.results_gaps__opportunities r,
               gb_platform.audience_angle aa,
               gb_app.def_sheet_platform_product p
         WHERE r.platform_id = %s
           AND p.tag_id_int = r.affinity_tag_id_int
           AND aa.id = r.audience_angle_id
         ORDER BY r.tam DESC
        """, [platform_id])

        rows_audiences_tags = self.conn.select_all("""
        SELECT r.audience_angle_id, r.tag_id_int
          FROM gb_calc.results_gaps__opportunities_tags r
         WHERE r.platform_id = %s
        """, [platform_id])

        tags_per_audience = {}
        for row in rows_audiences_tags:
            audience_angle_id = row["audience_angle_id"]
            if audience_angle_id not in tags_per_audience:
                tags_per_audience[audience_angle_id] = []
            tags_per_audience[audience_angle_id].append(row["tag_id_int"])

        items = []
        for row in rows_audiences:

            if opportunity_guid is not None and row["guid"] != opportunity_guid:
                continue

            d = {
                "audience_detail": self.calc_utils.audience_to_client_data_2(
                    platform_id=platform_id,
                    loved_cnt=row["potential_cnt"],
                    data=row
                ),
                "affinity": {
                    UserObfuscator.TAG_ID_INT: row["affinity_tag_id_int"]
                },
                "ts": row["threat_score"],
                "tam": row["tam"],
                "guid": row["guid"]
            }

            if row["unlocked"] == 0:
                d["locked"] = True

            audience_angle_id = d["audience_detail"][UserObfuscator.AUDIENCE_ANGLE_ID_INT]
            if audience_angle_id in tags_per_audience:
                d[UserObfuscator.TAG_IDS_INT] = tags_per_audience[audience_angle_id]

            items.append(d)

        items.sort(key=lambda x: x["tam"], reverse=True)

        return {
            "platform_id": platform_id,
            UserObfuscator.TAG_IDS_INT: [
                tag_detail["tag_id_int"] for tag_detail in input_data["tag_details"]
            ],
            "items": items
        }
