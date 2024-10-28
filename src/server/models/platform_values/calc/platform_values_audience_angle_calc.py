import json

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.user.user_obfuscator import UserObfuscator
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.tags.tags_constants import TagsConstants
from src.utils.gembase_utils import GembaseUtils


class PlatformValuesAudienceAngleCalc:

    @staticmethod
    def calc(
            conn: DbConnection,
            platform_id: int,
            survey_id: int,
            dev_id_int: int,
            tag_details: [],
            include_angle: int | None = None,
            exclusive_angle: int | None = None,
            skip_copy_to_results=False,
            skip_top_behaviors=False,
            tag_details_multi=None
    ):
        if tag_details_multi is None:
            tag_details_multi = [
                {
                    "multi_id": 0,
                    "include_angle": include_angle,
                    "exclusive_angle": exclusive_angle,
                    "tag_details": tag_details
                }
            ]

        bulk_data = []
        for it in tag_details_multi:
            for tag_detail in it["tag_details"]:
                bulk_data.append((it["multi_id"], tag_detail[UserObfuscator.TAG_ID_INT], tag_detail["tag_rank"]))

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_tags",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                tag_id_int int not null,
                tag_rank int not null,
                CONSTRAINT x__table_name__x_pk
                    PRIMARY KEY (multi_id, tag_id_int)
            )
            """
        )

        conn.bulk("""
        INSERT INTO platform_values.tmp_my_tags (multi_id, tag_id_int, tag_rank)
        VALUES (%s, %s, %s)
        """, bulk_data)
        conn.commit()

        include_angle_query = ""
        include_angle_query_arr = []
        for it in tag_details_multi:
            if "include_angle" in it and it["include_angle"] is not None:
                include_angle_id = it["include_angle"]
                include_angle_query_arr.append(f"SELECT {it['multi_id']} as multi_id, {include_angle_id} as audience_angle_id")
        if len(include_angle_query_arr) > 0:
            include_angle_query = f"""
            UNION
            {" UNION ".join(include_angle_query_arr)}
            """

        query = f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                audience_angle_id int not null,
                CONSTRAINT x__table_name__x_pk
                    PRIMARY KEY (multi_id, audience_angle_id)
            )
            SELECT my_tags.multi_id, aa.id as audience_angle_id
              FROM platform.audience_angle aa,
                   platform.audience_angle_tags aat,
                   platform_values.tmp_my_tags my_tags,
                   platform.def_tags d
             WHERE (
                    my_tags.tag_rank = {TagsConstants.TAG_RANK_PRIMARY} 
                 OR my_tags.tag_rank = {TagsConstants.TAG_RANK_SECONDARY})
                AND aa.id = aat.id
                AND aat.tag_id_int = my_tags.tag_id_int
                AND aa.valid_combination = 1
                AND d.tag_id_int = my_tags.tag_id_int
                AND d.subcategory_int IN ({TagsConstants.SUBCATEGORY_GENRE_ID}, {TagsConstants.SUBCATEGORY_TOPICS_ID})
              GROUP BY my_tags.multi_id, aa.id, aa.angle_cnt
              HAVING count(1) = aa.angle_cnt
               {include_angle_query}
            """

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_angles",
            query=query
        )

        bulk_data_delete = []
        bulk_data_insert = []
        for it in tag_details_multi:
            if "exclusive_angle" in it and it["exclusive_angle"] is not None:
                bulk_data_delete.append((it["multi_id"],))
                bulk_data_insert.append((it["multi_id"], it["exclusive_angle"]))

        if len(bulk_data_delete) > 0:
            conn.bulk("""
            DELETE FROM platform_values.tmp_my_angles
            WHERE multi_id = %s
            """, bulk_data_delete)

        if len(bulk_data_insert) > 0:
            conn.bulk("""
            INSERT INTO platform_values.tmp_my_angles (multi_id, audience_angle_id)
            VALUES (%s, %s)
            """, bulk_data_insert)

        bin_bytes_cnt = PlatformValuesHelper.get_bin_bytes_cnt(conn)

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_my_tags_bin",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                b BINARY({bin_bytes_cnt}) not null,
                CONSTRAINT x__table_name__x_pk
                    PRIMARY KEY (multi_id)
            )
            SELECT my_tags.multi_id, 
                   BIT_OR(d.b) as b
              FROM platform.def_tags_bin d,
                   platform_values.tmp_my_tags my_tags
             WHERE my_tags.tag_id_int = d.tag_id_int
             GROUP BY my_tags.multi_id
            """
        )

        # needs this temporary table twice in some queries
        for i in range(2):
            PlatformValuesHelper.recreate_table(
                conn=conn,
                table_name=f"tmp_audience_angles_{i}",
                query=f"""
                CREATE TABLE platform_values.x__table_name__x (
                    multi_id int not null,
                    id BIGINT UNSIGNED NOT NULL,
                    b BINARY({bin_bytes_cnt}) NOT NULL,
                    angle_cnt INT UNSIGNED NOT NULL,
                    arpu DECIMAL(8,4) NOT NULL,
                    loyalty_installs BIGINT UNSIGNED NOT NULL,
                    potential_downloads BIGINT UNSIGNED NOT NULL,
                    quality INT UNSIGNED DEFAULT 40 NOT NULL,
                    released_years INT UNSIGNED DEFAULT 0 NOT NULL,
                    loved_ratio_ext decimal(8,4) not null,
                    rejected_ratio_ext decimal(8,4) not null,
                    CONSTRAINT x__table_name__x_pk
                    PRIMARY KEY (multi_id, id)
                )
                SELECT my_tags_bin.multi_id,
                       aap.id, 
                       aa.b, 
                       aa.angle_cnt, 
                       aa.arpu, 
                       platform.get_loyalty_installs(
                           aap.loyalty_installs,
                           platform.get_potential_downloads(
                               my_tags_bin.b,
                               aap.id
                           )
                       ) as loyalty_installs,
                       platform.get_potential_downloads(
                               my_tags_bin.b,
                               aap.id
                           ) as potential_downloads,
                       aap.loved_ratio_ext,
                       aap.rejected_ratio_ext
                  FROM platform.audience_angle_potential aap,
                       platform.audience_angle aa,
                       platform_values.tmp_my_tags_bin my_tags_bin,
                       platform_values.tmp_my_angles my_angles
                 WHERE aap.id = my_angles.audience_angle_id
                   AND my_angles.multi_id = my_tags_bin.multi_id
                   AND aap.survey_id = {survey_id}
                   AND aa.id = aap.id
                """
            )

        row_yearly_revenues = conn.select_one_or_none(f"""
        SELECT d.yearly_revenues
          FROM platform.platform_values_devs d
         WHERE d.dev_id_int = {dev_id_int}
        """)
        yearly_revenues = 0
        if row_yearly_revenues is not None:
            yearly_revenues = row_yearly_revenues["yearly_revenues"]

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_audience_angles__final",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                id bigint unsigned not null,
                age smallint unsigned not null,
                female tinyint unsigned not null,
                female_cnt smallint unsigned not null,
                ltv smallint unsigned not null,
                loved_cnt smallint unsigned not null,
                total_cnt smallint unsigned not null,
                rejected_cnt smallint unsigned not null,
                loyalty_installs bigint not null,
                potential_downloads bigint not null,
                arpu DECIMAL(8,4) not null,
                discount int default 0 not null,
                exp_d smallint unsigned not null,
                rev_d bigint unsigned not null,
                qua_d smallint unsigned not null, 
                quality_full int unsigned default 40 not null,
                quality_portfolio int unsigned default 0 not null,
                total_audience bigint unsigned default 0 not null,
                loved_ratio_ext decimal(8, 4) default 0 not null,
                rejected_ratio_ext decimal(8, 4) default 0 not null,
                dev_id_int int default 0 not null,
                constraint x__table_name__x_pk
                    primary key (multi_id, id)
            )
            SELECT z4.multi_id,
                   z4.id, 
                   z4.age,
                   z4.female,
                   z4.female_cnt,
                   z4.ltv as ltv,
                   z4.loved_cnt,
                   z4.total_cnt,
                   0 as rejected_cnt,
                   aa.loyalty_installs,
                   aa.potential_downloads,
                   aa.arpu,
                   0 as exp_d,
                   {yearly_revenues} as rev_d,
                   0 as qua_d,
                   0 as quality_full,
                   0 as quality_portfolio,
                   0 as total_audience,
                   aa.loved_ratio_ext,
                   aa.rejected_ratio_ext,
                   {dev_id_int} as dev_id_int
              FROM (
                    SELECT z3.multi_id,
                           z3.id,
                           MAX(z3.largest_age_group) as age, 
                           MAX(z3.largest_female_group) as female, 
                           MAX(z3.female_cnt) as female_cnt,
                           round(AVG(z3.spending)) AS ltv, 
                           MAX(z3.spending_row_total) AS loved_cnt,
                           MAX(total_rows.cnt) AS total_cnt
                      FROM (
                            SELECT z2.*,
                                   SUM(z2.female) OVER (PARTITION BY z2.multi_id, z2.id) as female_cnt,
                                   FIRST_VALUE(z2.age) OVER (PARTITION BY z2.multi_id, z2.id ORDER BY z2.age_count DESC) AS largest_age_group,
                                   FIRST_VALUE(z2.female) OVER (PARTITION BY z2.multi_id, z2.id ORDER BY z2.female_count DESC) AS largest_female_group
                              FROM (
                                    SELECT z1.multi_id,
                                           z1.id,
                                           si.spending,
                                           si.age,
                                           si.female,
                                           ROW_NUMBER() OVER (PARTITION BY z1.multi_id, z1.id ORDER BY si.spending) AS spending_row_num,
                                           COUNT(1) OVER (PARTITION BY z1.multi_id, z1.id) AS spending_row_total,
                                           COUNT(1) OVER (PARTITION BY z1.multi_id, z1.id, si.age) AS age_count,
                                           COUNT(1) OVER (PARTITION BY z1.multi_id, z1.id, si.female) AS female_count
                                      FROM (
                                            SELECT DISTINCT 
                                                   aa.multi_id,
                                                   aa.id,
                                                   stb.survey_instance_int
                                              FROM platform_values.tmp_audience_angles_0 aa,
                                                   platform.platform_values_survey_tags_bin stb
                                             WHERE stb.survey_id = {survey_id}
                                               AND BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
                                           ) z1,
                                           platform.platform_values_survey_info si
                                     WHERE si.survey_meta_id = {survey_id}
                                       AND si.survey_instance_int = z1.survey_instance_int
                                   ) z2
                           ) z3,
                           (
                            SELECT COUNT(1) AS cnt 
                              FROM platform.platform_values_survey_info si 
                             WHERE si.survey_meta_id = {survey_id}
                           ) AS total_rows
                     WHERE z3.spending_row_num IN (FLOOR((z3.spending_row_total + 1) / 2), FLOOR((z3.spending_row_total + 2) / 2) )
                     GROUP BY z3.multi_id, z3.id
                   ) z4,
                   platform_values.tmp_audience_angles_1 aa
                   where aa.id = z4.id
                     and aa.multi_id = z4.multi_id
            """
        )

        # update rejected

        conn.query(f"""
        UPDATE platform_values.tmp_audience_angles__final f
        INNER JOIN (
        SELECT aa.multi_id, aa.id, count(1) as rejected_cnt
          FROM platform_values.tmp_audience_angles_0 aa,
               platform.platform_values_survey_tags_bin stb,
               platform_values.tmp_my_tags_bin my_tags_bin
         WHERE BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
           AND BIT_COUNT(stb.b_rejected & my_tags_bin.b) > 0
           AND stb.survey_id = {survey_id}
           AND my_tags_bin.multi_id = aa.multi_id
         group by aa.multi_id, aa.id
         ) z1
         ON z1.id = f.id AND z1.multi_id = f.multi_id
         SET f.rejected_cnt = z1.rejected_cnt
        """)
        conn.commit()

        # discount per node

        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_discount_per_angle",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
                multi_id int not null,
                id bigint unsigned not null,
                quality int unsigned default 40 not null,
                quality_full int unsigned default 40 not null,
                quality_portfolio int unsigned default 0 not null,
                released_years int unsigned default 0 not null,
                constraint x__table_name__x_pk
                primary key (multi_id, id)
            )
            select multi_id, id 
              FROM platform_values.tmp_audience_angles_0
            """
        )

        conn.query(f"""
        UPDATE platform_values.tmp_discount_per_angle aa
        INNER JOIN (
                    SELECT z3.multi_id,
                           z3.id, 
                           ROUND(AVG(z3.quality)) AS quality,
                           MAX(z3.quality_portfolio) as quality_portfolio
                      FROM (
                            SELECT z2.multi_id,
                                   z2.id, 
                                   z2.quality,
                                   ROW_NUMBER() OVER (PARTITION BY z2.multi_id, z2.id ORDER BY z2.quality) AS row_num,
                                   COUNT(1) OVER (PARTITION BY z2.multi_id, z2.id) AS total_num,
                                   MAX(z2.quality_portfolio) OVER (PARTITION BY z2.id) as quality_portfolio
                              FROM (
                                    SELECT aa.multi_id, 
                                           aa.id,
                                           apps.rating * IF(BIT_COUNT(tb.b & aa.b) = aa.angle_cnt, 1, 0.75) AS quality,
                                           IF(BIT_COUNT(tb.b & aa.b) = aa.angle_cnt, 1, 0) as quality_portfolio
                                      FROM (
                                            SELECT a.app_id_int, a.rating
                                              FROM platform.platform_values_devs_apps da,
                                                   platform.platform_values_apps a
                                             WHERE da.dev_id_int = {dev_id_int}
                                               AND da.app_id_int = a.app_id_int
                                               AND a.tier >= 3
                                           ) apps,
                                           platform.platform_values_tags_bin tb,
                                           platform_values.tmp_audience_angles_0 aa
                                     WHERE tb.app_id_int = apps.app_id_int
                                   ) z2
                           ) z3
                     WHERE z3.row_num IN (FLOOR((z3.total_num + 1) / 2), FLOOR((z3.total_num + 2) / 2) ) 
                     GROUP BY z3.multi_id, z3.id 
                    ) z4 
         ON z4.id = aa.id AND z4.multi_id = aa.multi_id
        SET aa.quality = z4.quality,
            aa.quality_portfolio = z4.quality_portfolio
        """)
        conn.commit()

        conn.query(f"""
        UPDATE platform_values.tmp_discount_per_angle aa
        INNER JOIN (
                    SELECT z3.multi_id,
                           z3.id,
                           ROUND(AVG(z3.quality_full)) AS quality_full
                      FROM (
                            SELECT z2.multi_id,
                                   z2.id,
                                   z2.quality_full,
                                   ROW_NUMBER() OVER (PARTITION BY z2.multi_id, z2.id ORDER BY z2.quality_full) AS row_num,
                                   COUNT(1) OVER (PARTITION BY z2.multi_id, z2.id) AS total_num
                              FROM (
                                    SELECT my_angles.multi_id,
                                           aa.id,
                                           apps.rating as quality_full
                                      FROM (
                                            SELECT a.app_id_int,
                                                   a.rating
                                              FROM platform.platform_values_devs_apps da,
                                                   platform.platform_values_apps a
                                             WHERE da.dev_id_int = {dev_id_int}
                                               AND da.app_id_int = a.app_id_int
                                               AND a.tier >= 3
                                           ) apps,
                                           platform.platform_values_tags_bin tb,
                                           platform.audience_angle aa,
                                           platform_values.tmp_audience_angles_0 my_angles
                                     WHERE tb.app_id_int = apps.app_id_int
                                       AND aa.id = my_angles.id
                                   ) z2
                           ) z3
                     WHERE z3.row_num IN (FLOOR((z3.total_num + 1) / 2), FLOOR((z3.total_num + 2) / 2) )
                     GROUP BY z3.multi_id, z3.id
                   ) z4
         ON z4.id = aa.id AND z4.multi_id = aa.multi_id
        SET aa.quality_full = z4.quality_full
        """)
        conn.commit()

        conn.query(f"""
        UPDATE platform_values.tmp_discount_per_angle aa
        INNER JOIN (
                    SELECT z3.multi_id, z3.id, MAX(z3.released_years) AS released_years
                      FROM (
                            SELECT z2.multi_id,
                                   z2.id, 
                                   z2.released_years
                              FROM (
                                    SELECT aa.multi_id,
                                           aa.id,
                                           apps.released_years
                                      FROM (
                                            SELECT a.app_id_int, a.released_years
                                              FROM platform.platform_values_devs_apps da,
                                                   platform.platform_values_apps a
                                             WHERE da.dev_id_int = {dev_id_int}
                                               AND da.app_id_int = a.app_id_int
                                               AND a.tier >= 3
                                           ) apps,
                                           platform.platform_values_tags_bin tb,
                                           platform_values.tmp_audience_angles_0 aa
                                     WHERE tb.app_id_int = apps.app_id_int
                                       AND BIT_COUNT(tb.b & aa.b) = aa.angle_cnt
                                   ) z2
                           ) z3
                     GROUP BY z3.multi_id, z3.id
                   ) z4
         ON z4.id = aa.id AND z4.multi_id = aa.multi_id
        SET aa.released_years = z4.released_years
        """)
        conn.commit()

        conn.query("""
        UPDATE platform_values.tmp_audience_angles__final f
         INNER JOIN platform_values.tmp_discount_per_angle d 
            ON d.id = f.id AND d.multi_id = f.multi_id
           SET f.qua_d = d.quality, 
               f.exp_d = d.released_years,
               f.quality_full = d.quality_full,
               f.quality_portfolio = d.quality_portfolio,
               f.discount = platform.calc_discount(
                   f.rev_d,
                   d.quality,
                   d.released_years
               )
        """)
        conn.commit()

        # total audience
        conn.query(f"""
        UPDATE platform_values.tmp_audience_angles__final
        SET total_audience = platform.calc_total_audience(
            loved_cnt,
            total_cnt,
            rejected_cnt,
            potential_downloads,
            loyalty_installs,
            loved_ratio_ext,
            rejected_ratio_ext
        )
        """)
        conn.commit()

        # top behaviors

        if not skip_top_behaviors:
            query = f"""
                CREATE TABLE platform_values.x__table_name__x (
                    multi_id int not null,
                    id bigint unsigned not null,
                    tag_id_int smallint unsigned not null,
                    tag_order tinyint unsigned not null,
                    constraint x__table_name__x_pk
                        primary key (multi_id, id, tag_id_int)
                )
                SELECT z3.multi_id,
                       z3.id, 
                       z3.tag_id_int, 
                       z3.row_num as tag_order
                  FROM (
                        SELECT z2.multi_id,
                               z2.id, 
                               z2.tag_id_int,
                               ROW_NUMBER() OVER (PARTITION BY z2.multi_id, z2.id, z2.subcategory_int ORDER BY cnt DESC) AS row_num
                          FROM (
                                SELECT z1.multi_id,
                                       z1.id, 
                                       p.tag_id_int,
                                       p.subcategory_int,
                                       COUNT(1) AS cnt
                                  FROM (
                                        SELECT DISTINCT 
                                               aa.multi_id,
                                               aa.id, 
                                               stb.survey_instance_int
                                          FROM platform_values.tmp_audience_angles_0 aa,
                                               platform.platform_values_survey_tags_bin stb,
                                               platform_values.tmp_my_tags_bin my_tags_bin
                                         WHERE stb.survey_id = {survey_id}
                                           AND BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
                                           AND BIT_COUNT(stb.b_rejected & my_tags_bin.b) = 0
                                           AND my_tags_bin.multi_id = aa.multi_id
                                       ) z1,
                                       platform.platform_values_survey_tags st,
                                       app.def_sheet_platform_product p
                                 WHERE p.subcategory_int IN ({TagsConstants.SUBCATEGORY_GENRE_ID}, {TagsConstants.SUBCATEGORY_TOPICS_ID}, {TagsConstants.SUBCATEGORY_BEHAVIORS_ID}, {TagsConstants.SUBCATEGORY_NEEDS_ID}, {TagsConstants.SUBCATEGORY_DOMAINS_ID})
                                   AND st.survey_meta_id = {survey_id}
                                   AND st.survey_instance_int = z1.survey_instance_int
                                   AND st.tag_id_int = p.tag_id_int
                                   AND st.loved = 1
                                 GROUP BY 
                                       z1.multi_id,
                                       z1.id, 
                                       p.tag_id_int,
                                       p.subcategory_int
                               ) z2
                       ) z3
                 WHERE z3.row_num <= 3
                """
            PlatformValuesHelper.recreate_table(
                conn=conn,
                table_name="tmp_audience_angles__top_tags",
                query=query
            )
        else:
            PlatformValuesHelper.recreate_table(
                conn=conn,
                table_name="tmp_audience_angles__top_tags",
                query="""
                CREATE TABLE platform_values.x__table_name__x (
                    multi_id int not null,
                    id bigint unsigned not null,
                    tag_id_int smallint unsigned not null,
                    tag_order tinyint unsigned not null,
                    constraint x__table_name__x_pk
                        primary key (multi_id, id, tag_id_int)
                )
                """
            )

        if not skip_copy_to_results:
            PlatformValuesAudienceAngleCalc.copy_data_to_results(
                conn=conn,
                platform_id=platform_id
            )

    @staticmethod
    def clear_results(
            conn: DbConnection,
            platform_id: int
    ):
        conn.query("""
        DELETE FROM platform_values.results_audience_angles__final
         WHERE platform_id = %s
        """, [platform_id])
        conn.commit()

        conn.query("""
        DELETE FROM platform_values.results_audience_angles__top_tags WHERE platform_id = %s
        """, [platform_id])
        conn.commit()

        conn.query("""
        DELETE FROM platform_values.results_audience_angle__input_tags WHERE platform_id = %s
        """, [platform_id])
        conn.commit()

    @staticmethod
    def copy_data_to_results(
            conn: DbConnection,
            platform_id: int
    ):
        conn.query("""
        INSERT INTO platform_values.results_audience_angles__final
        (platform_id, multi_id, audience_angle_id, age, female, female_cnt, ltv, 
        loved_cnt, total_cnt, rejected_cnt, arpu, 
        loyalty_installs, potential_downloads, exp_d, rev_d, qua_d, total_audience, loved_ratio_ext,
        rejected_ratio_ext, quality_full, quality_portfolio, dev_id_int, discount)
        SELECT %s as platform_id, f.multi_id, f.id as audience_angle_id, f.age, f.female, f.female_cnt, f.ltv,
        f.loved_cnt, f.total_cnt, f.rejected_cnt, f.arpu,
        f.loyalty_installs, f.potential_downloads, f.exp_d, f.rev_d, f.qua_d, f.total_audience,
        f.loved_ratio_ext, f.rejected_ratio_ext, f.quality_full, f.quality_portfolio, f.dev_id_int, f.discount
        FROM platform_values.tmp_audience_angles__final f
        """, [platform_id])
        conn.commit()

        conn.query("""
        INSERT INTO platform_values.results_audience_angles__top_tags
        (platform_id, audience_angle_id, tag_id_int, tag_order)
        SELECT %s as platform_id, f.id as audience_angle_id, f.tag_id_int, f.tag_order
        FROM platform_values.tmp_audience_angles__top_tags f
        """, [platform_id])
        conn.commit()

        conn.query("""
        INSERT INTO platform_values.results_audience_angle__input_tags
        (platform_id, multi_id, tag_id_int, tag_rank)
        SELECT %s as platform_id, my_tags.multi_id, my_tags.tag_id_int, my_tags.tag_rank
        FROM platform_values.tmp_my_tags my_tags
        """, [platform_id])
        conn.commit()

    @staticmethod
    def generate_client_data(
            conn: DbConnection,
            platform_id: int,
            is_admin=False
    ) -> {}:

        rows_final = conn.select_all("""
        SELECT f.row_id, f.multi_id, f.audience_angle_id, f.loved_cnt, f.total_cnt, f.rejected_cnt, f.age, 
               f.female, f.female_cnt, f.ltv, f.arpu, 
               f.exp_d, f.rev_d, f.qua_d, f.loyalty_installs, f.potential_downloads,
               f.loved_ratio_ext, f.rejected_ratio_ext, f.total_audience,
               aa.installs
          FROM platform_values.results_audience_angles__final f,
               platform.audience_angle aa
         where f.platform_id = %s
           and f.audience_angle_id = aa.id
        """, [platform_id])

        row_input_data = conn.select_one_or_none("""
        SELECT r.input_data FROM platform_values.requests r
        WHERE r.platform_id = %s
        """, [platform_id])

        angle_ranked_tags_sort_value = {}
        ranks = [TagsConstants.TAG_RANK_PRIMARY, TagsConstants.TAG_RANK_SECONDARY, TagsConstants.TAG_RANK_TERTIARY]
        ranked_tags = {}
        for rank in ranks:
            ranked_tags[rank] = []
        input_tag_ids = [-1]
        if row_input_data is not None and row_input_data["input_data"] is not None:
            input_data = json.loads(row_input_data["input_data"])
            if "tag_details" in input_data:
                for it in input_data["tag_details"]:
                    tag_rank = it["tag_rank"]
                    if tag_rank in ranked_tags:
                        ranked_tags[tag_rank].append(it["tag_id_int"])
                input_tag_ids = [it["tag_id_int"] for it in input_data["tag_details"]]

            for i in range(len(ranks)):
                rank_i = ranks[i]
                for j in range(i, len(ranks)):
                    rank_j = ranks[j]
                    for k in ranked_tags[rank_i]:
                        for l in ranked_tags[rank_j]:
                            if k == l:
                                angle_ranked_tags_sort_value[k] = rank_i * 100 + 50
                            else:
                                angle_id = PlatformValuesHelper.create_audience_angle_2_comb_id(tag_id_int_1=k, tag_id_int_2=l)
                                angle_ranked_tags_sort_value[angle_id] = rank_i * 100 + rank_j

        rows_app_platforms = conn.select_all(f"""
        SELECT z2.id, dtb2.tag_id_int
          FROM (
                SELECT aa.id, platform.get_app_platforms(z1.b, f.audience_angle_id) as app_platforms
                  FROM platform_values.results_audience_angles__final f,
                       platform.audience_angle aa,
                       (
                           SELECT BIT_OR(dtb.b) as b
                             FROM platform.def_tags_bin dtb
                            WHERE dtb.tag_id_int IN ({conn.values_arr_to_db_in(input_tag_ids, int_values=True)})
                       ) z1
                 where f.platform_id = %s
                   and f.audience_angle_id = aa.id
           ) z2,
           platform.def_tags_bin dtb2
           WHERE BIT_COUNT(z2.app_platforms & dtb2.b) = 1
        """, [platform_id])

        app_platforms_per_angle = {}
        for row in rows_app_platforms:
            if row["id"] not in app_platforms_per_angle:
                app_platforms_per_angle[row["id"]] = []
            app_platforms_per_angle[row["id"]].append(row["tag_id_int"])

        rows_tags = conn.select_all("""
        SELECT af.audience_angle_id, aat.tag_id_int
          FROM platform.audience_angle_tags aat,
               platform_values.results_audience_angles__final af
          where aat.id = af.audience_angle_id
            and af.platform_id = %s
        """, [platform_id])

        map_audience_tags = {}
        for row in rows_tags:
            if row["audience_angle_id"] not in map_audience_tags:
                map_audience_tags[row["audience_angle_id"]] = []
            map_audience_tags[row["audience_angle_id"]].append(row[UserObfuscator.TAG_ID_INT])

        rows_top_tags = conn.select_all("""
        SELECT f.audience_angle_id, f.tag_id_int, f.tag_order, p.subcategory_int
          FROM platform_values.results_audience_angles__top_tags f,
               app.def_sheet_platform_product p
          where f.platform_id = %s
          AND p.tag_id_int = f.tag_id_int
          order by f.tag_order
        """, [platform_id])

        helper = PlatformValuesHelper(conn=conn)

        data_per_angle = {}

        for row in rows_final:

            if row["loved_cnt"] == 0:
                continue

            audience_angle_id = row["audience_angle_id"]

            app_platforms = []
            if audience_angle_id in app_platforms_per_angle:
                app_platforms = app_platforms_per_angle[audience_angle_id]

            data_per_angle[audience_angle_id] = helper.audience_to_client_data_2(
                data=row,
                platform_id=platform_id,
                app_platforms=app_platforms,
                is_admin=is_admin,
                angle_tags=map_audience_tags[audience_angle_id] if audience_angle_id in map_audience_tags else NOne
            )

        top_tags_map = {
            "top_behaviors": TagsConstants.SUBCATEGORY_BEHAVIORS_ID,
            "top_needs": TagsConstants.SUBCATEGORY_NEEDS_ID,
            "top_domains": TagsConstants.SUBCATEGORY_DOMAINS_ID,
            "top_genres": TagsConstants.SUBCATEGORY_GENRE_ID,
            "top_topics": TagsConstants.SUBCATEGORY_TOPICS_ID,
        }

        for row in rows_top_tags:
            audience_angle_id = row["audience_angle_id"]
            locked = GembaseUtils.try_get_from_dict(data_per_angle, audience_angle_id, "locked")
            if audience_angle_id in data_per_angle and (locked is None or not locked):
                for k in top_tags_map:
                    if row["subcategory_int"] == top_tags_map[k]:
                        if k not in data_per_angle[audience_angle_id]:
                            data_per_angle[audience_angle_id][k] = {
                                UserObfuscator.TAG_IDS_INT: []
                            }
                        data_per_angle[audience_angle_id][k][UserObfuscator.TAG_IDS_INT].append(
                            row[UserObfuscator.TAG_ID_INT]
                        )

        arr = [data_per_angle[audience_angle_id] for audience_angle_id in data_per_angle]

        arr.sort(key=lambda x: (-angle_ranked_tags_sort_value[x[UserObfuscator.AUDIENCE_ANGLE_ID_INT]] if x[UserObfuscator.AUDIENCE_ANGLE_ID_INT] in angle_ranked_tags_sort_value else -500, x["total_audience"]), reverse=True)

        arr = arr[:20]

        return arr
