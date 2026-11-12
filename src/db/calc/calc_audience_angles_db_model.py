from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_db_model import BaseDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.calc.helpers.audience_stats import AudienceStats
from src.db.games_db_model import GamesDbModel
from src.models.user_obfuscator import UserObfuscator


class CalcAudienceAnglesDbModel(BaseDbModel):

    def __init__(self, conn: DbConnection):
        super(CalcAudienceAnglesDbModel, self).__init__(conn=conn)
        self.calc_utils = CalcUtils(conn=conn)
        
    def calc(
            self,
            platform_id: int,
            survey_id: int,
            dev_id_int: int,
            tag_details: list[dict],
            include_angle: int | None = None,
            exclusive_angle: int | None = None
    ):
        ranked_tags_ids = []
        for tag_detail in tag_details:
            ranked_tags_ids.append(tag_detail["tag_id_int"])
        ranked_tags_ids_db = self.conn.values_arr_to_db_in(ranked_tags_ids, int_values=True)

        rows = self.conn.select_all(f"""
        SELECT p.tag_id_int, 
               m.subcategory
          FROM gb_platform.def_tags p,
               gb_app.map_tag_subcategory m
         WHERE p.tag_id_int IN ({ranked_tags_ids_db})
           AND m.id = p.subcategory_int
           AND m.subcategory IN ('Genre', 'Topics', 'Behaviors')
        """)

        audience_angle_ids = []

        if exclusive_angle is not None:
            audience_angle_ids.append(exclusive_angle)
        else:

            # all variations of genre, genre-genre, topic, topic-topic, topic-genre, and behavior-behavior

            genres = []
            topics = []
            behaviors = []
            for row in rows:
                tag_id_int = row["tag_id_int"]
                subcategory = row["subcategory"]
                if subcategory == "Genre":
                    genres.append(tag_id_int)
                elif subcategory == "Topics":
                    topics.append(tag_id_int)
                elif subcategory == "Behaviors":
                    behaviors.append(tag_id_int)

            # 05.03.
            # 5. DECA Games: Kill Shot Bravo: 3D Sniper FPS naslo obrovske audience angle z Behaviors
            # (komba s Destroy), ale pre The Walking Dead No Man's Land naslo len mensie o polovicu a
            # malo behavioralnych, preco? Zapol som Destroy aj pre Walking Dead a naslo tiez .. vypnime
            # behavioralne angle z vyhladavania audiences, lebo to je celkom mess, ukazuje to obrovsky
            # potencial, ale je to vlastne prilis broad lebo tak su tie behaviors myslene (mam uz v
            # taskoch spravit advanced audience angles filter, ale este som sa k tomu nedostal,
            # dolezitejsie su IPcky)
            behaviors = []

            for tag_detail in tag_details:
                tag_rank = tag_detail["tag_rank"]
                tag_id_int = tag_detail["tag_id_int"]
                if tag_rank != GamesDbModel.TAG_RANK_PRIMARY and tag_rank != GamesDbModel.TAG_RANK_SECONDARY:
                    if tag_id_int in genres:
                        genres.remove(tag_id_int)
                    elif tag_id_int in topics:
                        topics.remove(tag_id_int)

            for i in range(len(genres)):
                if genres[i] not in audience_angle_ids:
                    audience_angle_ids.append(genres[i])
                for j in range(i + 1, len(genres)):
                    aa_id = CalcUtils.create_audience_angle_2_comb_id(genres[i], genres[j])
                    if aa_id not in audience_angle_ids:
                        audience_angle_ids.append(aa_id)
            for i in range(len(topics)):
                if topics[i] not in audience_angle_ids:
                   audience_angle_ids.append(topics[i])
                for j in range(i + 1, len(topics)):
                    aa_id = CalcUtils.create_audience_angle_2_comb_id(topics[i], topics[j])
                    if aa_id not in audience_angle_ids:
                        audience_angle_ids.append(aa_id)
                for j in range(len(genres)):
                    aa_id = CalcUtils.create_audience_angle_2_comb_id(topics[i], genres[j])
                    if aa_id not in audience_angle_ids:
                        audience_angle_ids.append(aa_id)
            for i in range(len(behaviors)):
                for j in range(i + 1, len(behaviors)):
                    aa_id = CalcUtils.create_audience_angle_2_comb_id(behaviors[i], behaviors[j])
                    if aa_id not in audience_angle_ids:
                        audience_angle_ids.append(aa_id)

        if include_angle is not None:
            if include_angle not in audience_angle_ids:
                audience_angle_ids.append(include_angle)

        audience_angle_ids_db = self.conn.values_arr_to_db_in(audience_angle_ids, int_values=True)

        bin_bytes_cnt = self.calc_utils.get_bin_bytes_cnt()

        self.calc_utils.recreate_table(
            table_name="tmp_my_tags",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                b BINARY({bin_bytes_cnt}) not null
            )
            SELECT BIT_OR(d.b) as b
              FROM gb_platform.def_tags_bin d
             WHERE d.tag_id_int IN ({ranked_tags_ids_db})
            """
        )

        self.calc_utils.recreate_table(
            table_name="tmp_my_tags_demographics",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                b BINARY({bin_bytes_cnt}) not null
            )
            SELECT BIT_OR(d.b) as b
              FROM gb_platform.def_tags_bin d,
                   gb_app.def_sheet_platform_product p
             WHERE d.tag_id_int IN ({ranked_tags_ids_db})
               AND p.tag_id_int = d.tag_id_int
               AND p.subcategory_int in ({GamesDbModel.SUBCATEGORY_GENRE_ID}, {GamesDbModel.SUBCATEGORY_TOPICS_ID})
            """
        )

        # needs this temporary table twice in some queries
        for i in range(2):
            self.calc_utils.recreate_table(
                table_name=f"tmp_audience_angles{'' if i == 0 else '_2'}",
                query=f"""
                CREATE TABLE gb_calc.x__table_name__x (
                    id BIGINT UNSIGNED NOT NULL,
                    b BINARY({bin_bytes_cnt}) NOT NULL,
                    angle_cnt INT UNSIGNED NOT NULL,
                    arpu DECIMAL(8,4) NOT NULL,
                    loyalty_installs BIGINT UNSIGNED NOT NULL,
                    quality INT UNSIGNED DEFAULT 40 NOT NULL,
                    released_years INT UNSIGNED DEFAULT 0 NOT NULL,
                    CONSTRAINT x__table_name__x_pk
                    PRIMARY KEY (id)
                )
                SELECT id, b, angle_cnt, arpu, loyalty_installs 
                  FROM gb_platform.audience_angle 
                 WHERE id IN ({audience_angle_ids_db})
                """
            )

        row_yearly_revenues = self.conn.select_one_or_none(f"""
        SELECT d.yearly_revenues
          FROM gb_platform.platform_values_devs d
         WHERE d.dev_id_int = {dev_id_int}
        """)
        yearly_revenues = 0
        if row_yearly_revenues is not None:
            yearly_revenues = row_yearly_revenues["yearly_revenues"]

        self.calc_utils.recreate_table(
            table_name="tmp_audience_angles__final",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                age smallint unsigned not null,
                female tinyint unsigned not null,
                female_cnt smallint unsigned not null,
                ltv smallint unsigned not null,
                loved_cnt smallint unsigned not null,
                total_cnt smallint unsigned not null,
                rejected_cnt smallint unsigned not null,
                arpu DECIMAL(8,4) not null,
                exp_d smallint unsigned not null,
                rev_d bigint unsigned not null,
                qua_d smallint unsigned not null, 
                constraint x__table_name__x_pk
                    primary key (id)
            )
            SELECT z4.id, 
                   z4.age,
                   z4.female,
                   z4.female_cnt,
                   z4.ltv as ltv,
                   z4.loved_cnt,
                   z4.total_cnt,
                   0 as rejected_cnt,
                   aa.loyalty_installs, 
                   aa.arpu,
                   0 as exp_d,
                   {yearly_revenues} as rev_d,
                   0 as qua_d
              FROM (
                    SELECT z3.id,
                           MAX(z3.largest_age_group) as age, 
                           MAX(z3.largest_female_group) as female, 
                           MAX(z3.female_cnt) as female_cnt,
                           round(AVG(z3.spending)) AS ltv, 
                           MAX(z3.spending_row_total) AS loved_cnt,
                           MAX(total_rows.cnt) AS total_cnt
                      FROM (
                            SELECT z2.*,
                                   SUM(z2.female) OVER (PARTITION BY z2.id) as female_cnt,
                                   FIRST_VALUE(z2.age) OVER (PARTITION BY z2.id ORDER BY z2.age_count DESC) AS largest_age_group,
                                   FIRST_VALUE(z2.female) OVER (PARTITION BY z2.id ORDER BY z2.female_count DESC) AS largest_female_group
                              FROM (
                                    SELECT z1.id,
                                           si.spending,
                                           si.age,
                                           si.female,
                                           ROW_NUMBER() OVER (PARTITION BY z1.id ORDER BY si.spending) AS spending_row_num,
                                           COUNT(1) OVER (PARTITION BY z1.id) AS spending_row_total,
                                           COUNT(1) OVER (PARTITION BY z1.id, si.age) AS age_count,
                                           COUNT(1) OVER (PARTITION BY z1.id, si.female) AS female_count
                                      FROM (
                                            SELECT DISTINCT 
                                                   aa.id,
                                                   stb.survey_instance_int
                                              FROM gb_calc.tmp_audience_angles aa,
                                                   gb_platform.platform_values_survey_tags_bin stb,
                                                   gb_calc.tmp_my_tags_demographics my_tags
                                             WHERE stb.survey_id = {survey_id}
                                               AND BIT_COUNT(stb.b_hated & my_tags.b) = 0
                                               AND BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
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
                     GROUP BY z3.id
                   ) z4,
                   gb_calc.tmp_audience_angles_2 aa
                   where aa.id = z4.id
            """
        )

        # update rejected

        self.conn.query(f"""
        UPDATE gb_calc.tmp_audience_angles__final f
        INNER JOIN (
        SELECT aa.id, count(1) as rejected_cnt
          FROM gb_calc.tmp_audience_angles aa,
               gb_platform.platform_values_survey_tags_bin stb,
               gb_calc.tmp_my_tags my_tags
         WHERE BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
           AND BIT_COUNT(stb.b_hated & my_tags.b) > 0
           AND stb.survey_id = {survey_id}
         group by aa.id
         ) z1
         ON z1.id = f.id
         SET f.rejected_cnt = z1.rejected_cnt
        """)
        self.conn.commit()

        # discount per node

        self.calc_utils.recreate_table(
            table_name="tmp_discount_per_angle",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                quality int unsigned default 40 not null,
                released_years int unsigned default 0 not null,
                constraint x__table_name__x_pk
                primary key (id)
            )
            select id 
              FROM gb_calc.tmp_audience_angles
            """
        )

        self.conn.query(f"""
        UPDATE gb_calc.tmp_discount_per_angle aa
        INNER JOIN (
                    SELECT z3.id, 
                           ROUND(AVG(z3.quality)) AS quality
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
                                               AND a.tier >= 3
                                           ) apps,
                                           gb_platform.platform_values_tags_bin tb,
                                           gb_calc.tmp_audience_angles aa
                                     WHERE tb.app_id_int = apps.app_id_int
                                   ) z2
                           ) z3
                     WHERE z3.row_num IN (FLOOR((z3.total_num + 1) / 2), FLOOR((z3.total_num + 2) / 2) ) 
                     GROUP BY z3.id 
                    ) z4 
         ON z4.id = aa.id
        SET aa.quality = z4.quality
        """)
        self.conn.commit()

        self.conn.query(f"""
        UPDATE gb_calc.tmp_discount_per_angle aa
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
                                               AND a.tier >= 3
                                           ) apps,
                                           gb_platform.platform_values_tags_bin tb,
                                           gb_calc.tmp_audience_angles aa
                                     WHERE tb.app_id_int = apps.app_id_int
                                       AND BIT_COUNT(tb.b & aa.b) = aa.angle_cnt
                                   ) z2
                           ) z3
                     GROUP BY z3.id
                   ) z4
         ON z4.id = aa.id
        SET aa.released_years = z4.released_years
        """)
        self.conn.commit()

        self.conn.query("""
        UPDATE gb_calc.tmp_audience_angles__final aa
         INNER JOIN gb_calc.tmp_discount_per_angle d ON d.id = aa.id
           SET aa.qua_d = d.quality, 
               aa.exp_d = d.released_years
        """)
        self.conn.commit()

        # top behaviors

        self.calc_utils.recreate_table(
            table_name="tmp_audience_angles__top_tags",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                id bigint unsigned not null,
                tag_id_int smallint unsigned not null,
                tag_order tinyint unsigned not null,
                constraint x__table_name__x_pk
                    primary key (id, tag_id_int)
            )
            SELECT z3.id, 
                   z3.tag_id_int, 
                   z3.row_num as tag_order
              FROM (
                    SELECT z2.id, 
                           z2.tag_id_int,
                           ROW_NUMBER() OVER (PARTITION BY z2.id, z2.subcategory_int ORDER BY cnt DESC) AS row_num
                      FROM (
                            SELECT z1.id, 
                                   p.tag_id_int,
                                   p.subcategory_int,
                                   COUNT(1) AS cnt
                              FROM (
                                    SELECT DISTINCT 
                                           aa.id, 
                                           stb.survey_instance_int
                                      FROM gb_calc.tmp_audience_angles aa,
                                           gb_platform.platform_values_survey_tags_bin stb,
                                           gb_calc.tmp_my_tags my_tags
                                     WHERE stb.survey_id = {survey_id}
                                       AND BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
                                       AND BIT_COUNT(stb.b_hated & my_tags.b) = 0
                                   ) z1,
                                   gb_platform.platform_values_survey_tags st,
                                   gb_app.def_sheet_platform_product p,
                                   gb_app.map_tag_subcategory m
                             WHERE p.subcategory_int IN ({GamesDbModel.SUBCATEGORY_GENRE_ID}, {GamesDbModel.SUBCATEGORY_TOPICS_ID}, {GamesDbModel.SUBCATEGORY_BEHAVIORS_ID}, {GamesDbModel.SUBCATEGORY_NEEDS_ID}, {GamesDbModel.SUBCATEGORY_DOMAINS_ID})
                               AND st.survey_meta_id = {survey_id}
                               AND st.survey_instance_int = z1.survey_instance_int
                               AND st.tag_id_int = p.tag_id_int
                               AND st.loved = 1
                             GROUP BY 
                                   z1.id, 
                                   p.tag_id_int,
                                   p.subcategory_int
                           ) z2
                   ) z3
             WHERE z3.row_num <= 3
            """
        )

        self.__copy_data_to_results(platform_id=platform_id)

    def __copy_data_to_results(
            self,
            platform_id: int
    ):

        rows_final = self.conn.select_all("""
        SELECT f.id, 
               f.loved_cnt, 
               f.total_cnt, 
               f.rejected_cnt, 
               f.loyalty_installs
          FROM gb_calc.tmp_audience_angles__final f
        """)

        arr = []
        potential_downloads = self.calc_utils.get_potential_downloads()
        for row in rows_final:
            audience = AudienceStats(
                loved_survey_cnt=row["loved_cnt"],
                total_survey_cnt=row["total_cnt"],
                rejected_survey_cnt=row["rejected_cnt"],
                potential_downloads=potential_downloads,
                loyalty_installs=row["loyalty_installs"]
            )
            arr.append({
                "sort_val": audience.total_audience,
                "id": row["id"]
            })

        rows_unlocked_angles = self.conn.select_all("""
        SELECT f.id
          FROM gb_calc.tmp_audience_angles__final f
         WHERE NOT EXISTS (
                 SELECT 1
                   FROM gb_platform.audience_angle_tags aat,
                        gb_app.def_sheet_platform_product p
                  WHERE aat.id = f.id
                    AND p.tag_id_int = aat.tag_id_int
                    AND p.unlocked = 0
                )
        """)

        unlocked_angles = []
        for row in rows_unlocked_angles:
            unlocked_angles.append(row["id"])

        arr.sort(key=lambda x: x["sort_val"], reverse=True)

        unlocked_angles_final = []
        for it in arr:
            if it["id"] in unlocked_angles:
                if len(unlocked_angles_final) < 3:
                    unlocked_angles_final.append(it["id"])

        # if len(unlocked_angles_final) > 0:
        #     self.conn.query(f"""
        #     UPDATE gb_calc.tmp_audience_angles__final f
        #        SET f.locked_in_demo = 1
        #      WHERE f.id not in ({self.conn.values_arr_to_db_in(unlocked_angles_final, int_values=True)})
        #     """)

        self.conn.query("""
        DELETE FROM gb_calc.results_audience_angles__final
         WHERE platform_id = %s
        """, [platform_id])

        self.conn.query("""
        INSERT INTO gb_calc.results_audience_angles__final
        (platform_id, audience_angle_id, age, female, female_cnt, ltv, 
        loved_cnt, total_cnt, rejected_cnt, arpu, 
        loyalty_installs, exp_d, rev_d, qua_d)
        SELECT %s as platform_id, f.id as audience_angle_id, f.age, f.female, f.female_cnt, f.ltv,
        f.loved_cnt, f.total_cnt, f.rejected_cnt, f.arpu,
        f.loyalty_installs, f.exp_d, f.rev_d, f.qua_d
        FROM gb_calc.tmp_audience_angles__final f
        """, [platform_id])

        self.conn.query("""
        DELETE FROM gb_calc.results_audience_angles__top_tags WHERE platform_id = %s
        """, [platform_id])
        self.conn.query("""
        INSERT INTO gb_calc.results_audience_angles__top_tags
        (platform_id, audience_angle_id, tag_id_int, tag_order)
        SELECT %s as platform_id, f.id as audience_angle_id, f.tag_id_int, f.tag_order
        FROM gb_calc.tmp_audience_angles__top_tags f
        """, [platform_id])

        self.conn.commit()

    def generate_client_data(
            self,
            platform_id: int,
            exclusive_angle_id: int | None = None
    ) -> list[dict]:

        rows_final = self.conn.select_all("""
        SELECT f.audience_angle_id, f.loved_cnt, f.total_cnt, f.rejected_cnt, f.age, f.female, f.female_cnt, f.ltv, f.arpu, 
               f.exp_d, f.rev_d, f.qua_d, f.loyalty_installs, 0 as locked_in_demo
          FROM gb_calc.results_audience_angles__final f
         where f.platform_id = %s
        """, [platform_id])

        rows_tags = self.conn.select_all("""
        SELECT af.audience_angle_id, d.tag_id_int
          FROM gb_platform.audience_angle aa,
               gb_calc.results_audience_angles__final af,
               gb_platform.def_tags_bin d
          where aa.id = af.audience_angle_id
            and af.platform_id = %s
            AND BIT_COUNT(d.b & aa.b) = 1
        """, [platform_id])
        map_audience_tags = {}
        for row in rows_tags:
            if row["audience_angle_id"] not in map_audience_tags:
                map_audience_tags[row["audience_angle_id"]] = []
            map_audience_tags[row["audience_angle_id"]].append(row[UserObfuscator.TAG_ID_INT])

        rows_top_tags = self.conn.select_all("""
        SELECT f.audience_angle_id, f.tag_id_int, f.tag_order, p.subcategory_int
          FROM gb_calc.results_audience_angles__top_tags f,
               gb_app.def_sheet_platform_product p
          where f.platform_id = %s
          AND p.tag_id_int = f.tag_id_int
          order by f.tag_order
        """, [platform_id])

        loved_ratio_fix = self.calc_utils.get_loved_ratio_fix()

        data_per_angle = {}

        for row in rows_final:

            audience_angle_id = row["audience_angle_id"]

            if exclusive_angle_id is not None:
                if exclusive_angle_id != audience_angle_id:
                    continue

            loved_cnt = row["loved_cnt"]

            if loved_cnt == 0:
                continue

            for tag_id_int in map_audience_tags[audience_angle_id]:
                loved_ratios = []
                if tag_id_int in loved_ratio_fix:
                    loved_ratios.append(loved_ratio_fix[tag_id_int])
                loved_cnt = int(loved_ratios[0] * row["total_cnt"])

            data_per_angle[audience_angle_id] = self.calc_utils.audience_to_client_data_2(
                data=row,
                platform_id=platform_id,
                loved_cnt=loved_cnt
            )

        for row in rows_tags:
            audience_angle_id = row["audience_angle_id"]
            locked = Utils.try_get_from_dict(data_per_angle, audience_angle_id, "locked")
            if audience_angle_id in data_per_angle and (locked is None or not locked):
                data_per_angle[audience_angle_id][UserObfuscator.TAG_IDS_INT].append(
                    row[UserObfuscator.TAG_ID_INT]
                )

        top_tags_map = {
            "top_behaviors": GamesDbModel.SUBCATEGORY_BEHAVIORS_ID,
            "top_needs": GamesDbModel.SUBCATEGORY_NEEDS_ID,
            "top_domains": GamesDbModel.SUBCATEGORY_DOMAINS_ID,
            "top_genres": GamesDbModel.SUBCATEGORY_GENRE_ID,
            "top_topics": GamesDbModel.SUBCATEGORY_TOPICS_ID,
        }

        for row in rows_top_tags:
            audience_angle_id = row["audience_angle_id"]
            locked = Utils.try_get_from_dict(data_per_angle, audience_angle_id, "locked")
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
        arr.sort(key=lambda x: x["audience_stats"]["total_audience"], reverse=True)
        arr = arr[:20]

        return arr
