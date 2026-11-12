from server_core.db.db_connection import DbConnection
from src.db.base.base_db_model import BaseDbModel
from src.db.calc.calc_audience_angles_db_model import CalcAudienceAnglesDbModel
from src.db.calc.calc_competitors_db_model import CalcCompetitorsDbModel
from src.db.calc.calc_utils import CalcUtils
from src.db.calc.helpers.audience_stats import AudienceStats


class CalcProductNodesAudienceTsDbModel(BaseDbModel):

    def __init__(self, conn: DbConnection):
        super(CalcProductNodesAudienceTsDbModel, self).__init__(conn=conn)
        self.calc_utils = CalcUtils(conn=conn)

    def calc(
            self,
            platform_id: int,
            survey_id: int,
            dev_id_int: int,
            my_tier: int | None,
            my_growth: int | None,
            my_store: int | None,
            selected_tags_details: list[dict],
            exclude_apps_from_competitors: list[int],
            update_progress_data=None
    ):
        bin_bytes_cnt = self.calc_utils.get_bin_bytes_cnt()

        if update_progress_data is not None:
            update_progress_data({
                "state": "aud"
            })

        selected_tags_ids = []
        for it in selected_tags_details:
            selected_tags_ids.append(it["tag_id_int"])

        selected_tags_ids_db = self.conn.values_arr_to_db_in(selected_tags_ids, int_values=True)

        self.conn.query(f"""
            DELETE FROM gb_calc.results__rejected_per_audience_angle r
            WHERE r.platform_id = {platform_id}
            """)
        self.conn.commit()

        if len(selected_tags_ids) > 0:

            self.calc_utils.recreate_table(
                table_name="tmp_selected_tags_bin",
                query=f"""
                        CREATE TABLE gb_calc.x__table_name__x (
                            b BINARY({bin_bytes_cnt}) not null
                        )
                        SELECT BIT_OR(d.b) as b
                          FROM gb_platform.def_tags_bin d
                         WHERE d.tag_id_int in ({selected_tags_ids_db})
                        """
            )

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
                        SELECT aa.id, 
                               count(1) as rejected_cnt
                          FROM gb_platform.audience_angle aa,
                               gb_platform.platform_values_survey_tags_bin stb,
                               gb_calc.tmp_selected_tags_bin my_tags
                         WHERE bit_count(stb.b_loved & aa.b) = aa.angle_cnt
                           AND bit_count(stb.b_hated & my_tags.b) > 0
                           AND stb.survey_id = {survey_id}
                           AND aa.angle_cnt = 1
                         GROUP BY aa.id
                        """
            )

            self.conn.query(f"""
                    INSERT INTO gb_calc.results__rejected_per_audience_angle
                    (platform_id, audience_angle_id, rejected_cnt)
                    SELECT {platform_id}, r.id, r.rejected_cnt
                      FROM gb_calc.tmp_rejected_per_angle r
                    """)
            self.conn.commit()

        # competitors per node

        self.conn.query("""
                        delete from gb_calc.results__ts_per_audience_angle
                        where platform_id = %s
                        """, [platform_id])

        self.conn.commit()

        rows_angles = self.conn.select_all("""
        SELECT aa.id as id, aa.b as b
          FROM gb_platform.audience_angle aa
         WHERE aa.angle_cnt = 1
        """)

        page = 0
        total = len(rows_angles)
        limit = 1

        for row_angle in rows_angles:

            audience_angle_id = row_angle["id"]
            
            if len(selected_tags_details) > 0:
                CalcAudienceAnglesDbModel(conn=self.conn).calc(
                    platform_id=platform_id,
                    survey_id=survey_id,
                    dev_id_int=dev_id_int,
                    tag_details=selected_tags_details,
                    exclusive_angle=audience_angle_id
                )

            competitors_count = CalcCompetitorsDbModel(conn=self.conn).find_competitors_for_audience_angle(
                platform_id=platform_id,
                survey_id=survey_id,
                dev_id_int=dev_id_int,
                my_tier=my_tier,
                my_growth=my_growth,
                my_store=my_store,
                my_tags_details=selected_tags_details,
                audience_angle_id=audience_angle_id,
                audience_angle_platform_id=platform_id,
                exclude_apps_from_competitors=exclude_apps_from_competitors,
                advanced_filter=None,
                skip_results_copy=True
            )

            # results
            if competitors_count == 0:
                self.conn.query(f"""
                insert into gb_calc.results__ts_per_audience_angle 
                    (platform_id, audience_angle_id, ts, no_data, competitors_count)
                values ({platform_id}, {audience_angle_id}, 0, 1, 0)
                """)
            else:
                rows_ts = self.conn.select_all("""
                SELECT greatest(0, cast(tf.threat_score as signed) - cast(tf.threat_score_discount as signed)) as ts
                  FROM gb_calc.tmp_ts_final tf
                """)

                ts_arr = []
                for row in rows_ts:
                    ts_arr.append(int(row["ts"]))

                ts_final = self.calc_utils.calc_ts(ts_arr=ts_arr)

                self.conn.query(f"""
                    insert into gb_calc.results__ts_per_audience_angle 
                    (platform_id, audience_angle_id, ts, no_data, competitors_count)
                    VALUES ({platform_id}, {audience_angle_id}, {ts_final}, 0, {competitors_count})
                    """)
                self.conn.commit()

            if update_progress_data is not None:
                update_progress_data({
                    "state": "ts",
                    "p": page,
                    "t": total,
                    "l": limit
                })

            page += 1

    def generate_client_data(
            self,
            platform_id: int,
            survey_id: int
    ) -> dict:

        query = f"""
                SELECT aa.id as tag_id_int, aap.loved_cnt, aap.total_cnt, 
                IF(r.rejected_cnt is null, 0, r.rejected_cnt) as rejected_cnt, 
                ts.ts, aa.loyalty_installs, p.unlocked, ts.no_data, ts.competitors_count
                 FROM gb_platform.audience_angle aa
                 INNER JOIN gb_app.def_sheet_platform_product p
                 ON p.tag_id_int = aa.id
                 INNER JOIN gb_platform.audience_angle_potential aap
                 ON aap.id = aa.id
                 AND aap.survey_id = {survey_id}
                 LEFT JOIN gb_calc.results__rejected_per_audience_angle r
                 ON r.platform_id = {platform_id}
                 AND r.audience_angle_id = aap.id
                 LEFT JOIN gb_calc.results__ts_per_audience_angle ts
                 ON ts.platform_id = {platform_id}
                 AND ts.audience_angle_id = aap.id
                 WHERE aa.angle_cnt = 1
                """

        rows = self.conn.select_all(query)
        potential_downloads = self.calc_utils.get_potential_downloads()

        items = []
        loved_ratio_fix = self.calc_utils.get_loved_ratio_fix()

        for row in rows:
            tag_id_int = row["tag_id_int"]
            total_cnt = row["total_cnt"]
            loved_cnt = row["loved_cnt"]

            if tag_id_int in loved_ratio_fix:
                loved_cnt = int(loved_ratio_fix[tag_id_int] * total_cnt)

            audience_stats = AudienceStats(
                loved_survey_cnt=loved_cnt,
                total_survey_cnt=total_cnt,
                rejected_survey_cnt=row["rejected_cnt"],
                potential_downloads=potential_downloads,
                loyalty_installs=row["loyalty_installs"],
                no_data=True if row["no_data"] == 1 else False
            )

            d = {
                "affinity": {"tag_id_int": row["tag_id_int"]},
                "audience_stats": audience_stats.generate_client_data(),
            }


            items.append(d)

        return {
            "platform_id": platform_id,
            "items": items
        }
