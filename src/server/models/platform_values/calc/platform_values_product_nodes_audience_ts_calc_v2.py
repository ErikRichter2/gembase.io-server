from gembase_server_core.db.db_connection import DbConnection
from src.server.models.platform_values.calc.platform_values_audience_angle_calc import PlatformValuesAudienceAngleCalc
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.tags.tags_constants import TagsConstants
from src.server.models.tags.tags_mapper import TagsMapper
from src.server.models.user.user_obfuscator import UserObfuscator
from src.server.models.platform_values.calc.platform_values_competitors_calc import PlatformValuesCompetitorsCalc


class PlatformValuesProductNodesAudienceTsCalcV2:

    @staticmethod
    def calc(
            conn: DbConnection,
            platform_id: int,
            survey_id: int,
            dev_id_int: int,
            my_tier: int | None,
            my_growth: int | None,
            selected_tags_details: [],
            exclude_apps_from_competitors: [],
            update_progress_data=None
    ):
        if update_progress_data is not None:
            update_progress_data({
                "state": "aud"
            })

        map_tags = TagsMapper(conn=conn).map_tags

        # find angles for each node

        tags_ids = [it["tag_id_int"] for it in selected_tags_details]

        affinities = []
        for it in map_tags["def"]:
            tag_id_int = it["tag_id_int"]

            if it["is_prompt"] != 1 or it["is_survey"] != 1:
                continue

            tag_details = selected_tags_details.copy()

            if tag_id_int not in tags_ids:
                tag_details.append({
                    UserObfuscator.TAG_ID_INT: tag_id_int,
                    "tag_rank": 0
                })

            affinities.append({
                "multi_id": tag_id_int,
                "affinity": tag_id_int,
                "exclusive_angle": tag_id_int,
                "tag_details": tag_details
            })

        bulk = 20
        affinities_bulk = []
        arr = []
        for i in range(len(affinities)):
            arr.append(affinities[i])
            if len(arr) == bulk or i == len(affinities) - 1:
                affinities_bulk.append(arr)
                arr = []

        PlatformValuesAudienceAngleCalc.clear_results(
            conn=conn,
            platform_id=platform_id
        )

        PlatformValuesCompetitorsCalc.clear_results(
            conn=conn,
            platform_id=platform_id
        )

        index = 0
        for it in affinities_bulk:
            print(f"Angles {index}")
            index += 1

            PlatformValuesAudienceAngleCalc.calc(
                conn=conn,
                platform_id=platform_id,
                survey_id=survey_id,
                dev_id_int=dev_id_int,
                tag_details=[],
                skip_top_behaviors=True,
                tag_details_multi=it
            )

            conn.commit()

        conn.query("""
                INSERT INTO platform_values.results_affinities 
                (platform_id, affinity_tag_id_int, audience_angle_row_id)
                SELECT f.platform_id, f.multi_id as affinity_tag_id_int, f.row_id as audience_angle_row_id
                  FROM platform_values.results_audience_angles__final f
                 WHERE f.platform_id = %s
                """, [platform_id])
        conn.commit()

        row_id_per_angle_id = {}
        rows_final = conn.select_all("""
                SELECT f.audience_angle_row_id, f.affinity_tag_id_int
                FROM platform_values.results_affinities f
                where f.platform_id = %s
                """, [platform_id])
        for row in rows_final:
            row_id_per_angle_id[row["affinity_tag_id_int"]] = row["audience_angle_row_id"]

        bulk = 5
        affinities_bulk = []
        arr = []
        for i in range(len(affinities)):
            if affinities[i]["multi_id"] not in row_id_per_angle_id:
                continue

            arr.append({
                "audience_angle_row_id": row_id_per_angle_id[affinities[i]["multi_id"]],
                "multi_id": affinities[i]["multi_id"],
                "tag_details": affinities[i]["tag_details"]
            })
            if len(arr) == bulk or i == len(affinities) - 1:
                affinities_bulk.append(arr)
                arr = []

        index = 0
        for it in affinities_bulk:
            print(f"Competitors {index}")
            index += 1
            if update_progress_data is not None:
                update_progress_data({
                    "state": "ts",
                    "p": index,
                    "t": len(affinities_bulk),
                    "l": 1
                })

            PlatformValuesCompetitorsCalc.find_competitors_for_audience_angle(
                conn=conn,
                platform_id=platform_id,
                survey_id=survey_id,
                dev_id_int=dev_id_int,
                my_growth=my_growth,
                my_tier=my_tier,
                my_tags_details=[],
                audience_angle_row_id=-1,
                exclude_apps_from_competitors=exclude_apps_from_competitors,
                multi_tags=it,
                skip_results_copy=True
            )

            rows_ts = conn.select_all("""
            SELECT tf.multi_id, platform.calc_ts_final(tf.threat_score, af.discount) as ts
              FROM platform_values.tmp_ts_final tf,
                   platform_values.results_audience_angles__final af
             WHERE tf.audience_angle_row_id = af.row_id
            """)

            arr_per_multi = {}
            for b in it:
                arr_per_multi[b["multi_id"]] = []

            for row in rows_ts:
                arr_per_multi[row["multi_id"]].append(row["ts"])

            bulk_data = []
            for multi_id in arr_per_multi:
                ts_final = PlatformValuesHelper.calc_ts(arr_per_multi[multi_id])
                bulk_data.append((ts_final, len(arr_per_multi[multi_id]), platform_id, multi_id))

            conn.bulk("""
                        UPDATE platform_values.results_affinities f
                        SET f.ts = %s, f.competitors_cnt = %s
                        WHERE f.platform_id = %s
                        AND f.affinity_tag_id_int = %s
                        """, bulk_data)
            conn.commit()

    @staticmethod
    def generate_client_data(
            conn: DbConnection,
            platform_id: int,
            is_admin=False
    ) -> []:

        rows_audiences = conn.select_all("""
        SELECT a.*, aa.installs
          FROM platform_values.results_audience_angles__final a,
          platform.audience_angle aa
         WHERE a.platform_id = %s
         AND a.audience_angle_id = aa.id
        """, [platform_id])

        rows_ts = conn.select_all("""
        SELECT a.*
          FROM platform_values.results_affinities a
         WHERE a.platform_id = %s
        """, [platform_id])

        rows_app_platforms = conn.select_all("""
        SELECT a.*
          FROM platform_values.results_audience_angle__input_tags a,
               platform.def_tags d
         WHERE a.platform_id = %s
           AND d.tag_id_int = a.tag_id_int
           AND d.subcategory_int = %s
        """, [platform_id, TagsConstants.SUBCATEGORY_PLATFORMS_ID])

        data_per_affinity = {}
        for row in rows_audiences:
            multi_id = row["multi_id"]
            data_per_affinity[multi_id] = {
                "aud": row,
                "ts": None,
                "competitors_cnt": None,
                "app_platforms": []
            }

        for row in rows_ts:
            if row["affinity_tag_id_int"] in data_per_affinity:
                data_per_affinity[row["affinity_tag_id_int"]]["ts"] = row["ts"]
                data_per_affinity[row["affinity_tag_id_int"]]["competitors_cnt"] = row["competitors_cnt"]

        for row in rows_app_platforms:
            if row["multi_id"] in data_per_affinity:
                data_per_affinity[row["multi_id"]]["app_platforms"].append(row["tag_id_int"])

        res = []
        for affinity_tag_id_int in data_per_affinity:
            audience_detail = PlatformValuesHelper(conn=conn).audience_to_client_data_2(
                data=data_per_affinity[affinity_tag_id_int]["aud"],
                platform_id=platform_id,
                app_platforms=data_per_affinity[affinity_tag_id_int]["app_platforms"],
                is_admin=is_admin
            )

            item = {
                "affinity": {
                    UserObfuscator.TAG_ID_INT: affinity_tag_id_int,
                },
                UserObfuscator.AUDIENCE_ANGLE_ID_INT: audience_detail[UserObfuscator.AUDIENCE_ANGLE_ID_INT],
                "tam": audience_detail["tam"],
                "audience_stats": audience_detail["audience_stats"],
            }
            res.append(item)

            if data_per_affinity[affinity_tag_id_int]["ts"] is not None:
                item["ts"] = data_per_affinity[affinity_tag_id_int]["ts"]
            if data_per_affinity[affinity_tag_id_int]["competitors_cnt"] is not None:
                item["cnt"] = data_per_affinity[affinity_tag_id_int]["competitors_cnt"]

        return {
            "platform_id": platform_id,
            "data": res
        }
