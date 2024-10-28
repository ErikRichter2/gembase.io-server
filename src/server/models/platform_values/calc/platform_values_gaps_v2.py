from gembase_server_core.db.db_connection import DbConnection
from src.server.models.platform_values.calc.platform_values_audience_angle_calc import PlatformValuesAudienceAngleCalc
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.tags.tags_constants import TagsConstants
from src.server.models.tags.tags_mapper import TagsMapper
from src.server.models.user.user_obfuscator import UserObfuscator
from src.server.models.platform_values.calc.platform_values_competitors_calc import PlatformValuesCompetitorsCalc
from src.utils.gembase_utils import GembaseUtils


class PlatformValuesGapsV2Calc:

    @staticmethod
    def calc(
            conn: DbConnection,
            platform_id: int,
            survey_id: int,
            dev_id_int: int,
            selected_tags_details: [],
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

        map_tags = TagsMapper(conn=conn).map_tags

        # find best angle for affinity

        tags_ids = [it["tag_id_int"] for it in selected_tags_details]

        affinities = []
        for it in map_tags["def"]:
            tag_id_int = it["tag_id_int"]

            if tag_id_int in tags_ids:
                continue

            if it["subcategory_int"] == TagsConstants.SUBCATEGORY_PLATFORMS_ID:
                continue
            if it["subcategory_int"] == TagsConstants.SUBCATEGORY_COMPLEXITY_ID:
                continue

            if it["is_prompt"] != 1 or it["is_survey"] != 1:
                continue

            affinities.append({
                "multi_id": tag_id_int,
                "affinity": tag_id_int,
                "tag_details": selected_tags_details + [{
                    UserObfuscator.TAG_ID_INT: tag_id_int,
                    "tag_rank": 0
                }]
            })

        bulk = 20
        affinities_bulk = []
        arr = []
        for i in range(len(affinities)):
            arr.append(affinities[i])
            if len(arr) == bulk or i == len(affinities) - 1:
                affinities_bulk.append(arr)
                arr = []

        best_audiences_per_affinity = {}

        PlatformValuesAudienceAngleCalc.clear_results(
            conn=conn,
            platform_id=platform_id
        )

        PlatformValuesCompetitorsCalc.clear_results(
            conn=conn,
            platform_id=platform_id
        )

        for it in affinities_bulk:

            t = GembaseUtils.timestamp()

            PlatformValuesAudienceAngleCalc.calc(
                conn=conn,
                platform_id=platform_id,
                survey_id=survey_id,
                dev_id_int=dev_id_int,
                tag_details=[],
                skip_copy_to_results=True,
                skip_top_behaviors=True,
                tag_details_multi=it
            )

            rows_final = conn.select_all("""
            SELECT DISTINCT f.multi_id,
                   FIRST_VALUE(f.id) over (PARTITION BY f.multi_id ORDER BY f.total_audience DESC) as id
              FROM platform_values.tmp_audience_angles__final f
            """)

            bulk_data = []
            for row in rows_final:
                bulk_data.append((row["multi_id"], row["id"]))

            conn.bulk("""
            DELETE FROM platform_values.tmp_audience_angles__final
            WHERE multi_id = %s
            AND id != %s
            """, bulk_data)
            conn.commit()

            PlatformValuesAudienceAngleCalc.copy_data_to_results(
                conn=conn,
                platform_id=platform_id
            )
            conn.commit()

            print(f"{GembaseUtils.timestamp() - t}s")

        conn.query("""
        INSERT INTO platform_values.results_affinities 
        (platform_id, affinity_tag_id_int, audience_angle_row_id, uuid)
        SELECT f.platform_id, f.multi_id as affinity_tag_id_int, f.row_id as audience_angle_row_id, uuid() as uuid
          FROM platform_values.results_audience_angles__final f
         WHERE f.platform_id = %s
        """, [platform_id])
        conn.commit()

        rows_final = conn.select_all("""
        SELECT f.audience_angle_row_id, f.affinity_tag_id_int
        FROM platform_values.results_affinities f
        where f.platform_id = %s
        """, [platform_id])
        for row in rows_final:
            best_audiences_per_affinity[row["affinity_tag_id_int"]] = row["audience_angle_row_id"]

        bulk = 5
        affinities_bulk = []
        arr = []
        for i in range(len(affinities)):
            if affinities[i]["multi_id"] not in best_audiences_per_affinity:
                continue

            arr.append({
                "audience_angle_row_id": best_audiences_per_affinity[affinities[i]["multi_id"]],
                "multi_id": affinities[i]["multi_id"],
                "tag_details": affinities[i]["tag_details"]
            })
            if len(arr) == bulk or i == len(affinities) - 1:
                affinities_bulk.append(arr)
                arr = []

        index = 0
        for it in affinities_bulk:

            index += 1
            progress_data["state"] = "gaps"
            progress_data["p"] = index
            progress_data["t"] = len(affinities_bulk)
            update_progress()

            PlatformValuesCompetitorsCalc.find_competitors_for_audience_angle(
                conn=conn,
                platform_id=platform_id,
                survey_id=survey_id,
                dev_id_int=dev_id_int,
                my_growth=None,
                my_tier=None,
                my_tags_details=[],
                audience_angle_row_id=-1,
                exclude_apps_from_competitors=[],
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
    def generate_client_data_for_single_gap(
            conn: DbConnection,
            uuid: str
    ) -> {}:
        row = conn.select_one_or_none("""
                SELECT aff.platform_id, aff.audience_angle_row_id
                  FROM platform_values.results_affinities aff
                 WHERE aff.uuid = %s
                """, [uuid])

        if row is None:
            return None

        res = PlatformValuesGapsV2Calc.generate_client_data(
            conn=conn,
            platform_id=row["platform_id"]
        )

        for it in res["data"]:
            if it["uuid"] == uuid:
                return it

        return None

    @staticmethod
    def generate_client_data(
            conn: DbConnection,
            platform_id: int,
            is_admin=False,
            is_locked=False
    ) -> {}:

        rows_audiences = conn.select_all("""
                SELECT a.*, aa.installs
                  FROM platform_values.results_audience_angles__final a,
                  platform.audience_angle aa
                 WHERE a.platform_id = %s
                 AND aa.id = a.audience_angle_id
                 ORDER BY a.total_audience DESC
                """, [platform_id])

        rows_ts = conn.select_all("""
                SELECT a.affinity_tag_id_int, a.ts, a.uuid, a.competitors_cnt
                  FROM platform_values.results_affinities a
                 WHERE a.platform_id = %s
                """, [platform_id])

        rows_input_tags = conn.select_all("""
                SELECT a.multi_id, a.tag_id_int, a.tag_rank, IF(d.subcategory_int = %s, 1, 0) as is_platform
                  FROM platform_values.results_audience_angle__input_tags a,
                       platform.def_tags d
                 WHERE a.platform_id = %s
                   AND d.tag_id_int = a.tag_id_int
                """, [TagsConstants.SUBCATEGORY_PLATFORMS_ID, platform_id])

        data_per_affinity = {}
        for i in range(len(rows_audiences)):
            row = rows_audiences[i]
            multi_id = row["multi_id"]
            data_per_affinity[multi_id] = {
                "aud": row,
                "ts": None,
                "competitors_cnt": None,
                "app_platforms": [],
                "input_tags": []
            }

            if i > 0 and is_locked:
                data_per_affinity[multi_id]["locked"] = True

        for row in rows_ts:
            affinity_tag_id_int = row["affinity_tag_id_int"]
            data_per_affinity[affinity_tag_id_int]["ts"] = row["ts"]
            data_per_affinity[affinity_tag_id_int]["competitors_cnt"] = row["competitors_cnt"]
            data_per_affinity[affinity_tag_id_int]["uuid"] = row["uuid"]

        for row in rows_input_tags:
            if row["is_platform"]:
                data_per_affinity[row["multi_id"]]["app_platforms"].append(row["tag_id_int"])
            data_per_affinity[row["multi_id"]]["input_tags"].append({
                UserObfuscator.TAG_ID_INT: row[UserObfuscator.TAG_ID_INT],
                "tag_rank": row["tag_rank"]
            })

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
                "uuid": data_per_affinity[affinity_tag_id_int]["uuid"],
                "input_tags": data_per_affinity[affinity_tag_id_int]["input_tags"]
            }
            res.append(item)

            if data_per_affinity[affinity_tag_id_int]["ts"] is not None:
                item["ts"] = data_per_affinity[affinity_tag_id_int]["ts"]
            if data_per_affinity[affinity_tag_id_int]["competitors_cnt"] is not None:
                item["cnt"] = data_per_affinity[affinity_tag_id_int]["competitors_cnt"]

            if "locked" in data_per_affinity[affinity_tag_id_int] and data_per_affinity[affinity_tag_id_int]["locked"]:
                item["locked"] = True

        return {
            "platform_id": platform_id,
            "data": res
        }
