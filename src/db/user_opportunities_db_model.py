import json
from typing import Optional

from server_core.db.db_connection import DbConnection
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.calc_db_model import CalcDbModel
from src.models.user_obfuscator import UserObfuscator


class UserOpportunitiesDbModel(BaseUserDbModel):

    def __init__(self, conn: DbConnection, user_id: int):
        super(UserOpportunitiesDbModel, self).__init__(conn=conn, user_id=user_id)

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

        calc_db = CalcDbModel(conn=self.conn)

        items = []
        for row in rows_audiences:

            if opportunity_guid is not None and row["guid"] != opportunity_guid:
                continue

            d = {
                "audience_detail": calc_db.audience_to_client_data_2(
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
