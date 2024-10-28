from gembase_server_core.db.db_connection import DbConnection


class SurveyDataModel:

    @staticmethod
    def get_survey_meta_id(
            conn: DbConnection,
            survey_control_guid: str
    ) -> int | None:
        row = conn.select_one_or_none("""
        SELECT m.id
          FROM survey_data.survey_meta m
         WHERE m.survey_control_guid = %s
        """, [survey_control_guid])

        if row is None:
            return None

        return row["id"]

    @staticmethod
    def postprocess(conn: DbConnection, survey_control_guid: str):
        SurveyDataModel.calc_tam_for_survey_tags(conn=conn, survey_control_guid=survey_control_guid)

    @staticmethod
    def calc_tam_for_survey_tags(conn: DbConnection, survey_control_guid: str | None = None):
        all_surveys = -1
        if survey_control_guid is None:
            all_surveys = 1

        query = f"""
            DELETE FROM survey_data.survey_tam_per_tag s
             WHERE s.survey_meta_id IN (
                SELECT m.id
                  FROM survey_data.survey_meta m
                 WHERE (m.survey_control_guid = '{survey_control_guid}' OR {all_surveys} = 1)
             )
            """

        conn.query(query)

        query = f"""
            INSERT INTO survey_data.survey_tam_per_tag (
            survey_meta_id, 
            tag_id_int,
            ltv, 
            filtered_cnt, 
            total_cnt, 
            tam_base)
            SELECT z.survey_meta_id,
                   z.tag_id_int,
                   z.ltv,
                   z.filtered_cnt,
                   z.total_cnt,
                   IF(z.filtered_cnt = 0 or z.total_cnt = 0 or z.ltv = 0, 0, z.filtered_cnt / z.total_cnt * z.ltv) as tam_base
              FROM (
                SELECT MAX(x.survey_meta_id) as survey_meta_id,
                       x.tag_id_int,
                       AVG(x.spending) AS ltv,
                       MAX(x.tag_id_cnt) AS filtered_cnt,
                       MAX(x.total_cnt) AS total_cnt
                  FROM (
                    SELECT m.id AS survey_meta_id,
                           p.tag_id_int,
                           i.spending,
                           ROW_NUMBER() OVER (PARTITION BY t.tag_id ORDER BY i.spending) AS tag_id_row_order,
                           COUNT(1) OVER (PARTITION BY t.tag_id) AS tag_id_cnt,
                           y.cnt AS total_cnt
                      FROM survey_data.survey_meta m,
                           survey_data.survey_info i USE INDEX(),
                           survey_data.survey_tags t USE INDEX(),
                           app.def_sheet_platform_product p,
                         (SELECT COUNT(1) AS cnt
                             FROM survey_data.survey_meta mm,
                                  survey_data.survey_info ii
                            WHERE (mm.survey_control_guid = '{survey_control_guid}' OR {all_surveys} = 1)
                              AND mm.id = ii.survey_meta_id ) y
                     WHERE (m.survey_control_guid = '{survey_control_guid}' OR {all_surveys} = 1)
                       AND m.id = t.survey_meta_id
                       AND m.id = i.survey_meta_id
                       AND i.survey_instance = t.survey_instance
                       AND t.tag_id = p.tag_id
                       AND t.tag_value = 100
                       AND p.is_prompt = 1
                     ORDER BY p.tag_id_int, i.spending
                       ) x
                 WHERE x.tag_id_row_order IN (FLOOR((x.tag_id_cnt + 1) / 2), FLOOR((x.tag_id_cnt + 2) / 2) )
                 GROUP BY x.tag_id_int ) z
            """

        conn.query(query)
