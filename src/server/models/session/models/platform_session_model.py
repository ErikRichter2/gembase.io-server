from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.platform_values.calc.platform_values_gaps_v2 import PlatformValuesGapsV2Calc
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.survey.survey_data_model import SurveyDataModel
from src.server.models.tags.tags_constants import TagsConstants
from src.server.models.user.user_obfuscator import UserObfuscator


class PlatformSessionModel(BaseSessionModel):

    SURVEY_CONTROL_GUID = "a236b79f-6d2e-4f23-ad92-d018cb518346"

    def get_opportunity_detail(
            self,
            uuid: str
    ):
        return PlatformValuesGapsV2Calc.generate_client_data_for_single_gap(
            conn=self.conn(),
            uuid=uuid
        )

    def get_def(self):

        rows_product_nodes = self.conn().select_all("""
        SELECT p.tag_id_int, 
               p.category, 
               p.subcategory_int,
               m.subcategory,
               m.client_name as subcategory_client_name,
               p.node,
               p.adj, 
               p.is_survey,
               p.description,
               p.unlocked,
               p.competitors_pool_w,
               p.hidden,
               IF(p.tag_id_int = %s, 1, 0) as platform_mobile,
               IF(p.tag_id_int = %s, 1, 0) as platform_pc
          FROM app.def_sheet_platform_product p,
               app.map_tag_subcategory m
         WHERE p.is_prompt = 1
           AND m.id = p.subcategory_int
        """, [TagsConstants.PLATFORM_MOBILE, TagsConstants.PLATFORM_PC])

        module_locked = self.session().models().billing().is_module_locked(BillingUtils.BILLING_MODULE_AUDITOR)

        advanced_filter_weights = []
        advanced_filter_subcategories_arr = []

        for row in rows_product_nodes:
            if row["competitors_pool_w"] != 0:
                subcategory_int = row["subcategory_int"]
                if subcategory_int not in advanced_filter_subcategories_arr:
                    advanced_filter_subcategories_arr.append(subcategory_int)
                    advanced_filter_weights.append({
                        "subcategory_int": subcategory_int,
                        "weight": row["competitors_pool_w"]
                    })

            del row["competitors_pool_w"]

            row["locked"] = 0
            if row["unlocked"] == 0:
                if module_locked:
                    row["node"] = ""
                    row["adj"] = ""
                    row["description"] = ""
                    row["locked"] = 1

        rows_product_nodes.sort(key=lambda x: x[UserObfuscator.TAG_ID_INT])

        rows_product_nodes.append({
            "tag_id": "__wip",
            "category": "Content",
            "subcategory_int": 48,
            "subcategory": "IPs",
            "node": "",
            "adj": "",
            "is_survey": 1,
            "description": "",
            "locked": 1 if module_locked else 0,
            "wip": 1
        })

        rows_allowed_tags_per_locked_module = self.conn().select_all("""
        SELECT d.tag_id_int, d.module_id, d.is_set, d.is_loved, d.is_changeable
          FROM app.def_allowed_tags_per_locked_module d
        """)

        rows_user_positions = self.conn().select_all("""
        SELECT d.id, d.position_area, d.position, d.position_role
          FROM app.def_user_position d
        """)

        rows_user_roles = self.conn().select_all("""
            SELECT d.id, d.name
              FROM app.def_user_roles d
            """)

        rows_user_position_role = self.conn().select_all("""
            SELECT d.id, d.value
              FROM app.def_user_position_role d
            """)

        rows_user_position_area = self.conn().select_all("""
                SELECT d.id, d.value
                  FROM app.def_user_position_area d
                """)

        rows_install_tiers = self.conn().select_all("""
        SELECT t.store_id, t.tier, t.value_from, t.value_to
          FROM app.def_sheet_platform_values_install_tiers t
        """)

        rows_ts_params = self.conn().select_all("""
        SELECT p.param, p.v3, p.name
          FROM app.def_sheet_platform_threats_params p
         WHERE p.param IN ('similar', 'size', 'growth', 'quality', 'trend', 'tam')
        """)

        return {
            "product_nodes": rows_product_nodes,
            "user_roles": rows_user_roles,
            "user_positions": rows_user_positions,
            "user_position_role": rows_user_position_role,
            "user_position_area": rows_user_position_area,
            "allowed_tags_per_locked_module": rows_allowed_tags_per_locked_module,
            "advanced_filter_weights": advanced_filter_weights,
            "install_tiers": rows_install_tiers,
            "ts_params": rows_ts_params,
            "potential_downloads": {
                "geo": [
                    {"id": "eu", "ratio": PlatformValuesHelper.POTENTIAL_DOWNLOADS_EU_RATIO},
                    {"id": "na", "ratio": PlatformValuesHelper.POTENTIAL_DOWNLOADS_NA_RATIO},
                    {"id": "latam", "ratio": PlatformValuesHelper.POTENTIAL_DOWNLOADS_LATAM_RATIO},
                    {"id": "mena", "ratio": PlatformValuesHelper.POTENTIAL_DOWNLOADS_MENA_RATIO},
                    {"id": "apac", "ratio": PlatformValuesHelper.POTENTIAL_DOWNLOADS_APAC_RATIO}
                ],
                "platform": [
                    {"id": "mobile", "absolute": PlatformValuesHelper.POTENTIAL_DOWNLOADS_MOBILE},
                    {"id": "pc", "absolute": PlatformValuesHelper.POTENTIAL_DOWNLOADS_PC},
                    {"id": "console", "absolute": PlatformValuesHelper.POTENTIAL_DOWNLOADS_CONSOLE}
                ]
            }
        }

    def get_top_loved_apps(
            self,
            audience_angle_id_int: int
    ):
        query = """
        SELECT a.title
          FROM platform.audience_angle aa,
               scraped_data.apps a,
               platform.platform_values_tags_bin t
         WHERE aa.id = %s
           AND BIT_COUNT(aa.b & t.b) = aa.angle_cnt
           AND t.app_id_int = a.app_id_int
         ORDER BY aa.installs DESC
         LIMIT 3
        """

        rows = self.conn().select_all(query, [audience_angle_id_int])
        return [row["title"] for row in rows]

    def get_most_hated_tags(
            self,
            loved_tag_ids_int: list[int],
            hated_tag_ids_int: list[int]
    ):
        survey_id = SurveyDataModel.get_survey_meta_id(
            conn=self.conn(),
            survey_control_guid=PlatformSessionModel.SURVEY_CONTROL_GUID
        )
        loved_tags_ids_int_db = self.conn().values_arr_to_db_in(loved_tag_ids_int, int_values=True)

        if len(hated_tag_ids_int) == 0:
            hated_tag_ids_int = [-1]

        hated_tags_ids_int_db = self.conn().values_arr_to_db_in(hated_tag_ids_int, int_values=True)

        loved_cnt = len(loved_tag_ids_int)

        query = f"""
        SELECT st.tag_id_int
          FROM (
                SELECT stb.survey_instance_int
                  FROM (
                        SELECT BIT_OR(d.b) as b
                        FROM platform.def_tags_bin d
                        WHERE d.tag_id_int IN ({loved_tags_ids_int_db})
                       ) l,
                       (
                        SELECT BIT_OR(d.b) as b
                        FROM platform.def_tags_bin d
                        WHERE d.tag_id_int IN ({hated_tags_ids_int_db})
                       ) h,
                       platform.platform_values_survey_tags_bin stb,
                       platform.def_tags_bin d
                 WHERE stb.survey_id = {survey_id}
                   AND BIT_COUNT(stb.b_rejected & h.b) > 0
                   AND BIT_COUNT(stb.b_loved & l.b) = {loved_cnt}
                 GROUP BY stb.survey_instance_int
               ) z1,
               platform.platform_values_survey_tags st,
               platform.def_tags dt
         WHERE st.survey_meta_id = {survey_id}
           AND st.survey_instance_int = z1.survey_instance_int
           AND st.tag_id_int IN ({hated_tags_ids_int_db})
           AND st.rejected = 1
           AND dt.tag_id_int = st.tag_id_int
         GROUP BY st.tag_id_int
         ORDER BY COUNT(1) DESC
         LIMIT 5
        """

        rows = self.conn().select_all(query)

        return {
            "tag_ids_int": [row["tag_id_int"] for row in rows]
        }
