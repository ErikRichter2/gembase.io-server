from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.user_billing_db_model import UserBillingDbModel
from src.db.user_db_model import UserDbModel
from src.models.user_obfuscator import UserObfuscator


class UserPlatformDbModel(BaseUserDbModel):
    
    def __init__(self, user_db: UserDbModel):
        super(UserPlatformDbModel, self).__init__(user_db=user_db)
    
    def get_def(self):

        rows_product_nodes = self.conn.select_all("""
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
               p.competitors_pool_w
          FROM app.def_sheet_platform_product p,
               app.map_tag_subcategory m
         WHERE p.is_prompt = 1
           AND m.id = p.subcategory_int
        """)

        module_locked = self.user_db.user_billing_db.is_module_locked(UserBillingDbModel.BILLING_MODULE_AUDITOR)

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

        rows_allowed_tags_per_locked_module = self.conn.select_all("""
        SELECT d.tag_id_int, d.module_id, d.is_set, d.is_loved, d.is_changeable
          FROM app.def_allowed_tags_per_locked_module d
        """)

        rows_user_positions = self.conn.select_all("""
        SELECT d.id, d.position_area, d.position, d.position_role
          FROM app.def_user_position d
        """)

        rows_user_roles = self.conn.select_all("""
            SELECT d.id, d.name
              FROM app.def_user_roles d
            """)

        rows_user_position_role = self.conn.select_all("""
            SELECT d.id, d.value
              FROM app.def_user_position_role d
            """)

        rows_user_position_area = self.conn.select_all("""
                SELECT d.id, d.value
                  FROM app.def_user_position_area d
                """)

        rows_install_tiers = self.conn.select_all("""
        SELECT t.store_id, t.tier, t.value_from, t.value_to
          FROM app.def_sheet_platform_values_install_tiers t
        """)

        rows_ts_params = self.conn.select_all("""
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
            "ts_params": rows_ts_params
        }        