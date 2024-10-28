from gembase_server_core.db.db_connection import DbConnection
from src.server.models.dms.dms_constants import DmsConstants
from src.server.models.dms.dms_model import DmsCache
from src.server.models.tags.tags_mapper import TagsMapper


class PromptsDef:

    def __init__(self, conn: DbConnection):
        self.conn = conn
        self.prompt_sheet = DmsCache.get_json(conn, DmsConstants.prompts_guid)
        self.platform_sheet = DmsCache.get_json(conn, DmsConstants.platform_guid)
        self.map_tags = TagsMapper.instance(conn)

    def get_prompt_tags(self, prompt_id: str) -> []:
        res = []
        for row in self.prompt_sheet["Prompts"]:
            if row["ID"] == prompt_id:
                arr = row["Tags"].split(",")
                for tag_id in arr:
                    tag_id_int = self.map_tags["s2i"][tag_id]
                    res.append(tag_id_int)

        if len(res) == 0:
            raise Exception(f"Tags for prompt {prompt_id} not found")

        return res

    def get_prompt_template(self, prompt_id: str) -> str:
        for row in self.prompt_sheet["Prompts"]:
            if row["ID"] == prompt_id:
                return row["Prompt"]
        raise Exception(f"Prompt def not found {prompt_id}")

    def get_def_for_prompt_id(self, prompt_id: str):
        prompts = self.prompt_sheet["Prompts"]
        for row in prompts:
            if row["ID"] == prompt_id:
                return row

    def check_if_category_is_defined(self, prompt_id: str, category: str):
        prompt_row = self.get_def_for_prompt_id(prompt_id=prompt_id)
        category = category.lower()

        for row in self.prompt_sheet[prompt_row["Sheet"]]:
            if row["Category"].lower() == category.lower():
                return True

        return False

    def find_tag_id_from_sheet(self, sheet_id: str, category: str, tag: str) -> int | None:
        for row in self.prompt_sheet[sheet_id]:
            if row["Category"].lower() == category:
                if row["Tag"].lower() == tag:
                    return self.map_tags["s2i"][row["ID"]]
        return None

    def find_tag_id_from_prompt(self, prompt_id: str, category: str, tag: str) -> int | None:
        prompt_row = self.get_def_for_prompt_id(prompt_id=prompt_id)
        category = category.lower()
        tag = tag.lower()
        tag_id = None

        for row in self.prompt_sheet[prompt_row["Sheet"]]:
            if row["Category"].lower() == category:
                if row["Tag"].lower() == tag:
                    tag_id = row["ID"]
                    break

        if tag_id is not None:
            row = self.conn.select_one_or_none("""
            SELECT p.tag_id_int
              FROM app.def_sheet_platform_product p,
                   app.map_tag_id m
             WHERE m.id = p.tag_id_int
               AND m.tag_id = %s
            """, [tag_id])
            if row is None:
                return None
            return row["tag_id_int"]
        else:
            return None
