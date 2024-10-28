import flask

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.tags.tags_constants import TagsConstants


class TagsMapper:

    def __init__(self, conn: DbConnection):

        rows = conn.select_all("""
        SELECT p.tag_id_int, p.tag_id, p.node, p.subcategory_int, m.subcategory, 
        p.is_prompt, p.adj, p.competitors_pool_w, p.threatscore_similarity_w,
        p.is_survey
          FROM app.def_sheet_platform_product p,
               app.map_tag_subcategory m
         WHERE m.id = p.subcategory_int
        """)

        str_to_int = {}
        int_to_str = {}
        node_to_int_prompts = {}
        int_to_node = {}
        int_to_subc = {}
        int_to_subci = {}
        subci_to_int = {}
        i_2_def = {}
        competitors_pool_w = {}
        threatscore_similarity_w = {}

        for row in rows:
            i_2_def[row["tag_id_int"]] = row
            str_to_int[row["tag_id"]] = row["tag_id_int"]
            int_to_str[row["tag_id_int"]] = row["tag_id"]
            int_to_node[row["tag_id_int"]] = row["node"].strip().lower()
            competitors_pool_w[row["tag_id_int"]] = row["competitors_pool_w"]
            threatscore_similarity_w[row["tag_id_int"]] = row["threatscore_similarity_w"]

            if row["is_prompt"] == 1 and row["subcategory_int"] in [
                TagsConstants.SUBCATEGORY_TOPICS_ID,
                TagsConstants.SUBCATEGORY_GENRE_ID,
                TagsConstants.SUBCATEGORY_BEHAVIORS_ID
            ]:
                node = row["node"].strip().lower()
                if node in node_to_int_prompts:
                    raise Exception(f"Prompt node '{node}' already exists")
                node_to_int_prompts[node] = row["tag_id_int"]

            int_to_subc[row["tag_id_int"]] = row["subcategory"].strip().lower()

            int_to_subci[row["tag_id_int"]] = row["subcategory_int"]
            if row["subcategory_int"] not in subci_to_int:
                subci_to_int[row["subcategory_int"]] = []
            subci_to_int[row["subcategory_int"]].append(row["tag_id_int"])

        map_tags = {
            "i2s": int_to_str,
            "s2i": str_to_int,
            "i2n": int_to_node,
            "i2subc": int_to_subc,
            "i2subci": int_to_subci,
            "subci2i": subci_to_int,
            "n2i_prompts": node_to_int_prompts,
            "i2def": i_2_def,
            "competitors_pool_w": competitors_pool_w,
            "threatscore_similarity_w": threatscore_similarity_w,
            "def": rows
        }

        self.map_tags = map_tags

    @staticmethod
    def instance(conn: DbConnection, flask_cache=False):
        if flask_cache:
            map_tags = getattr(flask.g, "map_tags", None)
            if map_tags is not None:
                return map_tags
            map_tags = TagsMapper(conn=conn).map_tags
            setattr(flask.g, "map_tags", map_tags)
            return map_tags
        else:
            return TagsMapper(conn=conn).map_tags
