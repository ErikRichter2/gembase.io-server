from gembase_server_core.db.db_connection import DbConnection
from src.server.models.tags.tags_constants import TagsConstants


class TagsDef:

    def __init__(self, conn: DbConnection):
        self.__conn = conn
        self.__data = None
        self.__subc_per_tag = None
        self.__init()

    def __init(self):
        if self.__data is None:
            self.__data = self.__conn.select_all("""
            SELECT p.tag_id_int, p.unlocked, p.subcategory_int
              FROM app.def_sheet_platform_product p
            """)
            self.__subc_per_tag = {}
            for row in self.__data:
                self.__subc_per_tag[row["tag_id_int"]] = row["subcategory_int"]
        return self.__data

    def get_unlocked_tags_ids(self) -> list[int]:
        res = []

        for it in self.__data:
            if it["unlocked"] == 1:
                res.append(it["tag_id_int"])

        return res

    def check_tags_ids_exists(self, tags_ids_int: list[int]) -> bool:
        current_tags_ids_int = [x["tag_id_int"] for x in self.__data]
        for tag_id_int in tags_ids_int:
            if tag_id_int not in current_tags_ids_int:
                return False
        return True

    def check_tags_details(self, tags_details: list) -> bool:
        tags_ids_int = [x["tag_id_int"] for x in tags_details]
        if not self.check_tags_ids_exists(tags_ids_int=tags_ids_int):
            return False
        ranks_per_subc = {}
        for tag_detail in tags_details:
            tag_id_int = tag_detail["tag_id_int"]
            tag_rank = tag_detail["tag_rank"]
            if tag_rank not in TagsConstants.ALLOWED_TAG_RANKS:
                return False
            if tag_rank != TagsConstants.TAG_RANK_NONE:
                subc_id = self.__subc_per_tag[tag_id_int]
                if subc_id not in TagsConstants.ALLOWED_RANKED_SUBCATEGORIES:
                    return False
                if subc_id not in ranks_per_subc:
                    ranks_per_subc[subc_id] = []
                if tag_rank in ranks_per_subc[subc_id]:
                    return False
                ranks_per_subc[subc_id].append(tag_rank)
        return True
