import json
import re

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.apps.app_model import AppModel
from src.server.models.apps.app_tag_model import AppTagModel
from src.server.models.tags.prompts.base_prompt_handler import BasePromptHandler
from src.server.models.tags.prompts.prompts_def import PromptsDef
from src.server.models.quota.base_quota_context import BaseQuotaContext


class PriorityTagsPromptHandler(BasePromptHandler):

    DEFAULT_NUMBER_OF_CATEGORIES = 2

    def __init__(
        self,
        conn: DbConnection,
        user_id: int,
        quota_context: BaseQuotaContext,
        app_id_int: int,
        tags_ids_int: [],
        retry_prompt_row_id: int | None = None,
        prompt_id: str | None = None,
        number_of_categories=DEFAULT_NUMBER_OF_CATEGORIES
    ):
        if prompt_id is None:
            prompt_id = AppTagModel.PROMPT_PRIORITY_TAGS
        super(PriorityTagsPromptHandler, self).__init__(
            conn=conn,
            user_id=user_id,
            quota_context=quota_context,
            app_id_int=app_id_int,
            prompt_id=prompt_id,
            retry_prompt_row_id=retry_prompt_row_id
        )
        self.app_detail = None
        self.tags_ids_int = tags_ids_int
        self.number_of_categories = number_of_categories

    def __get_app_detail(self):
        if self.app_detail is None:
            self.app_detail = AppModel.get_app_detail(
                conn=self.conn,
                app_id_int=self.app_id_int,
                user_id=-1
            )
        return self.app_detail

    def __is_valid_app_data(self) -> bool:

        app_detail = self.__get_app_detail()

        if app_detail is None:
            return False

        app_desc = app_detail["description"]
        if app_desc is None:
            return False
        app_desc = app_desc.replace("\n", "").replace("\r", "").strip()

        if len(app_desc) < 10:
            return False

        return True

    def get_prompt_messages(self) -> {}:

        if not self.__is_valid_app_data():
            return {
                "state": "error",
                "error_id": -3,
                "error_msg": "invalid app description"
            }

        genre_tags = []
        topics_tags = []
        # behaviors_tags = []

        for tag_id_int in self.tags_ids_int:
            subc = self.get_helper().map_tags["i2subc"][tag_id_int]
            node = self.get_helper().map_tags["i2n"][tag_id_int]

            if subc == "genre":
                genre_tags.append(node)
            elif subc == "topics":
                topics_tags.append(node)
            # elif subc == "behaviors":
            #    behaviors_tags.append(node)

        # !!! set number_of_categories for parse !!!

        app_title = self.app_detail["title"]
        app_desc = self.app_detail["description"]
        platform = self.app_detail["platform"]

        system_msg = self.get_helper().get_prompt_template("gpt4_context")
        system_msg = system_msg.replace("[PLATFORM]", AppModel.get_platform_str(platform))
        system_msg = system_msg.replace("[GAME TITLE]", app_title)
        system_msg = system_msg.replace("[STORE DESCRIPTION]", app_desc)

        user_msg = self.get_helper().get_prompt_template(self.prompt_id)
        user_msg = user_msg.replace("[PLATFORM]", AppModel.get_platform_str(platform))
        user_msg = user_msg.replace("[GAME TITLE]", app_title)
        user_msg = user_msg.replace("[SELECTED TAGS 1]", ", ".join(genre_tags))
        user_msg = user_msg.replace("[SELECTED TAGS 2]", ", ".join(topics_tags))
        #user_msg = user_msg.replace("[SELECTED TAGS 3]", ", ".join(behaviors_tags))

        return {
            "state": "done",
            "system_msg": system_msg,
            "user_msg": user_msg
        }

    @staticmethod
    def parse(result: {}, prompts_helper: PromptsDef, number_of_categories=DEFAULT_NUMBER_OF_CATEGORIES):
        parsed = []
        unknown = []
        words = re.findall(r'[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)*', result["content"])
        current_rank = None
        current_category = None
        offset = 0

        data_def = {
            "genres": [
                "Genres",
                "genre"
            ],
            "topics": [
                "Themes",
                "topics"
            ],
        }

        for i in range(len(words)):
            index = i + offset
            if index >= len(words):
                break

            word = words[index].strip().lower()

            if word in data_def:
                current_category = word
                current_rank = None
                continue

            if current_category is None:
                return {
                    "state": "error",
                    "error_id": 2,
                    "error_msg": "invalid response"
                }

            if word == "primary":
                current_rank = 1
                continue
            if word == "secondary":
                current_rank = 2
                continue
            if word == "tertiary":
                current_rank = 3
                continue

            if current_rank is None:
                return {
                    "state": "error",
                    "error_id": 3,
                    "error_msg": "invalid response"
                }

            if word == "none" or word == "n/a":
                continue

            found = False
            arr = [current_category]
            for k in data_def:
                if k != current_category:
                    arr.append(k)
            for k in arr:
                if found:
                    break
                potential_word = ""
                for j in range(index, min(index + 5, len(words))):
                    potential_word += words[j].lower() + " "
                    tag_id_int = prompts_helper.find_tag_id_from_sheet(
                        sheet_id=data_def[k][0],
                        category=data_def[k][1],
                        tag=potential_word.strip().lower()
                    )
                    if tag_id_int is not None:
                        parsed.append({
                            "tag_id_int": tag_id_int,
                            "tag_rank": current_rank
                        })
                        offset += j - index
                        found = True
                        break
            if not found:
                unknown.append({
                    "category": current_category,
                    "node": word,
                    "tag_rank": current_rank
                })

        return {
            "state": "done",
            "tags_details": parsed,
            "unknown": unknown
        }

    def parse_result(self, save_unknown=False):
        if self.prompt_row_id is None:
            return {
                "state": "error"
            }

        row = self.conn.select_one("""
        SELECT p.result
          FROM tagged_data.gpt_prompts p
         WHERE p.id = %s
        """, [self.prompt_row_id])

        res = PriorityTagsPromptHandler.parse(
            result=json.loads(row["result"]),
            prompts_helper=self.get_helper(),
            number_of_categories=self.number_of_categories
        )

        if save_unknown and res["state"] == "done":
            PriorityTagsPromptHandler.save_unknown_tags(
                conn=self.conn,
                unknown=res["unknown"],
                audit_guid=self.audit_guid,
                app_id_int=self.app_id_int
            )

        return res

    @staticmethod
    def save_unknown_tags(conn: DbConnection, unknown: [], audit_guid: str, app_id_int: int):
        bulk_data = []
        for it in unknown:
            bulk_data.append((app_id_int, audit_guid, it["category"], it["node"], it["tag_rank"]))
        conn.bulk("""
        INSERT INTO tagged_data.gpt_unknown_ranked_tags (app_id_int, audit_guid, category, node, tag_rank)
        VALUES (%s, %s, %s, %s, %s)
        """, bulk_data)
