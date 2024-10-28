import json
import re

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.apps.app_model import AppModel
from src.server.models.tags.prompts.base_prompt_handler import BasePromptHandler
from src.server.models.tags.prompts.prompts_def import PromptsDef
from src.server.models.quota.base_quota_context import BaseQuotaContext


class DefaultPromptHandler(BasePromptHandler):
    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            quota_context: BaseQuotaContext,
            app_id_int: int,
            prompt_id: str,
            retry_prompt_row_id: int | None = None
    ):
        super(DefaultPromptHandler, self).__init__(
            conn=conn,
            user_id=user_id,
            quota_context=quota_context,
            app_id_int=app_id_int,
            prompt_id=prompt_id,
            retry_prompt_row_id=retry_prompt_row_id
        )
        self.app_detail = None

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

        prompts_helper = self.get_helper()

        system_prompt_def = prompts_helper.get_prompt_template("gpt4_context")
        user_prompt_def = prompts_helper.get_prompt_template(self.prompt_id)

        app_title = self.app_detail["title"]
        app_desc = self.app_detail["description"]
        platform = self.app_detail["platform"]

        system_msg = system_prompt_def
        system_msg = system_msg.replace("[GAME TITLE]", app_title)
        system_msg = system_msg.replace("[STORE DESCRIPTION]", app_desc)
        system_msg = system_msg.replace("[PLATFORM]", AppModel.get_platform_str(platform))

        user_msg = user_prompt_def
        user_msg = user_msg.replace("[GAME TITLE]", app_title)
        user_msg = user_msg.replace("[PLATFORM]", AppModel.get_platform_str(platform))

        return {
            "state": "done",
            "system_msg": system_msg,
            "user_msg": user_msg
        }

    @staticmethod
    def parse(prompt_id: str, result: {}, prompts_helper: PromptsDef):
        tags_ids_int = []
        tags_open = []
        words = re.findall(r'[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)*', result["content"])
        current_category = None

        offset = 0
        for i in range(len(words)):
            index = i + offset
            if index >= len(words):
                break

            word = words[index].strip().lower()

            is_category = prompts_helper.check_if_category_is_defined(
                prompt_id=prompt_id,
                category=word
            )

            if is_category:
                current_category = word
                continue
            elif current_category is None:
                return {
                    "state": "error",
                    "error_id": 2,
                    "error_msg": "invalid response"
                }

            found = False
            potential_word = ""
            for j in range(index, min(index + 5, len(words))):
                potential_word += words[j].lower() + " "
                tag_id_int = prompts_helper.find_tag_id_from_prompt(
                    prompt_id=prompt_id,
                    category=current_category,
                    tag=potential_word.strip().lower()
                )
                if tag_id_int is not None:
                    tags_ids_int.append(tag_id_int)
                    offset += j - index
                    found = True
                    break
            if not found:
                node_id = f"{prompt_id}__{current_category}"
                tags_open.append({
                    "node_id": node_id,
                    "value": word
                })

        return {
            "state": "done",
            "tags": tags_ids_int,
            "tags_open": tags_open
        }

    def parse_result(self):
        if self.prompt_row_id is None:
            return {
                "state": "error"
            }

        row = self.conn.select_one("""
        SELECT p.prompt_id, p.result
          FROM tagged_data.gpt_prompts p
         WHERE p.id = %s
        """, [self.prompt_row_id])

        return DefaultPromptHandler.parse(
            prompt_id=row["prompt_id"],
            result=json.loads(row["result"]),
            prompts_helper=self.get_helper()
        )
