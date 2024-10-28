import json

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.tags.prompts.prompts_def import PromptsDef
from src.external_api.openai_chat_gpt_model import OpenAiChatGptModel
from src.server.models.quota.base_quota_context import BaseQuotaContext


class BasePromptHandler:

    def __init__(
            self,
            conn: DbConnection,
            user_id: int,
            quota_context: BaseQuotaContext,
            app_id_int: int,
            prompt_id: str,
            retry_prompt_row_id: int | None = None
    ):
        self.conn = conn
        self.user_id = user_id
        self.quota_context = quota_context
        self.__prompts_helper = None
        self.prompt_row_id = None
        self.app_id_int = app_id_int
        self.prompt_id = prompt_id
        self.retry_prompt_row_id = retry_prompt_row_id
        self.audit_guid = None
        self.max_retry_count_exceeded = False

    def set_helper(self, prompts_helper: PromptsDef):
        self.__prompts_helper = prompts_helper

    def get_helper(self) -> PromptsDef:
        if self.__prompts_helper is None:
            self.__prompts_helper = PromptsDef(conn=self.conn)
        return self.__prompts_helper

    def run(self):

        retry_count = 0

        if self.retry_prompt_row_id != 0:
            row_retry = self.conn.select_one_or_none("""
            SELECT p.retry_count
              FROM tagged_data.gpt_prompts p
             WHERE p.id = %s
            """, [self.retry_prompt_row_id])
            if row_retry is not None:
                retry_count = row_retry["retry_count"]
            retry_count += 1

        if retry_count >= 5:
            self.max_retry_count_exceeded = True
            return {
                "state": "error",
                "error_id": "max_retry_count"
            }

        messages = self.get_prompt_messages()

        if messages["state"] == "done":

            result = OpenAiChatGptModel.gpt4(
                quota_context=self.quota_context,
                system_msg=messages["system_msg"],
                user_msg=messages["user_msg"]
            )

            self.audit_guid = result["audit_guid"]

            self.prompt_row_id = self.conn.insert("""
            INSERT INTO tagged_data.gpt_prompts
            (app_id_int, prompt_id, user_id, finish_reason, tokens, result, retry_row_id, retry_count, audit_guid) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [self.app_id_int, self.prompt_id, self.user_id, result["finish_reason"],
                  result["tokens"], json.dumps(result), self.retry_prompt_row_id, retry_count,
                  result["audit_guid"]])
            self.conn.commit()
        else:
            self.prompt_row_id = self.conn.insert("""
            INSERT INTO tagged_data.gpt_prompts
            (app_id_int, prompt_id, user_id, finish_reason, tokens, result, retry_row_id, retry_count, parse_state, parse_reason) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [self.app_id_int, self.prompt_id, self.user_id, "skip",
                  0, "", self.retry_prompt_row_id, retry_count, messages["error_id"], messages["error_msg"]])
            self.conn.commit()
            return {
                "state": "error",
                "error_msg": json.dumps(messages)
            }

        return {
            "state": "done"
        }

    def get_prompt_messages(self) -> {}:
        return {}

    def parse_result(self):
        pass
