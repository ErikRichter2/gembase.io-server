import json

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.external.open_ai.open_ai import chat_gpt4, GPT_MODEL_4o
from gembase_server_core.private_data.private_data_model import PrivateDataModel
from src.server.models.quota.base_quota_context import BaseQuotaContext
from src.utils.gembase_utils import GembaseUtils


class OpenAiChatGptModel:

    @staticmethod
    def gpt4_raw(messages: [], temperature=0, model=GPT_MODEL_4o):
        api_key = PrivateDataModel.get_private_data()['open_ai']['chat_gpt4']['key']
        completion = chat_gpt4(
            api_key=api_key,
            model=model,
            messages=messages,
            temperature=temperature
        )
        return completion

    @staticmethod
    def gpt4(quota_context: BaseQuotaContext, system_msg: str, user_msg: str, temperature=0, model=GPT_MODEL_4o) -> {}:

        messages = [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg}
        ]

        conn = DbConnection()
        input_data = {
            "model": model,
            "temperature": temperature,
            "messages": messages
        }
        audit_guid = GembaseUtils.get_guid()
        audit_row_id = conn.insert("""
        INSERT INTO audit.openai_chat_gpt (tokens, context, input_data, audit_guid) VALUES (0, %s, %s, %s)
        """, [json.dumps(quota_context.get_audit_context()), GembaseUtils.compress(json.dumps(input_data)), audit_guid])
        conn.commit()
        conn.close()

        completion = OpenAiChatGptModel.gpt4_raw(messages, temperature, model)
        finish_reason = completion.choices[0].finish_reason
        content = completion.choices[0].message.content
        total_tokens = completion['usage']['total_tokens']

        conn = DbConnection()
        conn.query("""
        UPDATE audit.openai_chat_gpt
           SET tokens = %s,
               response_data = %s
         WHERE id = %s
        """, [total_tokens, GembaseUtils.compress(json.dumps(completion)), audit_row_id])
        conn.commit()
        conn.close()

        quota_context.add(total_tokens, audit_guid)

        return {
            'content': content,
            "finish_reason": finish_reason,
            'tokens': total_tokens,
            "original_response": completion,
            "audit_guid": audit_guid
        }
