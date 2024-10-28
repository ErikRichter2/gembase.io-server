import openai

GPT_MODEL_4o = "gpt-4o"
GPT_MODEL_o1 = "o1-preview"

GPT4_FINISH_REASON_STOP = "stop"
GPT4_FINISH_REASON_LENGTH = "length"
GPT4_FINISH_REASON_FUNCTION_CALL = "function_call"
GPT4_FINISH_REASON_CONTENT_FILTER = "content_filter"
GPT4_FINISH_REASON_NULL = "null"


def chat_gpt4(messages: [], api_key: str, temperature=0, model=GPT_MODEL_4o) -> {}:

    if model is None:
        model = GPT_MODEL_4o

    if temperature is None:
        if model == GPT_MODEL_o1:
            temperature = 1
        else:
            temperature = 0

    openai.api_key = api_key
    completion = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=temperature
    )

    return completion
