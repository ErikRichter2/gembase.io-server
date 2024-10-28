import flask

from app import app
from gembase_server_core.external.open_ai.open_ai import chat_gpt4, GPT_MODEL_4o, GPT_MODEL_o1
from gembase_server_core.private_data.private_data_model import PrivateDataModel


@app.post("/api/gpt4-test")
def debug_gpt4_test():
    data = flask.request.json["data"]
    token = "a34f0923-4348-4a24-90d7-9392000c665b"

    if token != data["token"]:
        flask.abort(404)

    model = None
    if "model" in data:
        if data["model"] == "o1":
            model = GPT_MODEL_o1
        else:
            model = GPT_MODEL_4o

    api_key = PrivateDataModel.get_private_data()['open_ai']['chat_gpt4']['key']
    messages = []
    if data["system"] != "":
        messages.append({"role": "system", "content": data["system"]})
    if data["prompt"] != "":
        messages.append({"role": "user", "content": data["prompt"]})

    res = chat_gpt4(
        api_key=api_key,
        messages=messages,
        model=model,
        temperature=None
    )

    content = res.choices[0].message.content

    return content
