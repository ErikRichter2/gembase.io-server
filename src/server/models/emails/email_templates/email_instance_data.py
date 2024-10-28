from src.server.models.user.user_model import UserModel
from src.server.models.user.user_obfuscator import UserObfuscator
from src.utils.gembase_utils import GembaseUtils


class EmailInstanceData:

    def __init__(self):
        self.template_parameters = {}
        self.content_parameters = {}
        self.draft_parameters = {}
        self.from_address = None
        self.subject = None

    @staticmethod
    def from_json(instance_data):
        res = EmailInstanceData()
        res.template_parameters = instance_data["template_parameters"]
        res.content_parameters = instance_data["content_parameters"]
        res.draft_parameters = instance_data["draft_parameters"]
        res.from_address = instance_data["from_address"]
        res.subject = instance_data["subject"]
        return res

    def get_client_data(self, user: UserModel):

        res = {
            "template_parameters": self.template_parameters,
            "content_parameters": self.content_parameters,
            "draft_parameters": self.draft_parameters,
            "from_address": self.from_address,
            "subject": self.subject
        }

        draft_params = res["draft_parameters"]

        if "app_id" in draft_params and UserObfuscator.APP_ID_INT not in draft_params:
            draft_params[UserObfuscator.APP_ID_INT] = int(draft_params["app_id"])
        if "audience_angle_id" in draft_params and UserObfuscator.AUDIENCE_ANGLE_ID_INT not in draft_params:
            draft_params[UserObfuscator.AUDIENCE_ANGLE_ID_INT] = int(draft_params["audience_angle_id"])

        return GembaseUtils.json_copy(res)
