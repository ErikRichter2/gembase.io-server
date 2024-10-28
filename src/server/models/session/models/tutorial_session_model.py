import json

from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.user.user_obfuscator import UserObfuscator


class TutorialSessionModel(BaseSessionModel):

    def __get_tutorial_data(self):
        row = self.conn().select_one_or_none("""
        SELECT ut.tutorial
          FROM app.users_tutorial ut
         WHERE ut.user_id = %s
        """, [self.user_id()])

        if row is None or row["tutorial"] is None:
            return {}
        else:
            return json.loads(row["tutorial"])

    def get_client_data(self):
        return {
            "tutorial": self.__get_tutorial_data(),
            "app_id_int": BillingUtils.UNLOCKED_DEFAULT_APP_ID_INT,
            "gaps": {
                UserObfuscator.TAG_IDS_INT: [
                    37,  # builder
                    31,  # combat
                    231  # adventure
                ]
            }
        }

    def set_module_seen(
            self,
            module_id: int
    ):
        data = self.__get_tutorial_data()

        if "modules_seen" not in data:
            data["modules_seen"] = []
        if module_id not in data["modules_seen"]:
            data["modules_seen"].append(module_id)
            self.__save(data=data)
        if len(data["modules_seen"]) >= 3:
            self.__finish_tutorial()

    def __save(self, data: dict):
        if self.session().user().is_fake_logged():
            return

        exists = self.conn().select_one_or_none("""
        SELECT 1
          FROM app.users_tutorial
         WHERE user_id = %s
        """, [self.user_id()])

        if exists is None:
            self.conn().query("""
            INSERT INTO app.users_tutorial (user_id, tutorial)
            VALUES (%s, %s)
            """, [self.user_id(), json.dumps(data)])
        else:
            self.conn().query("""
            UPDATE app.users_tutorial
               SET tutorial = %s
             WHERE user_id = %s
            """, [json.dumps(data), self.user_id()])

    def __finish_tutorial(self):
        self.session().user().set__tutorial_finished()
