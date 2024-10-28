import random

from src.server.models.survey.v2.survey_model_v2 import SurveyModelV2
from src.server.models.survey.v2.survey_page_model import SurveyPageModel
from src.utils.gembase_utils import GembaseUtils


class SurveyRandomRespondentModel:

    def __init__(self, config: {}, favorite_apps_ids_int: []):
        self.__config = config
        self.end_type = None

        self.__model: SurveyModelV2 = SurveyModelV2(
            config=self.__config,
            client_data=None,
            server_data=None,
            ext_data=None
        )

        while True:
            export = self.__model.export()

            while True:
                submit = {}
                page = export["config_data"]["id"]
                if page == "gender":
                    submit[page] = "m" if random.random() < 0.5 else "f"
                elif page == "favorite_game":
                    submit[page] = GembaseUtils.random_from_array(favorite_apps_ids_int)
                else:
                    for section in export["config_data"]["sections"]:
                        if section["answers_group"]["type"] == "radio":
                            if page == "genres_dcm" or page == "topics_dcm" or page == "concepts_dcm":
                                if random.random() < 0.1:
                                    submit[section["id"]] = "none"
                                else:
                                    rnd = GembaseUtils.random_from_array(section["answers_group"][page]["choices"])
                                    submit[section["id"]] = rnd["id"]
                            else:
                                rnd = GembaseUtils.random_from_array(section["answers_group"]["answers"])
                                submit[section["id"]] = rnd["id"]
                        elif section["answers_group"]["type"] == "check":
                            submit[section["id"]] = []
                            arr = section["answers_group"]["answers"].copy()
                            random.shuffle(arr)
                            for i in range(random.randrange(0, len(arr))):
                                submit[section["id"]].append(arr[i]["id"])

                ended, end_type = SurveyPageModel.is_screenout(
                    page=export["config_data"]["id"],
                    config_data=self.__config,
                    client_data=submit
                )
                if ended:
                    if end_type == "trap":
                        if random.random() < 0.01:
                            break
                    else:
                        if random.random() < 0.1:
                            break
                else:
                    break

            self.__model.submit(submit)
            export = self.__model.export()
            if "end_type" in export["config_data"]:
                self.end_type = export["config_data"]["end_type"]
                break

    def get_client_data(self):
        return self.__model.get_client_data_raw()

    def get_server_data(self):
        return self.__model.get_server_data_raw()
