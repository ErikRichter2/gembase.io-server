from __future__ import annotations

import random
from typing import TYPE_CHECKING

from gembase_server_core.environment.runtime_constants import rr
from src.server.models.apps.app_model import AppModel
from src.server.models.tags.tags_mapper import TagsMapper
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.tags.tags_constants import TagsConstants
from src.session.session import gb_session
from src.utils.gembase_utils import GembaseUtils

if TYPE_CHECKING:
    from src.server.models.survey.v2.survey_model_v2 import SurveyModelV2


def get_export_for_end_page(end_type: str, config: {}):
    title = None
    redirect = None
    subtitle = None

    for it in config["end"]:
        if it["param"] == "title" and it["value"] == end_type:
            title = it["text"]
        if it["param"] == "redirect" and it["value"] == end_type:
            redirect = it["value_2"]
        if it["param"] == "subtitle":
            subtitle = it["text"]

    if not rr.is_prod():
        redirect = "https://www.google.com"

    return {
        "config_data": {
            "id": "end",
            "template": "end",
            "title": title,
            "subtitle": subtitle,
            "redirect": redirect,
            "end_type": end_type
        }
    }


class SurveyPageModel:

    survey_model: SurveyModelV2 = None

    @staticmethod
    def check_same_answers(page: str, config_data: {}, client_data: {}):
        is_check = False
        if page in config_data:
            for row in config_data[page]:
                if row["param"] == "exit_if_same_answers":
                    is_check = True
                    break

        if not is_check:
            return False

        is_same = None
        is_same_check = False
        if client_data is not None:
            for k in client_data:
                if is_same is None:
                    is_same = client_data[k]
                    is_same_check = True
                if is_same != client_data[k]:
                    is_same_check = False
                    break

        if is_same_check:
            return True

        return False

    @staticmethod
    def is_screenout(page: str, config_data: {}, client_data: {}):
        if page == "role":
            if client_data is not None and "role" in client_data:
                role_answer = client_data["role"]
                for row in config_data[page]:
                    if row["param"] == "answer" and row["value"] == role_answer:
                        if row["value_2"] == "screenout":
                            return True, "screenout"

        if page == "devices":
            if client_data is not None and "devices" in client_data:
                devices_answers = client_data["devices"]
                for row in config_data[page]:
                    if row["param"] == "answer" and row["value_2"] == "screenout_if_not" and row["value"] not in devices_answers:
                        return True, "screenout"

        if client_data is not None and page in client_data:
            answers = client_data[page]
            for row in config_data[page]:
                if "value_2" in row and row["param"] == "answer" and row["value_2"] == "trap" and row["value"] in answers:
                    return True, "trap"

        if SurveyPageModel.check_same_answers(
            page=page,
            config_data=config_data,
            client_data=client_data
        ):
            return True, "screenout"

        return False, None

    def check_screenout(self):
        current_page = self.survey_model.get_current_page()

        if self.survey_model.get_current_page() == "role":
            client_data = self.survey_model.get_client_data()
            if client_data is not None and "role" in client_data:
                config_data = self.survey_model.get_current_config()
                role_answer = client_data["role"]
                for row in config_data:
                    if row["param"] == "answer" and row["value"] == role_answer:
                        if row["value_2"] == "screenout":
                            return get_export_for_end_page("screenout", self.survey_model.get_config())
        if self.survey_model.get_current_page() == "devices":
            client_data = self.survey_model.get_client_data()
            if client_data is not None and "devices" in client_data:
                config_data = self.survey_model.get_current_config()
                devices_answers = client_data["devices"]
                for row in config_data:
                    if row["param"] == "answer" and row["value_2"] == "screenout_if_not" and row["value"] not in devices_answers:
                        return get_export_for_end_page("screenout", self.survey_model.get_config())

        client_data = self.survey_model.get_client_data()
        if client_data is not None and current_page in client_data:
            config_data = self.survey_model.get_current_config()
            answers = client_data[current_page]
            for row in config_data:
                if "value_2" in row and row["param"] == "answer" and row["value_2"] == "trap" and row["value"] in answers:
                    return get_export_for_end_page("trap", self.survey_model.get_config())

        if SurveyPageModel.check_same_answers(
            page=self.survey_model.get_current_page(),
            config_data=self.survey_model.get_all_config(),
            client_data=client_data
        ):
            return get_export_for_end_page("trap", self.survey_model.get_config())

        return None

    def track_dcm_submit_time(self, dcm_type: str, index: int):
        timestamp = GembaseUtils.timestamp_int()
        ext_data = self.survey_model.get_ext_data()
        if ext_data is None:
            ext_data = {}
        if "dcm_time" not in ext_data:
            ext_data["dcm_time"] = {}
        if dcm_type not in ext_data["dcm_time"]:
            ext_data["dcm_time"][dcm_type] = []
        track_data = None
        for it in ext_data["dcm_time"][dcm_type]:
            if it["i"] == index:
                track_data = it
                break
        if track_data is None:
            track_data = {
                "i": index,
                "f": timestamp,
                "l": timestamp
            }
            ext_data["dcm_time"][dcm_type].append(track_data)
        else:
            track_data["l"] = timestamp
        self.survey_model.set_ext_data(ext_data)

    def set_survey_model(self, survey_model: SurveyModelV2):
        self.survey_model = survey_model

    def init(self):
        server_data = self.survey_model.get_server_data()
        if server_data is None:
            v = self.get_config_param_value("condition_answer")
            if v is not None:
                c = self.survey_model.get_client_data(v)
                if c is not None:
                    v2 = self.get_config_param_value("condition_answer", value="value_2")
                    v3 = self.get_config_param_value("condition_answer", value="value_3")
                    found = False
                    for k in c:
                        if v2 in c[k]:
                            found = True
                            break

                    if not found:
                        self.set_next_page_by_order()
                        return

            self.init_server_data()

    def init_server_data(self):
        pass

    def export(self):
        self.init()
        return self.export_internal()

    def export_internal(self):
        return {}

    def submit(self, data):
        pass

    def submit_internal(self):
        pass

    def set_next_page_by_order(self):
        current_page = self.survey_model.get_current_page()
        order = self.survey_model.get_config("order")
        for i in range(len(order)):
            if order[i]["id"] == current_page:
                i += 1
                if i < len(order):
                    self.survey_model.set_current_page(order[i]["id"])

    def get_config_param_value(self, param: str, value: str = "value") -> str | None:
        return self.survey_model.get_config_param_value(param, value)

    def add_param_value_if_set(self, config_data, param: str):
        p = self.get_config_param_value(param)
        if p is not None:
            config_data[param] = p

    def add_param_text_if_set(self, config_data, param: str):
        p = self.get_config_param_value(param, "text")
        if p is not None:
            config_data[param] = p

    def create_default_export(self):
        data = {
            "id": self.survey_model.get_current_page(),
            "sections": []
        }
        self.add_param_value_if_set(data, "template")
        self.add_param_value_if_set(data, "split_to_pages")
        config_data = self.survey_model.get_current_config()
        if config_data is not None:
            for row in config_data:
                if row["export"] == "1":
                    if row["value"] is not None and row["value"] != "":
                        data[row["param"]] = row["value"]
                    elif row["text"] is not None and row["text"] != "":
                        data[row["param"]] = row["text"]
        return data


class SurveyDefaultPageModel(SurveyPageModel):

    def export(self):
        current_page = self.survey_model.get_current_page()
        export_data = self.create_default_export()
        config_data = self.survey_model.get_current_config()
        client_data = self.survey_model.get_client_data()

        screenout = self.check_screenout()
        if screenout is not None:
            return screenout

        sections = []

        config_type = self.get_config_param_value("config_type")
        if config_type == "item_per_section":
            for row in config_data:
                if row["param"] == "item":
                    sections.append({
                        "id": row["value"],
                        "title": row["text"],
                        "answers_group": {
                            "type": "",
                            "answers": []
                        }
                    })
            random_items = self.get_config_param_value("random_items")
            if random_items == "TRUE":
                random.shuffle(sections)
        else:
            sections.append({
                "id": current_page,
                "answers_group": {
                    "type": "",
                    "answers": []
                }
            })

        export_data["sections"] = sections
        answers = []
        check_answers_count = None
        check_answers_min_count = None
        check_answers_max_count = None

        last_answers = []

        for row in config_data:
            p = row["param"]
            t = row["text"]
            v = row["value"]
            if p == "answers_type":
                for it in sections:
                    it["answers_group"]["type"] = v
            elif p == "answer":
                answer = {
                    "id": v,
                    "text": t
                }
                if "value_2" in row:
                    if row["value_2"] == "last":
                        last_answers.append(answer)
                answers.append(answer)
            elif p == "check_answers_count":
                check_answers_count = int(v)
            elif p == "check_answers_min_count":
                check_answers_min_count = int(v)
            elif p == "check_answers_max_count":
                check_answers_max_count = int(v)

        random_answers = self.get_config_param_value("random_answers")
        if random_answers is not None and random_answers == "TRUE":
            random.shuffle(answers)
        for it in last_answers:
            answers.remove(it)
            answers.append(it)

        global_config = self.survey_model.get_config("global_config")
        for row in global_config:
            p = row["param"]
            v = row["value"]
            if check_answers_count is None and p == "check_answers_count":
                check_answers_count = int(v)
            elif check_answers_min_count is None and p == "check_answers_min_count":
                check_answers_min_count = int(v)
            elif check_answers_max_count is None and p == "check_answers_max_count":
                check_answers_max_count = int(v)

        if check_answers_count is not None and check_answers_count != -1:
            for it in sections:
                it["answers_group"]["check_answers_count"] = check_answers_count
        if check_answers_min_count is not None and check_answers_min_count != -1:
            for it in sections:
                it["answers_group"]["check_answers_min_count"] = check_answers_min_count
        if check_answers_max_count is not None and check_answers_max_count != -1:
            for it in sections:
                it["answers_group"]["check_answers_max_count"] = check_answers_max_count

        for it in answers:
            for s in sections:
                s["answers_group"]["answers"].append(it)

        return {
            "config_data": export_data,
            "client_data": client_data
        }

    def submit(self, data):
        self.survey_model.set_client_data(data)
        if self.check_screenout() is None:
            self.set_next_page_by_order()


class SurveyAgePageModel(SurveyPageModel):

    def export(self):
        current_page = self.survey_model.get_current_page()
        export_data = self.create_default_export()
        config_data = self.survey_model.get_current_config()

        section_data = {
            "id": current_page,
            "answers_group": {
                "type": "",
                "answers": []
            }
        }
        export_data["sections"].append(section_data)

        for row in config_data:
            p = row["param"]
            t = row["text"]
            v = row["value"]
            v2 = row["value_2"]
            if p == "answers_type":
                section_data["answers_group"]["type"] = v
            elif p == "answer":
                section_data["answers_group"]["answers"].append({
                    "id": f"{v}__{v2}",
                    "text": t,
                    "from": v,
                    "to": v2
                })

        return {
            "config_data": export_data,
            "client_data": self.survey_model.get_client_data()
        }

    def submit(self, data):
        self.survey_model.set_client_data(data)
        self.set_next_page_by_order()


class SurveySpendingPageModel(SurveyPageModel):

    def export(self):
        current_page = self.survey_model.get_current_page()
        export_data = self.create_default_export()
        config_data = self.survey_model.get_current_config()

        section_data = {
            "id": current_page,
            "answers_group": {
                "type": "",
                "answers": []
            }
        }
        export_data["sections"].append(section_data)

        for row in config_data:
            p = row["param"]
            t = row["text"]
            v = row["value"]
            v2 = row["value_2"]
            if p == "answers_type":
                section_data["answers_group"]["type"] = v
            elif p == "answer":
                section_data["answers_group"]["answers"].append({
                    "id": f"{v}__{v2}",
                    "text": t,
                    "from": v,
                    "to": v2
                })

        return {
            "config_data": export_data,
            "client_data": self.survey_model.get_client_data()
        }

    def submit(self, data):
        self.survey_model.set_client_data(data)
        self.set_next_page_by_order()


class SurveyPlayingPageModel(SurveyPageModel):

    def export(self):
        current_page = self.survey_model.get_current_page()
        export_data = self.create_default_export()
        config_data = self.survey_model.get_current_config()

        section_data = {
            "id": current_page,
            "answers_group": {
                "type": "",
                "answers": []
            }
        }
        export_data["sections"].append(section_data)

        for row in config_data:
            p = row["param"]
            t = row["text"]
            v = row["value"]
            v2 = row["value_2"]
            if p == "answers_type":
                section_data["answers_group"]["type"] = v
            elif p == "answer":
                section_data["answers_group"]["answers"].append({
                    "id": f"{v}__{v2}",
                    "text": t,
                    "from": v,
                    "to": v2
                })

        return {
            "config_data": export_data,
            "client_data": self.survey_model.get_client_data()
        }

    def submit(self, data):
        self.survey_model.set_client_data(data)
        self.set_next_page_by_order()


class SurveyLoyaltyPageModel(SurveyPageModel):

    def export(self):
        current_page = self.survey_model.get_current_page()
        export_data = self.create_default_export()
        config_data = self.survey_model.get_current_config()

        section_data = {
            "id": current_page,
            "answers_group": {
                "type": "",
                "answers": []
            }
        }
        export_data["sections"].append(section_data)

        for row in config_data:
            p = row["param"]
            t = row["text"]
            v = row["value"]
            v2 = row["value_2"]
            if p == "answers_type":
                section_data["answers_group"]["type"] = v
            elif p == "answer":
                section_data["answers_group"]["answers"].append({
                    "id": f"{v}__{v2}",
                    "text": t,
                    "from": v,
                    "to": v2
                })

        return {
            "config_data": export_data,
            "client_data": self.survey_model.get_client_data()
        }

    def submit(self, data):
        self.survey_model.set_client_data(data)
        self.set_next_page_by_order()


class SurveyItemsPageModel(SurveyPageModel):

    def init_server_data(self):
        config = self.survey_model.get_current_config()
        items = []
        items_optional = []
        optional_count = 0
        examples_count = 0
        for row in config:
            p = row["param"]
            v = row["value"]
            if p == "item":
                if row["value_2"] == "optional":
                    items_optional.append(v)
                else:
                    items.append(v)
            elif p == "optional_count":
                optional_count = int(v)
            elif p == "examples_count":
                examples_count = int(v)

        random.shuffle(items_optional)
        for i in range(min(optional_count, len(items_optional))):
            items.append(items_optional[i])

        random.shuffle(items)

        server_data = {
            "current": items[0],
            "items": []
        }

        for item in items:
            rows = self.get_apps(item)

            if len(rows) > 0:
                random.shuffle(rows)
                app_ids = []
                for i in range(min(examples_count, len(rows))):
                    app_ids.append(rows[i]["app_id_int"])
                server_data["items"].append({
                    "id": item,
                    "apps_ids_int": app_ids
                })

        self.survey_model.set_server_data(server_data)

    def export(self):

        screenout = self.check_screenout()
        if screenout is not None:
            return screenout

        config_data = self.survey_model.get_current_config()
        server_data = self.survey_model.get_server_data()
        export_data = self.create_default_export()

        answers = []
        for row in config_data:
            p = row["param"]
            t = row["text"]
            v = row["value"]
            if p == "answer":
                answers.append({
                    "id": v,
                    "text": t
                })

        for genre in server_data["items"]:

            section_data = {
                "id": genre["id"],
                "title": "",
                "answers_group": {
                    "type": "radio",
                    "answers": answers
                },
                "apps_data": []
            }
            export_data["sections"].append(section_data)

            for row in config_data:
                if row["param"] == "item" and row["value"] == genre["id"]:
                    section_data["title"] = row["text"]
                    break

            apps_ids_int_db = gb_session().conn().values_arr_to_db_in(genre["apps_ids_int"], int_values=True)
            rows_apps = gb_session().conn().select_all(f"""
            SELECT a.app_id_int, a.title, a.icon
              FROM scraped_data.apps a
             WHERE a.app_id_int IN ({apps_ids_int_db})
            """)
            for row in rows_apps:
                section_data["apps_data"].append({
                    "id": row["app_id_int"],
                    "title": row["title"],
                    "icon": row["icon"]
                })

        return {
            "config_data": export_data,
            "client_data": self.survey_model.get_client_data()
        }

    def submit(self, data):
        client_data = {}
        for k in data:
            client_data[k] = data[k]
        self.survey_model.set_client_data(client_data)

    def get_apps(self, item) -> []:
        return []


class SurveyGenreItemsPageModel(SurveyItemsPageModel):

    def submit(self, data):
        super(SurveyGenreItemsPageModel, self).submit(data)
        if self.check_screenout() is None:
            self.survey_model.set_current_page("genres_dcm")

    def get_apps(self, item) -> []:
        res = []
        survey_apps = self.survey_model.get_config("apps")
        for row in survey_apps:
            if row["genre"] == item:
                res.append({"app_id_int": row["id"]})

        return res


class SurveyTopicItemsPageModel(SurveyItemsPageModel):
    def submit(self, data):
        super(SurveyTopicItemsPageModel, self).submit(data)
        if self.check_screenout() is None:
            self.survey_model.set_current_page("topics_dcm")

    def get_apps(self, item) -> []:
        res = []
        survey_apps = self.survey_model.get_config("apps")
        for row in survey_apps:
            if row["topic"] == item:
                res.append({"app_id_int": row["id"]})

        return res


class SurveyConceptsDcmPageModel(SurveyPageModel):

    def init_server_data(self):
        concepts_dcm_config = self.survey_model.get_config("concepts_dcm_config")

        if concepts_dcm_config is None:
            self.set_next_page_by_order()
            return

        headers = concepts_dcm_config["headers"]
        features_1 = [x for x in concepts_dcm_config["features"] if x["feature"]["pool_id"] == "features_1"]
        features_2 = [x for x in concepts_dcm_config["features"] if x["feature"]["pool_id"] == "features_2"]

        if len(headers) == 0 or len(features_1) == 0 or len(features_2) == 0:
            self.set_next_page_by_order()
            return

        dcm = []
        uniques = []

        dcm_count = 5

        for i in range(5):
            loop_cnt = 999
            while True:

                loop_cnt -= 1
                if loop_cnt <= 0:
                    break

                dcm_cards = []

                for j in range(2):
                    random.shuffle(headers)
                    random.shuffle(features_1)
                    random.shuffle(features_2)

                    dcm_card = {
                        "id": f"{headers[0]['id']}__{features_1[0]['id']}__{features_2[0]['id']}",
                        "header": headers[0]["id"],
                        "feature_1": features_1[0]["id"],
                        "feature_2": features_2[0]["id"]
                    }

                    dcm_cards.append(dcm_card)

                def is_unique():
                    for unique_set in uniques:
                        exists = True
                        for i in range(2):
                            if dcm_cards[i]["id"] != unique_set[i]:
                                exists = False
                        if exists:
                            return False
                    uniques.append([x["id"] for x in dcm_cards])
                    return True

                if is_unique():
                    dcm.append({
                        "index": i + 1,
                        "choices": dcm_cards,
                    })
                    break

        if len(dcm) != dcm_count:
            self.set_next_page_by_order()
            return

        self.survey_model.set_server_data({
            "index": 1,
            "dcm": dcm
        })

    def export(self):
        server_data = self.survey_model.get_server_data()
        config_data = self.create_default_export()
        config_data["template"] = "concepts_dcm"
        concepts_dcm_config = self.survey_model.get_config("concepts_dcm_config")

        map_headers = {}
        for it in concepts_dcm_config["headers"]:
            map_headers[it["id"]] = it
        map_features = {}
        for it in concepts_dcm_config["features"]:
            map_features[it["id"]] = it

        index = server_data["index"]
        for it in server_data["dcm"]:
            if it["index"] == index:
                choices = []
                for choice in it["choices"]:
                    choices.append({
                        "id": choice["id"],
                        "title": map_headers[choice["header"]]["header"]["title"],
                        "description": map_headers[choice["header"]]["header"]["description"],
                        "feature_1": map_features[choice["feature_1"]]["feature"]["text"],
                        "feature_2": map_features[choice["feature_2"]]["feature"]["text"]
                    })
                config_data["sections"] = [{
                    "id": it["index"],
                    "answers_group": {
                        "type": "radio",
                        "concepts_dcm": {
                            "index": index,
                            "choices": choices
                        },
                    }
                }]

                break

        return {
            "config_data": config_data,
            "client_data": self.survey_model.get_client_data()
        }

    def submit(self, data):
        client_data = self.survey_model.get_client_data()
        if client_data is None:
            client_data = {}
        for k in data:
            client_data[k] = data[k]
        self.survey_model.set_client_data(client_data)
        server_data = self.survey_model.get_server_data()
        dcm_count = 5
        index = int(server_data["index"])
        if index < dcm_count:
            self.track_dcm_submit_time("concepts_dcm", index)
            index += 1
            server_data["index"] = index
            self.survey_model.set_server_data(server_data)
        else:
            self.set_next_page_by_order()


class SurveyGenresDcmPageModel(SurveyPageModel):
    def init_server_data(self):
        genres_client_data = self.survey_model.get_client_data("genres")
        # genres_client_data = {
        #     "genre__puzzle": "4",
        #     "genre__simulation": "4"
        # }
        genres_pool = []
        for genre in genres_client_data:
            if int(genres_client_data[genre]) == 4:
                genres_pool.append(genre)

        if len(genres_pool) == 0:
            self.set_next_page_by_order()
            return

        d = []
        core_cnt = 0

        rows_core = gb_session().conn().select_all("""
        SELECT p.tag_id_int, p.parent_genre_for_core_tag_id_int
          FROM app.def_sheet_platform_product p
         WHERE p.parent_genre_for_core_tag_id_int != 0
        """)

        cores_per_genre = {}
        for row in rows_core:
            genre_id = row["parent_genre_for_core_tag_id_int"]
            if genre_id not in cores_per_genre:
                cores_per_genre[genre_id] = []
            cores_per_genre[genre_id].append(row["tag_id_int"])

        for genre_id in cores_per_genre:
            d.append({
                "id": genre_id,
                "core_pool": cores_per_genre[genre_id]
            })
            core_cnt += len(cores_per_genre[genre_id])

        dcm = []
        uniques = []
        genres_cnt = min(2, len(d))
        dcm_count = int(self.survey_model.get_config_param_value("dcm_count", page="genres"))

        if core_cnt < 5:
            self.set_next_page_by_order()
            return

        for i in range(dcm_count):
            is_single = random.random() <= 0.25
            loop_cnt = 999
            while True:

                loop_cnt -= 1
                if loop_cnt <= 0:
                    break

                random.shuffle(d)
                rnd_genres = []
                rnd_cores = []

                chosen_genres = []

                final_genres_cnt = genres_cnt
                if is_single:
                    final_genres_cnt = 1
                for j in range(final_genres_cnt):
                    genre = d[j]
                    rnd_genres.append(genre["id"])
                    chosen_genres.append(genre)

                core_pools = []

                if len(chosen_genres) == 1:
                    p = chosen_genres[0]["core_pool"]
                    if len(p) >= 2:
                        core_pools.append(p)
                else:
                    for j in range(2):
                        p = chosen_genres[j]["core_pool"]
                        if len(p) >= 1:
                            core_pools.append(p)

                if len(core_pools) == 0:
                    continue

                if len(core_pools) == 1:
                    if len(core_pools[0]) <= 1:
                        continue
                    random.shuffle(core_pools[0])
                    rnd_cores.append(core_pools[0][0])
                    rnd_cores.append(core_pools[0][1])
                else:
                    for j in range(2):
                        random.shuffle(core_pools[j])
                        rnd_cores.append(core_pools[j][0])

                if len(rnd_cores) != 2:
                    continue

                def is_unique():
                    for unique_set in uniques:
                        t = True
                        for it in rnd_genres + rnd_cores:
                            t = t and it in unique_set
                            if not t:
                                break
                        if t:
                            return False
                    uniques.append(rnd_genres + rnd_cores)
                    return True

                if is_unique():
                    index = i + 1
                    dcm.append({
                        "id": index,
                        "genres": rnd_genres,
                        "choices": rnd_cores
                    })
                    break

        if len(dcm) != dcm_count:
            self.set_next_page_by_order()
            return

        self.survey_model.set_server_data({
            "index": 1,
            "dcm": dcm
        })

    def export(self):
        server_data = self.survey_model.get_server_data()
        config_data = self.create_default_export()
        config_data["template"] = "genres_dcm"

        index = server_data["index"]

        tags_ids = []

        for it in server_data["dcm"]:
            if it["id"] == index:
                tags_ids = tags_ids + it["choices"]
                for i in range(len(it["genres"])):
                    if it["genres"][i] != "none":
                        tags_ids.append(it["genres"][i])

        rows_adj = gb_session().conn().select_all(f"""
        SELECT p.tag_id_int, p.adj, p.node
          FROM app.def_sheet_platform_product p
         WHERE p.tag_id_int IN ({gb_session().conn().values_arr_to_db_in(tags_ids, int_values=True)})
        """)

        map_adj = {}
        for row in rows_adj:
            map_adj[row["tag_id_int"]] = row

        for it in server_data["dcm"]:
            if it["id"] == index:
                dcm = {
                    "id": it["id"],
                    "index": int(it["id"]),
                    "genres": [],
                    "choices": []
                }

                for core_id in it["choices"]:
                    dcm["choices"].append({
                        "id": core_id,
                        "text": map_adj[core_id]["adj"]
                    })
                for i in range(len(it["genres"])):
                    if it["genres"][i] == "none":
                        dcm["genres"].append("")
                    else:
                        dcm["genres"].append(map_adj[it["genres"][i]]["adj"] if i == 0 else map_adj[it["genres"][i]]["node"])

                config_data["sections"] = [{
                    "id": it["id"],
                    "answers_group": {
                        "type": "radio",
                        "genres_dcm": dcm
                    }
                }]

                break

        return {
            "config_data": config_data,
            "client_data": self.survey_model.get_client_data()
        }

    def submit(self, data):
        client_data = self.survey_model.get_client_data()
        if client_data is None:
            client_data = {}
        for k in data:
            client_data[k] = data[k]
        self.survey_model.set_client_data(client_data)
        server_data = self.survey_model.get_server_data()
        dcm_count = int(self.survey_model.get_config_param_value("dcm_count", page="genres"))
        index = int(server_data["index"])
        if index < dcm_count:
            self.track_dcm_submit_time("genres", index)
            index += 1
            server_data["index"] = index
            self.survey_model.set_server_data(server_data)
        else:
            self.set_next_page_by_order()


class SurveyTopicDcmPageModel(SurveyPageModel):
    def init_server_data(self):
        topics_client_data = self.survey_model.get_client_data("topics")
        topics_pool = []
        for topic in topics_client_data:
            if int(topics_client_data[topic]) == 4:
                topics_pool.append(topic)

        if len(topics_pool) == 0:
            self.set_next_page_by_order()
            return

        subcategories = [
            TagsConstants.SUBCATEGORY_ENTITIES_ID,
            TagsConstants.SUBCATEGORY_ERAS_ID,
            TagsConstants.SUBCATEGORY_FOCUS_ID,
            TagsConstants.SUBCATEGORY_DOMAINS_ID,
            TagsConstants.SUBCATEGORY_ENVIRONMENT_ID,
            TagsConstants.SUBCATEGORY_ROLES_ID
        ]

        rows_topics = gb_session().conn().select_all(f"""
        SELECT p.tag_id_int, p.subcategory_int
          FROM app.def_sheet_platform_product p
         WHERE p.subcategory_int IN ({gb_session().conn().values_arr_to_db_in(subcategories, int_values=True)})
        """)

        main_pool = {}
        for row in rows_topics:
            n = row["subcategory_int"]
            if n not in main_pool:
                main_pool[n] = []
            main_pool[n].append(row["tag_id_int"])

        dcm = []
        uniques = []
        items_cnt = min(2, len(topics_pool))
        dms_cnt = int(self.survey_model.get_config_param_value("dcm_count", page="topics"))

        for i in range(dms_cnt):

            loop_cnt = 999
            while True:

                loop_cnt -= 1
                if loop_cnt <= 0:
                    break

                rnd_topic = topics_pool[random.randrange(0, len(topics_pool))]
                choices = []

                rnd_main_pool = main_pool[TagsConstants.SUBCATEGORY_ENTITIES_ID] + main_pool[TagsConstants.SUBCATEGORY_ERAS_ID] + main_pool[TagsConstants.SUBCATEGORY_ENVIRONMENT_ID]
                rnd_main_id = rnd_main_pool[random.randrange(0, len(rnd_main_pool))]

                # Pool of Entities AND Eras AND Environments from Adj column
                if rnd_main_id in main_pool[TagsConstants.SUBCATEGORY_ENVIRONMENT_ID]:
                    rnd_1_pool = main_pool[TagsConstants.SUBCATEGORY_ENVIRONMENT_ID]
                    rnd_2_pool = main_pool[TagsConstants.SUBCATEGORY_ROLES_ID]
                # Pool of Roles OR Domains AND Focuses, where an ELEMENT is paired with ADJ_ELEMENT by
                # following rules:
                # - Entities with Roles
                # - Eras OR Environments with Domains AND Focuses
                else:
                    rnd_1_pool = main_pool[TagsConstants.SUBCATEGORY_ERAS_ID] + main_pool[TagsConstants.SUBCATEGORY_ENVIRONMENT_ID]
                    rnd_2_pool = main_pool[TagsConstants.SUBCATEGORY_DOMAINS_ID] + main_pool[TagsConstants.SUBCATEGORY_FOCUS_ID]

                for j in range(2):
                    rnd_1_id = rnd_1_pool[random.randrange(0, len(rnd_1_pool))]
                    rnd_2_id = rnd_2_pool[random.randrange(0, len(rnd_2_pool))]

                    choices.append({
                        "id": f"{i}__{j}",
                        "items": [rnd_1_id, rnd_2_id]
                    })

                def is_unique():
                    for unique_set in uniques:
                        t = True
                        t = t and rnd_topic in unique_set
                        for e1 in choices:
                            for e2 in e1["items"]:
                                t = t and e2 in unique_set
                        if t:
                            return False
                    arr = [rnd_topic]
                    for e1 in choices:
                        for e2 in e1["items"]:
                            arr.append(e2)
                    uniques.append(arr)
                    return True

                if is_unique():
                    index = i + 1
                    dcm.append({
                        "id": index,
                        "topic": rnd_topic,
                        "choices": choices
                    })
                    break

        self.survey_model.set_server_data({
            "index": 1,
            "dcm": dcm
        })

    def export(self):
        server_data = self.survey_model.get_server_data()
        config_data = self.create_default_export()
        config_data["template"] = "topics_dcm"
        index = server_data["index"]

        rows_tags = gb_session().conn().select_all("""
        SELECT p.tag_id_int, p.node, p.adj
          FROM app.def_sheet_platform_product p
        """)

        map_tags = {}
        for row in rows_tags:
            map_tags[row["tag_id_int"]] = row

        for it in server_data["dcm"]:
            if it["id"] == index:
                dcm = {
                    "id": it["id"],
                    "index": int(it["id"]),
                    "topic": map_tags[int(it["topic"])]["node"],
                    "choices": []
                }

                for choice in it["choices"]:
                    dcm_item = {
                        "id": choice["id"],
                        "items": []
                    }
                    for i in range(len(choice["items"])):
                        if i == 0:
                            dcm_item["items"].append(map_tags[choice["items"][i]]["adj"])
                        else:
                            dcm_item["items"].append(map_tags[choice["items"][i]]["node"])

                    dcm["choices"].append(dcm_item)

                config_data["sections"] = [{
                    "id": it["id"],
                    "answers_group": {
                        "type": "radio",
                        "topics_dcm": dcm
                    }
                }]

                break

        return {
            "config_data": config_data,
            "client_data": self.survey_model.get_client_data()
        }

    def submit(self, data):
        client_data = self.survey_model.get_client_data()
        if client_data is None:
            client_data = {}
        for k in data:
            client_data[k] = data[k]
        self.survey_model.set_client_data(client_data)
        server_data = self.survey_model.get_server_data()
        dcm_count = int(self.survey_model.get_config_param_value("dcm_count", page="topics"))
        index = int(server_data["index"])
        if index < dcm_count:
            self.track_dcm_submit_time("topics", index)
            index += 1
            server_data["index"] = index
            self.survey_model.set_server_data(server_data)
        else:
            self.set_next_page_by_order()


class SurveyEndPageModel(SurveyPageModel):

    def export(self):
        return get_export_for_end_page("completed", self.survey_model.get_config())


class SurveyCompetitorsPageModel(SurveyPageModel):
    def init_server_data(self):
        genres_client_data = self.survey_model.get_client_data("genres")
        topics_client_data = self.survey_model.get_client_data("topics")

        genres_pool = []
        if genres_client_data is not None:
            for k in genres_client_data:
                if int(genres_client_data[k]) == 4:
                    genres_pool.append(int(k))

        topics_pool = []
        if topics_client_data is not None:
            for k in topics_client_data:
                if int(topics_client_data[k]) == 4:
                    topics_pool.append(int(k))

        genre = None
        topic = None

        if len(genres_pool) > 0:
            genre = genres_pool[random.randrange(0, len(genres_pool))]
        if len(topics_pool) > 0:
            topic = topics_pool[random.randrange(0, len(topics_pool))]

        app_ids = []
        config_apps = self.survey_model.get_config("apps")
        for row in config_apps:
            if row["genre"] == genre and row["id"] not in app_ids:
                app_ids.append(row["id"])
            if row["topic"] == topic and row["id"] not in app_ids:
                app_ids.append(row["id"])

        if len(app_ids) == 0:
            self.set_next_page_by_order()
            return

        random.shuffle(app_ids)

        self.survey_model.set_server_data({
            "genre": genre,
            "topic": topic,
            "apps_ids_int": app_ids
        })

    def export(self):
        config_data = self.survey_model.get_current_config()
        server_data = self.survey_model.get_server_data()
        export_data = self.create_default_export()

        map_tag_id = TagsMapper.instance(gb_session().conn())

        genre = server_data["genre"]
        if genre is not None:
            export_data["title_genre"] = map_tag_id["i2n"][genre]

        topic = server_data["topic"]
        if topic is not None:
            export_data["title_topic"] = map_tag_id["i2n"][topic]

        answers = []
        for row in config_data:
            if row["param"] == "answer":
                answers.append({
                    "id": row["value"],
                    "text": row["text"]
                })

        sections = []
        apps_ids_int_db = gb_session().conn().values_arr_to_db_in(server_data["apps_ids_int"], int_values=True)
        rows = gb_session().conn().select_all(f"""
        SELECT a.app_id_int, a.title, a.icon
          FROM scraped_data.apps a
         WHERE a.app_id_int IN ({apps_ids_int_db})
        """)
        for row in rows:
            section = {
                "id": row["app_id_int"],
                "title": row["title"],
                "icon": row["icon"],
                "answers_group": {
                    "type": "radio",
                    "answers": answers
                }
            }

            sections.append(section)

        export_data["sections"] = sections

        return {
            "config_data": export_data,
            "client_data": self.survey_model.get_client_data()
        }

    def submit(self, data):
        client_data = {}
        for k in data:
            client_data[k] = data[k]
        self.survey_model.set_client_data(client_data)
        self.set_next_page_by_order()


class SurveyMultiplayerItemsPageModel(SurveyItemsPageModel):

    def submit(self, data):
        super(SurveyMultiplayerItemsPageModel, self).submit(data)
        self.set_next_page_by_order()

    def get_apps(self, item) -> []:
        res = []
        survey_apps = self.survey_model.get_config("apps")
        for row in survey_apps:
            if row["multiplayer"] == item:
                res.append({"app_id_int": row["id"]})

        return res


class SurveySpendingV2PageModel(SurveyPageModel):
    def export(self):
        export_data = self.create_default_export()
        config_data = self.survey_model.get_current_config()

        favorite_game = self.survey_model.get_client_data("favorite_game")
        if "___" in favorite_game["favorite_game"]:
            arr = favorite_game["favorite_game"].split("___")
            store = arr[0]
            app_id_in_store = arr[1]
            app_id_int = AppModel.get_app_id_int(conn=gb_session().conn(), app_id_in_store=app_id_in_store)
            if app_id_int is None or not ScraperModel.is_app_scraped(conn=gb_session().conn(), app_id_int=app_id_int):
                ScraperModel.scrap_app(conn=gb_session().conn(), app_id_in_store=app_id_in_store, store=store)
                app_id_int = AppModel.get_app_id_int(conn=gb_session().conn(), app_id_in_store=app_id_in_store)

            app_detail = AppModel.get_app_detail(conn=gb_session().conn(), app_id_int=app_id_int, remove_desc=True)

            export_data["title_app"] = {
                "icon": app_detail["icon"],
                "title": app_detail["title"]
            }

        section_spend = {
            "id": "spend",
            "answers_group": {
                "type": "",
                "answers": []
            }
        }

        section_time = {
            "id": "time",
            "answers_group": {
                "type": "",
                "answers": []
            }
        }

        export_data["sections"].append(section_spend)
        export_data["sections"].append(section_time)

        for row in config_data:
            p = row["param"]
            t = row["text"]
            v = row["value"]
            v2 = row["value_2"]
            v3 = row["value_3"]
            if p == "answers_type":
                section_spend["answers_group"]["type"] = v
                section_time["answers_group"]["type"] = v
            elif p == "subtitle":
                if v3 == "spend":
                    section_spend["title"] = t
                elif v3 == "time":
                    section_time["title"] = t
            elif p == "answer":
                if v3 == "spend":
                    section_spend["answers_group"]["answers"].append({
                        "id": f"{v}__{v2}",
                        "text": t,
                        "from": v,
                        "to": v2
                    })
                elif v3 == "time":
                    section_time["answers_group"]["answers"].append({
                        "id": v,
                        "text": t,
                    })

        return {
            "config_data": export_data,
            "client_data": self.survey_model.get_client_data()
        }

    def submit(self, data):
        self.survey_model.set_client_data(data)
        self.set_next_page_by_order()
