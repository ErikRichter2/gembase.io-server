import json
import time
import random

from flask import request

from src.server.models.survey.survey_def import SurveyDef
from src.session.session import gb_session


class SurveyModel:
    def __init__(self, sid: int, guid: str, data):
        self.id = sid
        self.guid = guid
        self.data = data
        self.is_dirty = False
        self.total_p = SurveyDef.total_progress
        self.current_p = 0
        self.init_progress()

    @staticmethod
    def create_survey_model(respondent_guid: str, url_params: {}):
        row = gb_session().conn().select_one_or_none("""
        SELECT a.config_dms_id, a.id
          FROM app.surveys a,
               survey.survey_whitelist w
         WHERE a.id = w.survey_id 
           AND w.guid = %s
        """, [respondent_guid])

        if row is None:
            return None

        sid = gb_session().conn().insert("""
        INSERT INTO survey.survey (survey_id, ip_address, guid, params) 
        VALUES (%s, %s, %s, %s)
        """, [row['id'], request.remote_addr, respondent_guid, json.dumps(url_params)])

        SurveyDef.init(gb_session().conn(), row['config_dms_id'])

        survey_model = SurveyModel(sid, respondent_guid, None)
        survey_model.reset()

        return survey_model

    @staticmethod
    def load_survey_model(respondent_guid: str):

        row = gb_session().conn().select_one_or_none("""
        SELECT a.config_dms_id, s.id, s.data
          FROM app.surveys a,
               survey.survey s,
               survey.survey_whitelist w
         WHERE a.id = w.survey_id 
           AND a.id = s.survey_id
           AND s.guid = w.guid
           AND w.guid = %s
        """, [respondent_guid])

        if row is None:
            return None

        SurveyDef.init(gb_session().conn(), row['config_dms_id'])

        data = row['data']
        if data is not None:
            data = json.loads(data)

        return SurveyModel(row['id'], respondent_guid, data)

    def reset(self):
        self.data = {
            'current_id': SurveyDef.get_first(),
            'client_data': {},
            'server_data': {}
        }
        self.init_progress()
        self.dirty()

    def dirty(self):
        self.is_dirty = True

    def update(self):
        if self.is_dirty:
            gb_session().conn().query("UPDATE survey.survey SET data = %s WHERE id = %s", [json.dumps(self.data), self.id])
            self.is_dirty = False

    def export(self):
        model = SurveyControllerFactory.create(self.get_current_id(), self)
        export = model.export()
        export['stats_data'] = self.get_stats()

        if SurveyDef.is_last(self.get_current_id()):
            progress = {
                'current': 1,
                'total': 1
            }
        else:
            progress = {
                'current': self.get_current_progress(self.get_current_id()),
                'total': self.get_total_progress(self.get_current_id()),
            }

        export['progress_data'] = progress
        export['guid'] = self.guid

        self.update()
        return export

    def set_client_time_tracking(self, time_tracking):
        stats = self.get_stats()
        current_id = self.get_current_id()
        if 'client_time_tracking' not in stats:
            stats['client_time_tracking'] = {}
        if current_id not in stats['client_time_tracking']:
            stats['client_time_tracking'][current_id] = {}
        tracking_data = stats['client_time_tracking'][current_id]
        for it in time_tracking:
            if it not in tracking_data:
                tracking_data[it] = {}
            tracking_data[it] = time_tracking[it]
        self.set_stats(stats)

    def set_page(self, page: str):
        self.set_current_id(page)

    def set_dcm_data(self, modular_concepts_dcm: []):
        self.set_server_data("dcm", {
                'index': 0,
                'items': [],
            })
        self.set_server_data("custom_dcm", modular_concepts_dcm)
        controller = DcmSurveyController("dcm", self)
        controller.generate_dcm_choices_if_not_generated()
        self.dirty()
        self.update()

    def submit(self, client_data):
        self.track_time()

        # submit client data
        controller = SurveyControllerFactory.create(self.get_current_id(), self)
        controller.submit(client_data)

        # move to next
        controller.next()

    def next(self):
        controller = SurveyControllerFactory.create(self.get_current_id(), self)
        controller.next()

    def prev(self):
        controller = SurveyControllerFactory.create(self.get_current_id(), self)
        controller.prev()

    def get_stats(self):
        data = self.get_data()
        if 'stats_data' in data:
            return data['stats_data']
        return {
            'time': [],
            'progress': {}
        }

    def set_stats(self, stats_data):
        data = self.get_data()
        data['stats_data'] = stats_data
        self.dirty()

    def track_time(self, custom_id: str = None):
        stats = self.get_stats()
        current_time_data = None
        if custom_id is None:
            custom_id = self.get_current_id()
        for it in stats['time']:
            if it['id'] == custom_id:
                current_time_data = it
                break
        ts = time.time()
        if current_time_data is None:
            current_time_data = {
                'id': custom_id,
                'first': ts
            }
            stats['time'].append(current_time_data)
        current_time_data['last'] = ts
        self.set_stats(stats)

    def get_client_data(self, def_id: str):
        data = self.get_data()
        if 'client_data' in data:
            if def_id in data['client_data']:
                return data['client_data'][def_id]
        return {}

    def set_client_data(self, def_id: str, client_data):
        data = self.get_data()
        if client_data is None:
            if 'client_data' in data:
                if def_id in data['client_data']:
                    del data['client_data']
        else:
            if 'client_data' not in data:
                data['client_data'] = {}
            data['client_data'][def_id] = client_data
        self.data = data
        self.dirty()

    def delete_data(self, def_id: str):
        self.set_client_data(def_id, None)
        self.set_server_data(def_id, None)

    def get_server_data(self, def_id: str, default_none: bool = False):
        data = self.get_data()
        if 'server_data' in data:
            if def_id in data['server_data']:
                return data['server_data'][def_id]
        if default_none:
            return None
        return {}

    def set_server_data(self, def_id: str, server_data):
        data = self.get_data()
        if server_data is None:
            if 'server_data' in data:
                if def_id in data['server_data']:
                    del data['server_data'][def_id]
        else:
            if 'server_data' not in data:
                data['server_data'] = {}
            data['server_data'][def_id] = server_data
        self.data = data
        self.dirty()

    def get_current_id(self) -> str:
        return self.get_data()['current_id']

    def set_current_id(self, survey_id: str):
        self.get_data()['current_id'] = survey_id
        self.dirty()

    def get_data(self):
        if self.data is None:
            self.reset()
        return self.data

    def init_progress(self):
        self.current_p = 0
        self.total_p = SurveyDef.total_progress
        self.current_p = self.get_current_progress(self.get_current_id())
        self.total_p = self.get_total_progress(self.get_current_id())

    def get_progress(self, def_id: str):
        stats_data = self.get_stats()
        if 'progress' in stats_data:
            if def_id in stats_data['progress']:
                data = stats_data['progress'][def_id]
                return data[0], data[1]
        return self.current_p, self.total_p

    def get_current_progress(self, def_id: str):
        return self.get_progress(def_id)[0]

    def get_total_progress(self, def_id: str):
        return self.get_progress(def_id)[1]


class SurveyController:

    def __init__(self, def_id: str, model: SurveyModel):
        self.model = model
        self.def_id = def_id

    def submit(self, client_data):
        self.model.set_client_data(self.def_id, self.validate(client_data))
        self.model.current_p = self.model.get_current_progress(self.def_id) + 1

    def check_disqualify_answer(self) -> bool:
        client_data = self.model.get_client_data(self.def_id)
        for question_id in client_data:
            if SurveyDef.is_disqualify_answer(self.def_id, question_id, client_data[question_id]):
                return True
        return False

    def next(self):
        if self.check_disqualify_answer():
            self.next_with_def_id('disqualify')
        elif not SurveyDef.is_last(self.def_id):
            self.next_with_def_id(SurveyDef.get_next(self.def_id))

    def next_with_def_id(self, next_id: str):
        next_controller = SurveyControllerFactory.create(next_id, self.model)
        next_controller.activate(from_next=True)

    def prev(self):
        if not SurveyDef.is_first(self.def_id):
            self.prev_with_def_id(SurveyDef.get_prev(self.def_id))

    def prev_with_def_id(self, prev_id: str):
        prev_controller = SurveyControllerFactory.create(prev_id, self.model)
        prev_controller.activate(from_prev=True)

    def update_progress(self, from_next=None):
        stats_data = self.model.get_stats()
        if from_next:
            if 'progress' not in stats_data:
                stats_data['progress'] = {}
            if self.def_id not in stats_data['progress']:
                stats_data['progress'][self.def_id] = {}
            stats_data['progress'][self.def_id] = [self.model.current_p, self.model.total_p]
            self.model.set_stats(stats_data)
        else:
            self.model.current_p = self.model.get_current_progress(self.def_id)
            self.model.total_p = self.model.get_total_progress(self.def_id)

    def activate(self, from_next=None, from_prev=None):
        self.model.set_current_id(self.def_id)
        self.update_progress(from_next)

    def get_default_options(self):
        options = SurveyDef.get_options(self.def_id)
        for option in options:
            if option['id'] == 'default':
                return option
        raise Exception("Validation not found")

    def get_options_for_question(self, question_id: str):
        current_def = SurveyDef.get(self.def_id)
        options = SurveyDef.get_options(self.def_id)
        for question in current_def['questions']:
            for item in question['items']:
                if item['id'] == question_id:
                    for option in options:
                        if option['id'] == question['options']:
                            return option
        raise Exception("Options not found")

    def validate(self, client_data, custom_options = None):
        if len(client_data) == 0:
            default_options = self.get_default_options()
            validation_type = default_options['validation']['type']
            if validation_type == 'text' or validation_type == 'none':
                return client_data
            raise Exception("Empty response")

        for question_id in client_data:
            if custom_options is not None:
                options = custom_options
            else:
                options = self.get_options_for_question(question_id)

            validation = options['validation']
            if validation['type'] == 'radio':
                found_id = False
                for item in options['items']:
                    if item['value'] == client_data[question_id]:
                        found_id = True
                        break
                if not found_id:
                    raise Exception("Invalid answer")
            elif validation['type'] == 'check' or validation['type'] == 'check_with_other':
                if 'max_checked' in validation:
                    max_checked = validation['max_checked']
                    l = len(client_data[question_id])
                    if l > max_checked:
                        raise Exception(f"More than {max_checked} answers selected")
                for it in client_data[question_id]:
                    found_id = False
                    for item in options['items']:
                        if item['value'] == it:
                            found_id = True
                            break
                    if not found_id:
                        raise Exception("Invalid answer")
            elif validation['type'] == 'text':
                l = len(client_data[question_id])
                if l >= 50:
                    client_data[question_id] = client_data[question_id][0:50]
            elif validation['type'] == 'none':
                if len(client_data[question_id]) != 0:
                    raise Exception("Invalid answer")
            else:
                raise Exception(f"Unknown validation type {validation['type']}")

        return client_data

    def get_client_data_for_export(self):
        return self.model.get_client_data(self.def_id)

    def get_questions_for_export(self, client_data, options):
        export_questions = []

        for question in SurveyDef.get_questions(self.def_id):
            export_question = {
                "options": question["options"],
                "items": {},
                "apps": {},
            }
            if 'show_only_when_answer' in question:
                export_question['show_only_when_answer'] = question['show_only_when_answer']
            for item in question['items']:
                if item['id'] not in client_data:
                    for option in options:
                        if option['id'] == question['options']:
                            if option['validation']['type'] == 'check' or option['validation']['type'] == 'check_with_other':
                                client_data[item['id']] = []
                            elif option['validation']['type'] == 'text':
                                client_data[item['id']] = ""
                export_question['items'][item['id']] = item

                if 'games' in item:
                    export_question['apps'][item['id']] = []
                    for game_name in item['games']:
                        row = gb_session().conn().select_one_or_none("""
                            SELECT icon_url FROM survey.def_survey_icons WHERE name = %s
                            """, [game_name])
                        if row is not None:
                            export_question['apps'][item['id']].append({"icon": row['icon_url'], "title": game_name})
                        else:
                            export_question['apps'][item['id']].append({"icon": "", "title": game_name})

                if 'app_ids' in item:
                    export_question['apps'][item['id']] = []
                    for app_id in item['app_ids']:
                        row = gb_session().conn().select_one_or_none("""
                        SELECT icon, title FROM scraped_data.apps WHERE app_id = %s
                        """, [app_id])
                        if row is not None:
                            export_question['apps'][item['id']].append(row)

            export_questions.append(export_question)

        return export_questions

    def export(self):
        current_def = SurveyDef.id_cache[self.def_id]
        client_data = self.get_client_data_for_export()

        export_data = {}

        if 'title' in current_def:
            export_data['title'] = current_def['title']
        if 'randomized' in current_def:
            export_data['randomized'] = current_def['randomized']
        if 'view' in current_def:
            export_data['view'] = current_def['view']

        export_data['group_id'] = current_def['group_id']

        if self.def_id in SurveyDef.id_order:
            if SurveyDef.is_first(self.def_id):
                export_data['first'] = True
            if SurveyDef.is_last(self.def_id):
                export_data['last'] = True

        export_data['options'] = SurveyDef.get_options(self.def_id)

        if client_data is None:
            client_data = {}

        export_data['questions'] = self.get_questions_for_export(client_data, export_data['options'])
        export_data['client_data'] = client_data

        return export_data


class GenresSurveyController(SurveyController):

    genre_id = 'genre'
    pages_key = 'genre_pages'

    def get_client_data_for_export(self):
        export_data = {}
        client_data = self.model.get_client_data(self.genre_id)
        for q in SurveyDef.get_questions(self.def_id):
            for i in q['items']:
                if i['id'] in client_data:
                    export_data[i['id']] = client_data[i['id']]
        return export_data

    def submit(self, client_data):
        client_data = self.validate(client_data)
        client_data_current = self.model.get_client_data(self.genre_id)

        for it in client_data:
            client_data_current[it] = client_data[it]

        self.model.set_client_data(self.genre_id, client_data_current)

        if self.def_id == self.genre_id:
            pages_new = self.get_default_pages()

            for it in client_data:
                genre = SurveyDef.get_subgenre_id_for_genre_question(it)
                if int(client_data[it]) >= 3:
                    if genre is not None and genre not in pages_new:
                        pages_new.append(genre)
            for it in self.get_instance_pages():
                if it not in pages_new:
                    self.model.delete_data(it)

            server_data = self.model.get_server_data(self.genre_id)
            server_data[self.pages_key] = pages_new
            self.model.set_server_data(self.genre_id, server_data)

        competitor_pages_current = []
        competitor_pages_new = []
        server_data = self.model.get_server_data(self.genre_id)
        if 'competitor_pages' in server_data:
            competitor_pages_current = server_data['competitor_pages']

        for it in client_data_current:
            for competitor in SurveyDef.get_competitor_ids_for_genre_question(it):
                if int(client_data_current[it]) >= 3:
                    if competitor is not None and competitor not in competitor_pages_new:
                        competitor_pages_new.append(competitor)

        for it in competitor_pages_current:
            if it not in competitor_pages_new:
                self.model.delete_data(it)

        server_data['competitor_pages'] = competitor_pages_new
        self.model.set_server_data(self.genre_id, server_data)
        self.model.current_p = self.model.get_current_progress(self.def_id) + 1
        self.model.total_p = self.model.get_total_progress(self.genre_id) + len(self.get_instance_pages()) - 1 + len(competitor_pages_new)

    def next(self):
        pages = self.get_instance_pages()
        index = pages.index(self.def_id)
        if index + 1 < len(pages):
            self.def_id = pages[index + 1]
            self.model.set_current_id(self.def_id)
            self.update_progress(True)
        else:
            self.next_with_def_id(SurveyDef.get_first_child_for_next_group(self.def_id))

    def prev(self):
        pages = self.get_instance_pages()
        index = pages.index(self.def_id)
        if index - 1 >= 0:
            self.def_id = pages[index - 1]
            self.model.set_current_id(self.def_id)
            self.update_progress()
        else:
            super().prev()

    def activate(self, from_next=False, from_prev=False):
        pages = self.get_instance_pages()
        if from_prev:
            self.def_id = pages[len(pages) - 1]
        elif from_next:
            self.def_id = pages[0]

        super().activate(from_next=from_next, from_prev=from_prev)

    def get_default_pages(self):
        return [self.genre_id]

    def get_instance_pages(self):
        server_data = self.model.get_server_data(self.genre_id)
        if self.pages_key not in server_data:
            return self.get_default_pages()
        return server_data[self.pages_key]


class CompetitorsSurveyController(SurveyController):

    def next(self):
        pages = self.get_pages()
        index = pages.index(self.def_id)
        if index + 1 < len(pages):
            self.def_id = pages[index + 1]
            self.model.set_current_id(self.def_id)
            self.update_progress(True)
        else:
            self.next_with_def_id(SurveyDef.get_first_child_for_next_group(self.def_id))

    def prev(self):
        pages = self.get_pages()
        index = pages.index(self.def_id)
        if index - 1 >= 0:
            self.def_id = pages[index - 1]
            self.model.set_current_id(self.def_id)
            self.update_progress()
        else:
            self.prev_with_def_id(SurveyDef.get_last_child_for_prev_group(self.def_id))

    def activate(self, from_next=False, from_prev=False):
        pages = self.get_pages()

        if len(pages) == 0:
            if from_prev:
                self.prev_with_def_id(SurveyDef.get_last_child_for_prev_group(self.def_id))
            elif from_next:
                self.next_with_def_id(SurveyDef.get_first_child_for_next_group(self.def_id))
            else:
                raise Exception(f"Cannot default activate controller {self.def_id}")
        else:
            if from_prev:
                self.def_id = pages[len(pages) - 1]
            elif from_next:
                self.def_id = pages[0]

            super().activate(from_next=from_next, from_prev=from_prev)

    def get_pages(self) -> []:
        genre_server_data = self.model.get_server_data('genre')
        if 'competitor_pages' in genre_server_data:
            return genre_server_data['competitor_pages']
        return []


class RoutineSurveyController(SurveyController):

    routine_id = 'routine'

    def submit(self, client_data):
        super().submit(client_data)

        if self.def_id == self.routine_id:
            pages_new = self.get_default_pages()

            for it in client_data[self.routine_id]:
                routine = SurveyDef.get_routine_id_for_routine_question(it)
                if routine is not None and routine not in pages_new:
                    pages_new.append(routine)

            for it in self.get_pages():
                if it not in pages_new:
                    self.model.delete_data(it)

            self.model.total_p = self.model.get_total_progress(self.routine_id) + len(pages_new) - 1

            server_data = self.model.get_server_data(self.routine_id)
            server_data['routine_pages'] = pages_new
            self.model.set_server_data(self.routine_id, server_data)

    def next(self):
        pages = self.get_pages()
        index = pages.index(self.def_id)
        if index + 1 < len(pages):
            self.def_id = pages[index + 1]
            self.model.set_current_id(self.def_id)
            self.update_progress(True)
        else:
            self.next_with_def_id(SurveyDef.get_first_child_for_next_group(self.def_id))

    def prev(self):
        pages = self.get_pages()
        index = pages.index(self.def_id)
        if index - 1 >= 0:
            self.def_id = pages[index - 1]
            self.model.set_current_id(self.def_id)
            self.update_progress()
        else:
            super().prev()

    def activate(self, from_next=False, from_prev=False):
        pages = self.get_pages()
        if from_prev:
            self.def_id = pages[len(pages) - 1]
        elif from_next:
            self.def_id = pages[0]

        super().activate(from_next=from_next, from_prev=from_prev)

    def get_default_pages(self):
        return [self.routine_id]

    def get_pages(self):
        server_data = self.model.get_server_data(self.routine_id)
        if 'routine_pages' not in server_data:
            return self.get_default_pages()
        return server_data['routine_pages']


class ThemesSurveyController(SurveyController):

    themes_id = 'themes'
    topics_id = 'topics'

    current_theme_key = 'current_theme'
    topic_pages_key = 'topics'

    def submit(self, client_data):
        client_data = self.validate(client_data)

        server_data = self.model.get_server_data(self.themes_id)
        client_data_current = self.model.get_client_data(self.themes_id)

        # themes
        if self.def_id == self.themes_id:

            for it in list(client_data_current.keys()):
                if it not in client_data[self.themes_id]:
                    del client_data_current[it]

            for it in client_data[self.themes_id]:
                if it not in client_data_current:
                    client_data_current[it] = {}

            self.model.track_time()
            client_themes_checked = client_data[self.def_id]

            topic_pages = []
            if len(client_themes_checked) > 0:
                options = SurveyDef.get_options(self.topics_id)
                for it in client_themes_checked:
                    for option in options:
                        if it == option['id']:
                            topic_pages.append(it)

            for it in list(client_data_current.keys()):
                if it not in topic_pages:
                    del client_data_current[it]

            if len(topic_pages) > 0:
                server_data[self.topic_pages_key] = topic_pages
                if self.current_theme_key not in server_data or server_data[self.current_theme_key] not in topic_pages:
                    server_data[self.current_theme_key] = topic_pages[0]
            else:
                if self.current_theme_key in server_data:
                    del server_data[self.current_theme_key]
                if self.topic_pages_key in server_data:
                    del server_data[self.topic_pages_key]

            self.model.set_server_data(self.themes_id, server_data)
            self.model.total_p = self.model.get_total_progress(self.themes_id) + len(topic_pages)
        # topics
        else:
            current_theme = server_data[self.current_theme_key]
            client_data_current[current_theme] = client_data[current_theme]

        self.model.set_client_data(self.themes_id, client_data_current)
        self.model.current_p = self.model.get_current_progress(self.def_id) + 1

    def get_pages(self) -> []:
        server_data = self.model.get_server_data(self.themes_id)
        if self.topic_pages_key in server_data:
            topic_pages = server_data[self.topic_pages_key]
            return topic_pages
        return []

    def next(self):
        was_next_page = False
        topic_pages = self.get_pages()
        if len(topic_pages) > 0:
            server_data = self.model.get_server_data(self.themes_id)
            if self.def_id == self.themes_id:
                was_next_page = True
                server_data[self.current_theme_key] = topic_pages[0]
                self.model.set_server_data(self.themes_id, server_data)
                self.def_id = self.topics_id
                self.model.set_current_id(self.def_id)
                self.update_progress(True)
            else:
                index = topic_pages.index(server_data[self.current_theme_key])
                if index + 1 < len(topic_pages):
                    was_next_page = True
                    server_data[self.current_theme_key] = topic_pages[index + 1]
                    self.model.set_server_data(self.themes_id, server_data)
                    self.model.set_current_id(self.def_id)
                    self.update_progress(True)
        if not was_next_page:
            next_id = SurveyDef.get_next(self.topics_id)
            next_controller = SurveyControllerFactory.create(next_id, self.model)
            next_controller.activate(from_next=True)

    def prev(self):
        if self.def_id == self.themes_id:
            super().prev()
        else:
            server_data = self.model.get_server_data(self.themes_id)
            topic_pages = self.get_pages()
            if len(topic_pages) == 0 or topic_pages.index(server_data[self.current_theme_key]) == 0:
                self.def_id = self.themes_id
                self.model.set_current_id(self.def_id)
                self.update_progress()
            else:
                index = topic_pages.index(server_data[self.current_theme_key])
                server_data[self.current_theme_key] = topic_pages[index - 1]
                self.model.set_server_data(self.themes_id, server_data)

    def activate(self, from_next=False, from_prev=False):
        pages = self.get_pages()
        server_data = self.model.get_server_data(self.themes_id)
        if from_prev:
            if len(pages) == 0:
                self.def_id = self.themes_id
            else:
                self.def_id = self.topics_id
                server_data[self.current_theme_key] = pages[len(pages) - 1]
                self.model.set_server_data(self.themes_id, server_data)
        elif from_next:
            self.def_id = self.themes_id

        super().activate(from_next=from_next, from_prev=from_prev)

    def get_options_for_question(self, question_id: str):
        if self.model.get_current_id() == self.themes_id:
            return super().get_options_for_question(question_id)

        options = SurveyDef.get_options(self.model.get_current_id())
        for option in options:
            if option['id'] == question_id:
                return option
        raise Exception("Validation not found")

    def get_client_data_for_export(self):
        client_data = self.model.get_client_data(self.themes_id)
        export_data = {}
        if self.def_id == self.themes_id:
            export_data[self.themes_id] = []
            for it in client_data:
                export_data[self.themes_id].append(it)
        else:
            server_data = self.model.get_server_data(self.themes_id)
            current_theme = server_data[self.current_theme_key]
            export_data[current_theme] = client_data[current_theme]

        return export_data

    def export(self):
        if self.def_id == self.themes_id:
            return super().export()

        export_data = self.get_client_data_for_export()
        server_data = self.model.get_server_data(self.themes_id)
        current_def = SurveyDef.id_cache[self.topics_id]
        current_theme = server_data[self.current_theme_key]

        export_options = []
        for item in SurveyDef.get_options(self.topics_id):
            if item['id'] == current_theme:
                export_options.append(item)

        theme_loca = ""
        options = SurveyDef.get_options(self.themes_id)
        for it in options:
            if it['id'] == 'themes':
                for it2 in it['items']:
                    if it2['value'] == current_theme:
                        theme_loca = it2['text']
                        break
                break

        questions = {
            "options": current_theme,
            "items": {
                current_theme : {
                    "id": current_theme,
                    "text_params": {
                        "theme": theme_loca,
                    },
                    "text": current_def['questions'][0]['items'][0]['text']
                }
            }
        }

        export = {
            'client_data': export_data,
            'questions': [questions],
            'options': export_options,
        }

        if 'title' in current_def:
            export['title'] = current_def['title']
        if 'randomized' in current_def:
            export['randomized'] = current_def['randomized']
        if 'view' in current_def:
            export['view'] = current_def['view']

        return export


class DcmSurveyController(SurveyController):

    dcm_intro = 'dcm_intro'
    dcm = 'dcm'

    def create_custom_options(self):
        server_data = self.model.get_server_data(self.def_id)
        index = server_data['index']
        items = server_data['items']
        survey_def = SurveyDef.get(self.def_id)

        custom_dcm = self.model.get_server_data("custom_dcm", True)
        if custom_dcm is not None:
            survey_def = self.get_custom_def()

        options = self.get_options_for_question(self.dcm)

        custom_options = {}
        for key in options:
            custom_options[key] = options[key]

        def get_dcm_title_texts(title_id: str) -> (str, str):
            for it in survey_def['dcm_title']:
                if it['id'] == title_id:
                    return it['title'], it['desc']
            raise Exception(f"Dcm title not found for id {title_id}")

        def get_dcm_feature_text(feature_gr_id: str, feature_item_id: str):
            for gr in survey_def['dcm_features']:
                if gr['id'] == feature_gr_id:
                    for it in gr['items']:
                        if it['id'] == feature_item_id:
                            return it['text']
            raise Exception(f"Dcm feature not found for gr id {feature_gr_id}, item id {feature_item_id}")

        options_items = []
        for choice in items[index]['choices']:
            features = []
            for feature in choice['dcm_features']:
                features.append(get_dcm_feature_text(feature, choice['dcm_features'][feature]))
            texts = get_dcm_title_texts(choice['dcm_title'])
            options_items.append({
                "value": choice['id'],
                "dcm_title": texts[0],
                "text": texts[1],
                "dcm_features": features,
            })
        custom_options['items'] = options_items
        return custom_options

    def get_custom_def(self):
        survey_def = {
            "dcm_title": [],
            "dcm_features": [
                {"id": "1", "items": []},
                {"id": "2", "items": []}
            ]
        }
        survey_def["dcm_title"].append({
            "id": "none",
            "title": "NONE",
            "desc": "SV349",
            "is_none": True
        })
        custom_dcm = self.model.get_server_data("custom_dcm", True)
        for it in custom_dcm:
            if it["type"] == "title":
                survey_def["dcm_title"].append({
                    "id": it["item_id"],
                    "title": it["title"],
                    "desc": it["desc"],
                })
            elif it["type"] == "feature_1":
                survey_def["dcm_features"][0]["items"].append({
                    "id": it["item_id"],
                    "text": it["title"],
                })
            elif it["type"] == "feature_2":
                survey_def["dcm_features"][1]["items"].append({
                    "id": it["item_id"],
                    "text": it["title"],
                })

        return survey_def

    def create_dcm(self, generated: []):
        survey_def = SurveyDef.get(self.def_id)

        custom_dcm = self.model.get_server_data("custom_dcm", True)
        if custom_dcm is not None:
            survey_def = self.get_custom_def()

        data = {}

        while True:
            while True:
                data['t'] = random.choice(survey_def['dcm_title'])['id']
                if data['t'] != 'none':
                    break

            f1 = survey_def['dcm_features'][0]
            f2 = survey_def['dcm_features'][1]

            data['f1'] = f1['id']
            data['f2'] = f2['id']
            data['f1r'] = random.choice(f1['items'])['id']
            data['f2r'] = random.choice(f2['items'])['id']

            found = False
            for it in generated:
                if data['t'] == it['t'] and data['f1'] == it['f1'] and data['f2'] == it['f2'] and data['f1r'] == it['f1r'] and data['f2r'] == it['f2r']:
                    found = True
                    break

                if data['t'] == it['t'] and data['f1'] == it['f2'] and data['f2'] == it['f1'] and data['f1r'] == it['f2r'] and data['f2r'] == it['f1r']:
                    found = True
                    break

                if data['t'] == it['t'] and data['f2'] == it['f1'] and data['f1'] == it['f2'] and data['f2r'] == it['f1r'] and data['f1r'] == it['f2r']:
                    found = True
                    break

            if not found:
                generated.append(data)

                row_id = gb_session().conn().insert("""
                INSERT INTO survey.survey_dcm_choices_stats (survey_id, title_id, f_1_set_id, f_1_item_id, f_2_set_id, f_2_item_id) 
                VALUES (%s, %s, %s, %s, %s, %s) 
                """, [self.model.id, data['t'], data['f1'], data['f1r'], data['f2'], data['f2r']])

                return {
                    'db_id': row_id,
                    'dcm_title': data['t'],
                    'dcm_features': {
                        data['f1']: data['f1r'],
                        data['f2']: data['f2r'],
                    },
                }

        return None

    def submit(self, client_data):
        if self.def_id == self.dcm_intro:
            return super().submit(client_data)

        client_data = self.validate(client_data, self.create_custom_options())
        server_data = self.model.get_server_data(self.def_id)
        index = server_data['index']
        items = server_data['items']

        client_data_current = self.model.get_client_data(self.def_id)
        client_data_current[items[index]['id']] = client_data[self.dcm]
        self.model.set_client_data(self.def_id, client_data_current)
        self.model.current_p = self.model.get_current_progress(self.def_id) + 1

        updated = False
        db_id = items[index]['db_id']
        for choice in items[index]['choices']:
            if choice['id'] == client_data[self.dcm]:
                gb_session().conn().query("""
                UPDATE survey.survey_dcm_stats SET chosen = %s WHERE id = %s
                """, [choice['db_id'], db_id])
                updated = True
                break
        if not updated:
            raise Exception(f"Error when updating DCM response")

    def next(self):
        if self.def_id == self.dcm_intro:
            return super().next()

        server_data = self.model.get_server_data(self.def_id)
        index = server_data['index']
        items = server_data['items']

        if index + 1 < len(items):
            server_data['index'] = index + 1
            self.model.set_server_data(self.def_id, server_data)
            self.update_progress(True)
        else:
            super().next()

    def prev(self):
        if self.def_id == self.dcm_intro:
            return super().prev()

        server_data = self.model.get_server_data(self.def_id)
        index = server_data['index']

        if index - 1 >= 0:
            server_data['index'] = index - 1
            self.model.set_server_data(self.def_id, server_data)
            self.update_progress()
        else:
            super().prev()

    def generate_dcm_choices_if_not_generated(self):
        server_data = self.model.get_server_data(self.def_id, True)
        custom_dcm = self.model.get_server_data("custom_dcm", True)
        if server_data is None or custom_dcm is not None:
            server_data = {
                'index': 0,
                'items': [],
            }
            max_cnt = 8
            generated = []
            for i in range(max_cnt):
                choices = []
                for j in range(2):
                    dcm_data = self.create_dcm(generated)
                    dcm_data['id'] = f"{i}_{j}"
                    choices.append(dcm_data)
                if len(choices) != 2:
                    raise Exception(f"Generated other than {2} choices")
                row_id = gb_session().conn().insert("""
                INSERT INTO survey.survey_dcm_stats(survey_id, dcm_1, dcm_2, chosen) 
                VALUES (%s, %s, %s, %s)
                """, [self.model.id, choices[0]['db_id'], choices[1]['db_id'], 0])
                choices.append({
                    'id': 'none',
                    'db_id': -1,
                    'dcm_title': 'none',
                    'dcm_features': {}
                })
                server_data['items'].append({
                    'id': str(i),
                    'db_id': row_id,
                    'choices': choices
                })

            self.model.set_server_data(self.def_id, server_data)

    def activate(self, from_next=None, from_prev=None):
        if self.def_id == self.dcm:
            self.generate_dcm_choices_if_not_generated()

        if from_next:
            server_data = self.model.get_server_data(self.dcm, True)
            if server_data is not None:
                server_data['index'] = 0
                self.model.set_server_data(self.dcm, server_data)
        if from_prev:
            server_data = self.model.get_server_data(self.dcm)
            server_data['index'] = len(server_data['items'])
            self.model.set_server_data(self.def_id, server_data)

        super().activate(from_next=from_next, from_prev=from_prev)

    def get_client_data_for_export(self):
        if self.def_id == self.dcm_intro:
            return super().get_client_data_for_export()

        server_data = self.model.get_server_data(self.def_id)
        client_data = self.model.get_client_data(self.def_id)
        index = server_data['index']
        items = server_data['items']
        data_id = items[index]['id']
        if data_id in client_data:
            return {
                'dcm': client_data[data_id]
            }
        else:
            return {}

    def export(self):
        if self.def_id == self.dcm_intro:
            return super().export()

        survey_def = SurveyDef.get(self.def_id)

        client_data = self.get_client_data_for_export()
        options = [self.create_custom_options()]
        questions = self.get_questions_for_export(client_data, options)

        export_data = {
            'view': 'dcm',
            'client_data': client_data,
            'questions': questions,
            'options': options,
        }

        if 'title' in survey_def:
            title = survey_def['title']
            server_data = self.model.get_server_data(self.def_id)
            index = server_data['index']
            items = server_data['items']
            title = title.replace('{{step_current}}', str(index + 1)).replace('{{step_max}}', str(len(items)))
            export_data['title'] = title

        if 'randomized' in survey_def:
            export_data['randomized'] = survey_def['randomized']
        if 'view' in survey_def:
            export_data['view'] = survey_def['view']

        return export_data


class DevicesSurveyController(SurveyController):

    @staticmethod
    def is_pc_or_console(options, response) -> bool:
        for option_item in options:
            if option_item['value'] in response:
                if 'pc_or_console' in option_item and option_item['pc_or_console'] == '1':
                    return True
        return False

    def next(self):
        client_data = self.model.get_client_data(self.def_id)
        options = SurveyDef.get_options(self.def_id)[0]['items']
        if DevicesSurveyController.is_pc_or_console(options, client_data["devices"]):
            self.next_with_def_id(SurveyDef.get_next(self.def_id))
        else:
            self.next_with_def_id('disqualify')


class ConceptsSurveyController(SurveyController):
    concepts_intro = 'concepts_intro'
    concepts = 'concepts'

    def submit(self, client_data):
        if self.def_id == self.concepts_intro:
            return super().submit(client_data)

        client_data = self.validate(client_data)
        server_data = self.model.get_server_data(self.def_id)
        index = server_data['index']
        items = server_data['items']

        client_data_current = self.model.get_client_data(self.def_id)
        client_data_current[items[index]] = client_data[self.concepts]
        self.model.set_client_data(self.def_id, client_data_current)
        self.model.current_p = self.model.get_current_progress(self.def_id) + 1


    def next(self):
        if self.def_id == self.concepts_intro:
            return super().next()

        server_data = self.model.get_server_data(self.def_id)
        index = server_data['index']
        items = server_data['items']

        if index + 1 < len(items):
            server_data['index'] = index + 1
            self.model.set_server_data(self.def_id, server_data)
            self.update_progress(True)
        else:
            super().next()

    def prev(self):
        if self.def_id == self.concepts_intro:
            return super().prev()

        server_data = self.model.get_server_data(self.def_id)
        index = server_data['index']

        if index - 1 >= 0:
            server_data['index'] = index - 1
            self.model.set_server_data(self.def_id, server_data)
            self.update_progress()
        else:
            super().prev()

    def generate_concept_choices_if_not_generated(self):
        server_data = self.model.get_server_data(self.def_id, True)
        if server_data is None:
            arr = SurveyDef.get(self.concepts)['concepts_data']
            items_m = []
            items_o = []
            for it in arr:
                if 'mandatory' in it and it['mandatory'] == '1':
                    items_m.append(it['id'])
            for it in arr:
                if it['id'] not in items_m:
                    items_o.append(it['id'])
            random.shuffle(items_o)
            d = items_o[:2] + items_m
            random.shuffle(d)
            server_data = {
                'index': 0,
                'items': [it for it in d],
            }
            self.model.set_server_data(self.def_id, server_data)

    def activate(self, from_next=None, from_prev=None):
        if self.def_id == self.concepts:
            self.generate_concept_choices_if_not_generated()

        if from_next:
            server_data = self.model.get_server_data(self.concepts, True)
            if server_data is not None:
                server_data['index'] = 0
                self.model.set_server_data(self.concepts, server_data)
        if from_prev:
            server_data = self.model.get_server_data(self.concepts)
            server_data['index'] = len(server_data['items'])
            self.model.set_server_data(self.def_id, server_data)

        super().activate(from_next=from_next, from_prev=from_prev)

    def get_client_data_for_export(self):
        if self.def_id == self.concepts_intro:
            return super().get_client_data_for_export()

        server_data = self.model.get_server_data(self.def_id)
        client_data = self.model.get_client_data(self.def_id)
        index = server_data['index']
        items = server_data['items']
        if items[index] in client_data:
            return {
                items[index]: client_data[items[index]]
            }
        else:
            return {}

    def export(self):
        if self.def_id == self.concepts_intro:
            return super().export()

        survey_def = SurveyDef.get(self.def_id)
        server_data = self.model.get_server_data(self.def_id)
        client_data = self.get_client_data_for_export()
        options = SurveyDef.get_options(self.def_id)
        questions = self.get_questions_for_export(client_data, options)
        index = server_data['index']
        items = server_data['items']

        concept_data = {}
        for it in SurveyDef.get(self.concepts)['concepts_data']:
            if it['id'] == items[index]:
                concept_data = it
                break

        export_data = {
            'view': 'concepts',
            'concept_data': concept_data,
            'client_data': client_data,
            'questions': questions,
            'options': options,
        }

        if 'title' in survey_def:
            title = survey_def['title']
            server_data = self.model.get_server_data(self.def_id)
            index = server_data['index']
            items = server_data['items']
            title = title.replace('{{step_current}}', str(index + 1)).replace('{{step_max}}', str(len(items)))
            export_data['title'] = title

        if 'randomized' in survey_def:
            export_data['randomized'] = survey_def['randomized']
        if 'view' in survey_def:
            export_data['view'] = survey_def['view']

        return export_data


class SurveyControllerFactory:
    @staticmethod
    def create(def_id: str, model: SurveyModel):
        controller = SurveyDef.get_controller(def_id)
        if controller is None:
            return SurveyController(def_id, model)
        elif controller == 'genre':
            return GenresSurveyController(def_id, model)
        elif controller == 'competitors':
            return CompetitorsSurveyController(def_id, model)
        elif controller == 'routine':
            return RoutineSurveyController(def_id, model)
        elif controller == 'themes':
            return ThemesSurveyController(def_id, model)
        elif controller == 'dcm':
            return DcmSurveyController(def_id, model)
        elif controller == 'devices':
            return DevicesSurveyController(def_id, model)
        elif controller == 'concepts':
            return ConceptsSurveyController(def_id, model)
        else:
            raise Exception(f"Unknown survey controller type {controller} for id {def_id}")
