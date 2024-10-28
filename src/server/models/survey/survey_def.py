from time import sleep

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.dms.dms_model import DmsCache


class SurveyLangs:

    __langs_data = None

    @staticmethod
    def get(conn: DbConnection, dms_id: int = None, guid: str = None, sheet_id: str = None) -> {}:

        res = DmsCache.get_from_cache(conn, dms_id=dms_id, guid=guid)
        data = res["data"]

        if data is None:
            raise Exception(f"Dms file for langs not found, dms_id: {dms_id}")

        if sheet_id is not None:
            data = data[sheet_id]

        if SurveyLangs.__langs_data is None or res["is_new_version"]:
            langs = ['EN', 'DE']
            SurveyLangs.__langs_data = {}
            for l in langs:
                SurveyLangs.__langs_data[l] = {}
            for it in data:
                for l in langs:
                    if l in it:
                        SurveyLangs.__langs_data[l][it['ID']] = it[l]

        return SurveyLangs.__langs_data


class SurveyDef:
    survey_config_dms_id = 0
    initialized = False
    init_in_progress = False
    total_progress = 0

    groups = []
    groups_order = []
    id_cache = {}
    id_order = []

    @staticmethod
    def get_pages(def_id: str) -> int:
        d = SurveyDef.get(def_id)
        g = SurveyDef.private_get_group(d['group_id'])
        return g['pages']

    @staticmethod
    def is_same_group(def_id_1: str, def_id_2: str) -> bool:
        d1 = SurveyDef.get(def_id_1)
        d2 = SurveyDef.get(def_id_2)
        return d1['group_id'] == d2['group_id']

    @staticmethod
    def get_controller(def_id: str) -> str | None:
        d = SurveyDef.get(def_id)
        group = SurveyDef.private_get_group(d['group_id'])
        if 'controller' in group:
            return group['controller']
        return None

    @staticmethod
    def private_get_group(group_id: str):
        for group in SurveyDef.groups:
            if group['id'] == group_id:
                return group
        raise Exception(f"Group not found {group_id}")

    @staticmethod
    def private_get_next_group(def_id: str) -> str | None:
        d = SurveyDef.get(def_id)
        for i in range(len(SurveyDef.groups_order)):
            if SurveyDef.groups_order[i] == d['group_id']:
                if i + 1 < len(SurveyDef.groups_order):
                    return SurveyDef.groups_order[i + 1]
        return None

    @staticmethod
    def private_get_prev_group(def_id: str) -> str | None:
        d = SurveyDef.get(def_id)
        for i in range(len(SurveyDef.groups_order)):
            if SurveyDef.groups_order[i] == d['group_id']:
                if i > 0:
                    return SurveyDef.groups_order[i - 1]
        return None

    @staticmethod
    def get_first_child_for_next_group(def_id: str) -> str | None:
        next_group_id = SurveyDef.private_get_next_group(def_id)
        if next_group_id is not None:
            group = SurveyDef.private_get_group(next_group_id)
            return group['items'][0]['id']
        return None

    @staticmethod
    def get_last_child_for_prev_group(def_id: str) -> str | None:
        prev_group_id = SurveyDef.private_get_prev_group(def_id)
        if prev_group_id is not None:
            group = SurveyDef.private_get_group(prev_group_id)
            return group['items'][len(group['items']) - 1]['id']
        return None

    @staticmethod
    def is_first(def_id: str) -> bool:
        return SurveyDef.get_first() == def_id

    @staticmethod
    def get_first() -> str:
        return SurveyDef.id_order[0]

    @staticmethod
    def is_last(def_id: str) -> bool:
        survey_def = SurveyDef.get(def_id)
        if 'last' in survey_def:
            return survey_def['last']
        return SurveyDef.get_last() == def_id

    @staticmethod
    def is_disqualify_answer(def_id: str, question_id: str, response: str) -> bool:
        options = SurveyDef.get_options(def_id)
        survey_def = SurveyDef.get(def_id)
        for q in survey_def['questions']:
            for i in q['items']:
                if i['id'] == question_id:
                    for o in options:
                        if o['id'] == q['options']:
                            if 'items' in o:
                                for oi in o['items']:
                                    if oi['value'] == response:
                                        if 'disqualify' in oi:
                                            return True
        return False

    @staticmethod
    def get_last() -> str:
        return SurveyDef.id_order[len(SurveyDef.id_order) - 1]

    @staticmethod
    def get_next(def_id: str) -> str | None:
        if not SurveyDef.is_last(def_id):
            index = SurveyDef.id_order.index(def_id)
            return SurveyDef.id_order[index + 1]
        return None

    @staticmethod
    def get_prev(def_id: str) -> str | None:
        if not SurveyDef.is_first(def_id):
            index = SurveyDef.id_order.index(def_id)
            return SurveyDef.id_order[index - 1]
        return None

    @staticmethod
    def get(def_id: str):
        return SurveyDef.id_cache[def_id]

    @staticmethod
    def has(def_id: str) -> bool:
        return def_id in SurveyDef.id_cache

    @staticmethod
    def get_options(def_id: str):
        d = SurveyDef.get(def_id)
        group = SurveyDef.private_get_group(d['group_id'])
        return group['options']

    @staticmethod
    def get_questions(def_id: str):
        d = SurveyDef.get(def_id)
        return d['questions']

    @staticmethod
    def get_subgenre_id_for_genre_question(question_id: str):
        gr = SurveyDef.private_get_group("genre")
        d = SurveyDef.get("genre")
        for question in d['questions']:
            for item in question['items']:
                if item['id'] == question_id:
                    for subgenre in gr['items']:
                        if 'genre' in subgenre and subgenre['genre'] in item['genre']:
                            return subgenre['id']
        return None

    @staticmethod
    def get_competitor_ids_for_genre_question(question_id: str) -> []:
        res = []
        gr_g = SurveyDef.private_get_group("genre")
        gr_c = SurveyDef.private_get_group("competitors")
        for def_genre in gr_g['items']:
            for q in def_genre['questions']:
                for i in q['items']:
                    if i['id'] == question_id:
                        for c in gr_c['items']:
                            if c['genre'] in i['genre']:
                                res.append(c['id'])
        return res

    @staticmethod
    def get_routine_id_for_routine_question(option_id: str) -> str | None:
        options = SurveyDef.get_options('routine')
        for option in options:
            if option['id'] == 'routine':
                for item in option['items']:
                    if item['value'] == option_id:
                        if 'routine' in item:
                            if SurveyDef.has(item['routine']):
                                return item['routine']
        return None

    @staticmethod
    def get_progress(def_id: str) -> int:
        survey_def = SurveyDef.get(def_id)
        gr = SurveyDef.private_get_group(survey_def['group_id'])
        if 'pages' in gr:
            return gr['pages']
        else:
            return len(gr['items'])

    @staticmethod
    def init(conn: DbConnection, survey_config_dms_id: int, force: bool = False, skip_images: bool = False):
        try:
            SurveyDef.init_internal(conn, survey_config_dms_id, force, skip_images)
        except Exception as e:
            SurveyDef.init_in_progress = False
            SurveyDef.initialized = False
            raise e

    @staticmethod
    def init_internal(conn: DbConnection, survey_config_dms_id: int, force: bool = False, skip_images: bool = False):

        if SurveyDef.init_in_progress:
            cnt = 20
            while SurveyDef.init_in_progress and cnt > 0:
                cnt += 1
                sleep(1000)
            if SurveyDef.init_in_progress:
                raise Exception(f"Error initializing survey config")

        res = DmsCache.get_from_cache(conn, dms_id=survey_config_dms_id)
        data = res["data"]

        if data is None:
            raise Exception(f"Survey config not found, dms: {survey_config_dms_id}")

        if SurveyDef.initialized and not force and not res["is_new_version"]:
            return

        SurveyDef.init_in_progress = True
        SurveyDef.initialized = False
        SurveyDef.id_cache = {}
        SurveyDef.id_order = []
        SurveyDef.groups = []
        SurveyDef.total_progress = 0
        games = []
        app_ids = []

        for group_id in data['screens']:

            group = data['screens'][group_id]
            group['id'] = group_id
            SurveyDef.groups.append(group)

            if 'pages' not in group:
                SurveyDef.total_progress += len(group['items'])
            else:
                SurveyDef.total_progress += group['pages']

            if 'options' not in group:
                group['options'] = []

            options = group['options']
            if len(options) == 0:
                options.append({
                    "validation": {
                        "type": "none",
                    },
                })

            if len(options) == 1:
                if 'id' not in options[0]:
                    options[0]['id'] = 'default'

            for item in group['items']:
                item['group_id'] = group_id
                item_id = item['id']
                if item_id in SurveyDef.id_cache:
                    raise Exception(f"Duplicate id {item_id}")

                SurveyDef.id_cache[item_id] = item

                group_games = None
                if 'games' in item:
                    group_games = item['games']
                    for game in group_games:
                        if game not in games:
                            games.append(game)

                for question in item['questions']:
                    if group_games is not None:
                        for i in range(len(group_games)):
                            question['items'].append({
                                "id": str(i),
                                "games": [group_games[i]],
                                "text": f"<b>{group_games[i]}</b>"
                            })
                    if 'options' not in question:
                        question['options'] = "default"
                    for question_item in question['items']:
                        if 'games' in question_item:
                            for game in question_item['games']:
                                if game not in games:
                                    games.append(game)
                        if 'app_ids' in question_item:
                            app_ids += question_item['app_ids']

        SurveyDef.id_order = []
        SurveyDef.groups_order = []
        for it in data['order']:
            gr = SurveyDef.private_get_group(it)
            SurveyDef.groups_order.append(it)
            for item in gr['items']:
                SurveyDef.id_order.append(item['id'])

        # steam icons
        if not skip_images:
            conn = DbConnection()
            steam_game_link_base = 'https://store.steampowered.com/app/'
            steam_game_link_icon = 'https://cdn.akamai.steamstatic.com/steam/apps/'
            for game in games:
                row = conn.select_one_or_none("SELECT * FROM survey.def_survey_icons WHERE name = %s", [game])
                if row is None:
                    icon_url = ""
                    if game == 'Ghost of Tsushima':
                        icon_url = 'https://gepig.com/game_cover_460w/7200.jpg'
                    elif game == 'Fortnite':
                        icon_url = 'https://gigait.ir/wp-content/uploads/2020/03/2366.jpg'
                    elif game == 'Until Dawn':
                        icon_url = '/media/gembase/survey/games/until_dawn_a.jpg'
                    elif game == 'Call of Duty: Modern Warfare':
                        icon_url = '/media/gembase/survey/games/cod_mw2_a.jpg'
                    else:
                        pass
                    conn.query("""
                    INSERT INTO survey.def_survey_icons (name, icon_url) VALUES (%s, %s)
                    """, [game, icon_url])
                    break
            conn.commit(True)

        SurveyDef.init_in_progress = False
        SurveyDef.initialized = True
