import json
import random
import pandas as pd
import numpy as np
import math

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.environment.runtime_constants import rr
from src.server.models.survey import survey_utils
from archive.calculate_dcm_themes_overlap import calculate_dcm_themes_overlap
from src.server.models.survey.survey_constants import sc
from src import external_api
from src.server.models.dms.dms_model import DmsModel
from src.utils.gembase_utils import GembaseUtils

ID_TO_CONFIG = 'id_to_config'


def load_definitions_from_dms(conn: DbConnection, guid: str) -> {}:
    return DmsModel.get_dms_data_to_json(conn, guid=f"{guid}__def")


def load_definitions_from_sheet():
    config_sheet = '1WjewP1nHlMcHSbFjOziIqyO4yDhg8cOv3_iRlOhAE_4'
    survey_def_sheet = '1cESlSLy0Fhn0wNpJcmx7AUcxMQX6x7CXHIdyWx36tYo'

    id_to_config = {
        sc.THEMES: {},
        sc.TOPICS: {},
        sc.BEHAVIORS: {},
        sc.NEEDS: {},
        sc.ROUTINE: {},
        sc.MOVIES: {},
        sc.HOBBIES: {},
        sc.SOCIALS: {}
    }

    for it in external_api.read_sheet(survey_def_sheet, sc.THEMES, True):
        id_to_config[sc.THEMES][it['id']] = it['id_config']
    for it in external_api.read_sheet(survey_def_sheet, sc.TOPICS, True):
        id_to_config[sc.TOPICS][f"{it['theme']}__{it['id']}"] = it['id_config']
    for it in external_api.read_sheet(survey_def_sheet, sc.BEHAVIORS, True):
        id_to_config[sc.BEHAVIORS][it['id']] = it['id_config']
    for it in external_api.read_sheet(survey_def_sheet, sc.NEEDS, True):
        id_to_config[sc.NEEDS][it['id']] = it['id_config']
    for it in external_api.read_sheet(survey_def_sheet, sc.ROUTINE, True):
        id_to_config[sc.ROUTINE][it['id']] = it['id_config']
    for it in external_api.read_sheet(survey_def_sheet, sc.MOVIES, True):
        id_to_config[sc.MOVIES][it['id']] = it['id_config']
    for it in external_api.read_sheet(survey_def_sheet, sc.HOBBIES, True):
        id_to_config[sc.HOBBIES][it['id']] = it['id_config']
    for it in external_api.read_sheet(survey_def_sheet, sc.SOCIALS, True):
        id_to_config[sc.SOCIALS][it['id']] = it['id_config']

    return {
        ID_TO_CONFIG: id_to_config,
        sc.AGE: external_api.read_sheet(survey_def_sheet, sc.AGE, True),
        sc.CHARTS: external_api.read_sheet(config_sheet, sc.CHARTS, True),
        sc.GENRES: external_api.read_sheet(survey_def_sheet, sc.GENRES, True),
        sc.GENRES_SUB: external_api.read_sheet(survey_def_sheet, sc.GENRES_SUB, True),
        sc.THEMES: external_api.read_sheet(config_sheet, sc.THEMES, True),
        sc.TOPICS: external_api.read_sheet(config_sheet, sc.TOPICS, True),
        sc.BEHAVIORS: external_api.read_sheet(config_sheet, sc.BEHAVIORS, True),
        sc.NEEDS: external_api.read_sheet(config_sheet, sc.NEEDS, True),
        sc.COMPETITORS: external_api.read_sheet(survey_def_sheet, sc.COMPETITORS, True),
        sc.ROUTINE: external_api.read_sheet(config_sheet, sc.ROUTINE, True),
        sc.MOVIES: external_api.read_sheet(config_sheet, sc.MOVIES, True),
        sc.SOCIALS: external_api.read_sheet(config_sheet, sc.SOCIALS, True),
        sc.HOBBIES: external_api.read_sheet(config_sheet, sc.HOBBIES, True),
        sc.DEVICES: external_api.read_sheet(survey_def_sheet, sc.DEVICES, True),
        sc.SPENDING: external_api.read_sheet(survey_def_sheet, sc.SPENDING, True),
        sc.ROLE: external_api.read_sheet(survey_def_sheet, sc.ROLE, True),
        sc.PLAYING: external_api.read_sheet(config_sheet, sc.PLAYING, True),
        sc.CONCEPTS: external_api.read_sheet(survey_def_sheet, sc.CONCEPTS, True),
        sc.SPENDING_GROUPS: external_api.read_sheet(survey_def_sheet, sc.SPENDING_GROUPS, True),
        sc.DCM_TITLE: external_api.read_sheet(survey_def_sheet, sc.DCM_TITLE, True),
        sc.DCM_FEATURES: external_api.read_sheet(survey_def_sheet, sc.DCM_FEATURES, True)
    }


def get_duration(st):
  min_t = -1
  max_t = -1
  for it in st:
    if min_t == -1 or min_t > it['first']:
      min_t = it['first']
    if max_t == -1 or max_t < it['last']:
      max_t = it['last']

  return round(max_t - min_t)


def process_survey_panel_data(guid, cd, url_params, total_time, survey_def):
    def get_age(id):
        for it in survey_def[sc.AGE]:
            if int(it['id']) == int(id):
                return int(it['from']), int(it['to'])
        raise Exception(f"Age id not found {id}")

    def get_def_from_id(def_data, id):
        for it in def_data:
            if it['id'] == id:
                return it

    df = {
        'guid': guid,
        'cint_id': '',
        'time': total_time
    }

    # age
    df['age'] = np.NaN
    df['age_url'] = np.NaN
    if 'age' in url_params:
        df['age_url'] = int(url_params['age'])
    age_from, age_to = get_age(cd['age']['1'])
    if age_from <= df['age_url'] <= age_to:
        df['age'] = df['age_url']
    else:
        df['age'] = round((age_to + age_from) / 2)

    # gender
    df['gender'] = cd['gender']['1']
    df['gender_url'] = np.NaN
    if 's' in url_params:
        if url_params['s'] == '1':
            df['gender_url'] = 'm'
        elif url_params['s'] == '2':
            df['gender_url'] = 'f'
    if df['gender'] == 'o':
        df['gender'] = np.NaN

    # spending
    df['spending'] = cd['spending']['spending']
    df['spending__avg'] = np.NaN
    for it in survey_def[sc.SPENDING]:
        if it['id'] == df['spending']:
            if it['from'] == 0:
                df['spending__avg'] = 0
            else:
                df['spending__avg'] = round((int(it['to']) + int(it['from'])) / 2, 2)
            break

    # playing
    df['playing'] = cd['playing']['playing']
    df['playing__avg'] = np.NaN
    for it in survey_def[sc.PLAYING]:
        if it['id'] == df['playing']:
            if it['from'] == -1:
                break
            if it['from'] == 0:
                df['playing__avg'] = 0
            else:
                df['playing__avg'] = round((int(it['to']) + int(it['from'])) / 2, 2)
            break

    # genres
    def get_genre_id(survey_id: str) -> str:
        for it in survey_def[sc.GENRES]:
            if it['survey_id'] == survey_id:
                return it['id']
    for it in survey_def[sc.GENRES]:
        df[f"{sc.get_prefix(sc.GENRES)}{it['id']}"] = 0
    for k in cd['genre']:
        genre_id = get_genre_id(k)
        col = f"{sc.get_prefix(sc.GENRES)}{genre_id}"
        if col in df:
            df[col] = round(int(cd['genre'][k]) / 4, 2)

    # genres sub
    def get_subgenre_id(survey_id: str) -> str:
        for it in survey_def[sc.GENRES_SUB]:
            if it['survey_id'] == survey_id:
                return it['id']
    for it in survey_def[sc.GENRES_SUB]:
        df[f"{sc.get_prefix(sc.GENRES_SUB)}{it['id']}"] = np.NaN
    for k in cd['genre']:
        subgenre_id = get_subgenre_id(k)
        col = f"{sc.get_prefix(sc.GENRES_SUB)}{subgenre_id}"
        if col in df:
            df[col] = round(int(cd['genre'][k]) / 4, 2)


    # themes, topics
    for it in survey_def[ID_TO_CONFIG][sc.THEMES]:
        df[f"{sc.get_prefix(sc.THEMES)}{survey_def[ID_TO_CONFIG][sc.THEMES][it]}"] = 0
    for it in survey_def[ID_TO_CONFIG][sc.TOPICS]:
        df[f"{sc.get_prefix(sc.TOPICS)}{survey_def[ID_TO_CONFIG][sc.TOPICS][it]}"] = np.NaN
    for o in cd['themes']:
        theme_id = survey_def[ID_TO_CONFIG][sc.THEMES][o]
        topic_id = survey_def[ID_TO_CONFIG][sc.TOPICS][f"{o}__{cd['themes'][o]}"]
        df[f"{sc.get_prefix(sc.THEMES)}{theme_id}"] = 1
        for it in survey_def[sc.TOPICS]:
            if it['theme'] == theme_id:
                df[f"{sc.get_prefix(sc.TOPICS)}{it['id']}"] = 0
        df[f"{sc.get_prefix(sc.TOPICS)}{topic_id}"] = 1


    # needs
    for it in survey_def[ID_TO_CONFIG][sc.NEEDS]:
        df[f"{sc.get_prefix(sc.NEEDS)}{survey_def[ID_TO_CONFIG][sc.NEEDS][it]}"] = np.NaN
    for gr in ['needs_1', 'needs_2']:
        for k in cd[gr]:
            need_id = survey_def[ID_TO_CONFIG][sc.NEEDS][k]
            df[f"{sc.get_prefix(sc.NEEDS)}{need_id}"] = round((int(cd[gr][k]) - 1) / 2, 2)

    # behaviours
    for it in survey_def[ID_TO_CONFIG][sc.BEHAVIORS]:
        df[f"{sc.get_prefix(sc.BEHAVIORS)}{survey_def[ID_TO_CONFIG][sc.BEHAVIORS][it]}"] = np.NaN
    for gr in ['behaviours_1', 'behaviours_2']:
        for k in cd[gr]:
            behaviour_id = survey_def[ID_TO_CONFIG][sc.BEHAVIORS][k]
            df[f"{sc.get_prefix(sc.BEHAVIORS)}{behaviour_id}"] = round((int(cd[gr][k]) - 1) / 2, 2)

    # routine, movies, hobbies, socials
    for i in [
        [sc.ROUTINE, sc.ROUTINE_OPEN],
        [sc.MOVIES, sc.MOVIES_OPEN],
        [sc.HOBBIES, sc.HOBBIES_OPEN],
        [sc.SOCIALS, sc.SOCIALS_OPEN]
    ]:
        data_id = i[0]
        col_open = i[1]
        col_prefix = sc.get_prefix(data_id)
        df[col_open] = ''
        for it in survey_def[ID_TO_CONFIG][data_id]:
            df[f"{col_prefix}{survey_def[ID_TO_CONFIG][data_id][it]}"] = np.NaN
        if data_id in cd:
            df[col_open] = cd[i[0]]['other']
            for it in survey_def[ID_TO_CONFIG][data_id]:
                df[f"{col_prefix}{survey_def[ID_TO_CONFIG][data_id][it]}"] = 0
            for k in cd[data_id][data_id]:
                df[f"{col_prefix}{survey_def[ID_TO_CONFIG][data_id][k]}"] = 1

    # role
    df['role'] = int(cd['role']['1'])

    # devices
    for it in survey_def[sc.DEVICES]:
        df[f"{sc.get_prefix(sc.DEVICES)}{it['col_name']}"] = 0
    for k in cd['devices']['devices']:
        d = get_def_from_id(survey_def[sc.DEVICES], k)
        df[f"{sc.get_prefix(sc.DEVICES)}{d['col_name']}"] = 1

    # best game
    df['best_game'] = survey_utils.normalize_game_name(cd['favorite']['1'])

    # single / multi
    df['single_player'] = round(int(cd['playstyle']['s']) / 4, 2)
    df['multi_player'] = round(int(cd['playstyle']['m']) / 4, 2)

    # slavic theme
    df['slavic_theme'] = int(cd['slavic_rus']['slavic_rus'])
    df['slavic_theme_open'] = cd['slavic_rus']['open']

    # concepts
    for it in survey_def[sc.CONCEPTS]:
        df[f"{sc.get_prefix(sc.CONCEPTS)}{it['col_name']}"] = np.NaN
    for k in cd['concepts']:
        d = get_def_from_id(survey_def[sc.CONCEPTS], k)
        df[f"{sc.get_prefix(sc.CONCEPTS)}{d['col_name']}"] = round((int(cd['concepts'][k]) - 1) / 4, 2)

    # competitors
    def get_competitor_col_name(id, genre):
        for it in survey_def[sc.COMPETITORS]:
            if it['id'] == id and it['genre'] == genre:
                return it['col_name']
        raise Exception(f"Topic not found id {id}, theme {genre}")

    for it in survey_def[sc.COMPETITORS]:
        df[f"{sc.get_prefix(sc.COMPETITORS)}{it['col_name']}"] = np.NaN
    for def_id in cd:
        if 'c__' in def_id:
            for k in cd[def_id]:
                col_name = get_competitor_col_name(k, def_id.replace("c__", ""))
                df[f"{sc.get_prefix(sc.COMPETITORS)}{col_name}"] = round((int(cd[def_id][k]) - 1) / 4, 2)

    return df


features_cache = {}


def process_survey_dcm_data(r, survey_def):
    def convert_id(survey_id: str, gr: str) -> str:
        if survey_id == '0':
            return survey_id
        col = f"g{gr}__id{survey_id}"
        if col in features_cache:
            return features_cache[col]
        else:
            for d in survey_def[sc.DCM_FEATURES]:
                if d['survey_id'] == survey_id and d['group'] == gr:
                    features_cache[col] = d['id']
                    return d['id']
        raise Exception(f"Feature not found, id: {survey_id}, gr: {gr}")

    df = r

    df['f1'] = convert_id(df['f1'], '1')
    df['f2'] = convert_id(df['f2'], '2')

    return df


def panel_data_fix(panel_df: pd.DataFrame, survey_guid) -> pd.DataFrame:
    if survey_guid == '57a1544a-ed69-4886-a901-5dbc5bac868c':

        df_fixed = pd.DataFrame(panel_df)
        tcols = []
        for col in df_fixed.columns:
            if sc.THEMES in col:
                tcols.append(col)
        df_tmp = df_fixed[tcols].sum(axis=1) == 2
        df_fixed.loc[df_fixed.index[df_tmp], 'themes__history'] = 1

        puzzles_subs = ['genres_sub__puzzle__hidden', 'genres_sub__puzzle__action', 'genres_sub__puzzle__environment']

        def generate_history_topics(r):
            if r[f"{sc.get_prefix(sc.THEMES)}history"] == 1:
                w = [0.45, 0.8, 0.95, 1]
                v = ['history__medieval', 'history__ancient', 'history__industrial', 'history__modern']
                rnd = random.random()
                for i in range(len(w)):
                    r[f"{sc.get_prefix(sc.TOPICS)}{v[i]}"] = 0
                for i in range(len(w)):
                    if rnd <= w[i]:
                        r[f"{sc.get_prefix(sc.TOPICS)}{v[i]}"] = 1
                        return r
            return r

        def generate_puzzles_competitors(r):
            if r[f"{sc.get_prefix(sc.GENRES)}puzzle"] >= 0.75:
                w = [[0.05, 0.15, 0.30, 0.65, 1],
                     [0.10, 0.20, 0.40, 0.75, 1],
                     [0.20, 0.30, 0.50, 0.80, 1],
                     [0.30, 0.40, 0.60, 0.85, 1],
                     [0.40, 0.50, 0.70, 0.90, 1],
                     [0.50, 0.60, 0.80, 0.95, 1]]
                v = ['shadow_of_the_tomb_raider', 'portal_2', 'inside', 'valiant_hearts', 'hidden_folks', 'wind_peaks']
                for i in range(len(v)):
                    rnd = random.random()
                    for j in range(len(w[i])):
                        if rnd <= w[i][j]:
                            r[f"{sc.get_prefix(sc.COMPETITORS)}{v[i]}"] = 0.25 * j
                            break
            return r

        def generate_puzzles_sub(r):
            if r[f"{sc.get_prefix(sc.GENRES)}puzzle"] >= 0.75:
                v1 = r[puzzles_subs[0]]
                v2 = r[puzzles_subs[1]]
                v3 = r[puzzles_subs[2]]
                if math.isnan(v1) and math.isnan(v2) and math.isnan(v3):
                    age = r['age']
                    age = round(age / 10)
                    age_from = age * 10
                    age_to = age_from + 10
                    gender = r['gender']
                    for i in range(len(puzzles_subs)):
                        df_tmp = df_fixed[(df_fixed['gender'] == gender) & (df_fixed['age'] >= age_from) & (
                                    df_fixed['age'] <= age_to)]
                        val_cnt = df_tmp[puzzles_subs[i]].value_counts(normalize=True)
                        rnd = random.random()
                        sum_val = 0
                        for val, cnt in val_cnt.iteritems():
                            sum_val += cnt
                            if rnd <= sum_val:
                                r[puzzles_subs[i]] = val
                                break
            return r

        random.seed('57a1544a-ed69-4886-a901-5dbc5bac868c')
        df_fixed = df_fixed.apply(lambda r: generate_history_topics(r), axis=1)
        df_fixed = df_fixed.apply(lambda r: generate_puzzles_competitors(r), axis=1)
        df_fixed = df_fixed.apply(lambda r: generate_puzzles_sub(r), axis=1)
        random.seed(None)

        return df_fixed
    return panel_df


def process(survey_guid: str, def_from_sheet=False, only_update_def_from_sheet=False, skip_overlap=False):

    print("START")

    conn = DbConnection()

    print("DEF")

    if def_from_sheet:
        survey_def = load_definitions_from_sheet()
        DmsModel.insert_or_update_dms(conn, json.dumps(survey_def), f"{survey_guid}__def", 'survey def', DmsModel.MIME_APPLICATION_JSON, 0)
        if only_update_def_from_sheet:
            conn.commit(True)
            return
    else:
        survey_def = load_definitions_from_dms(conn, survey_guid)

    if survey_def is None:
        raise Exception(f"Survey definitions not initialized !")

    row_survey = conn.select_one("""
    SELECT id FROM app.surveys WHERE guid = %s
    """, [survey_guid])
    survey_id = row_survey['id']

    print("PANEL")

    survey_data = {
        'panel': {},
        'dcm': {}
    }

    rows_panel = conn.select_all("""
    SELECT s.data, s.guid, s.params, w.group_id
      FROM app.surveys a,
           survey.survey s,
           survey.survey_whitelist w
     WHERE a.id = s.survey_id
       AND s.guid = w.guid
       AND w.state = 'completed'
       AND a.id = %s
    """, [survey_id])

    panel_dict = {}
    for row in rows_panel:
        group_id = str(row['group_id'])
        data = json.loads(row['data'])
        params = json.loads(row['params'])
        client_data = data['client_data']
        d = process_survey_panel_data(row['guid'], client_data, params, get_duration(data['stats_data']['time']), survey_def)
        d['cint_id'] = params['cid']
        if group_id not in panel_dict:
            panel_dict[group_id] = {}
            for it in d:
                panel_dict[group_id][it] = []
        for it in d:
            panel_dict[group_id][it].append(d[it])

    survey_data['panel'] = {}
    for gr in panel_dict:
        survey_data['panel'][gr] = panel_data_fix(pd.DataFrame.from_dict(panel_dict[gr]), survey_guid)

    print("DCM")

    rows_dcm = conn.select_all("""
    SELECT x.guid, 
           x.group_id, 
           x.survey_id as set_num, 
           x.card_id as card_num,
           IFNULL(ds.title_id, 'none') AS t, 
           IFNULL(ds.f_1_item_id, 0) AS f1, 
           IFNULL(ds.f_2_item_id, 0) AS f2, 
           x.score
      FROM (
    SELECT s.guid, ds.survey_id, w.group_id, ds.id AS card_id, dcs.id, CASE WHEN ds.chosen = dcs.id THEN 100 ELSE 0 END AS score
      FROM app.surveys a,
           survey.survey_whitelist w,
           survey.survey s,
           survey.survey_dcm_stats ds,
           survey.survey_dcm_choices_stats dcs
     WHERE a.id = s.survey_id
       AND ds.survey_id = s.id
       AND dcs.survey_id = s.id
       AND ds.dcm_1 = dcs.id
       AND w.survey_id = a.id
       AND w.guid = s.guid
       AND w.state = 'completed'
       AND a.id = %s
    UNION
    SELECT s.guid, ds.survey_id, w.group_id, ds.id AS card_id, dcs.id, CASE WHEN ds.chosen = dcs.id THEN 100 ELSE 0 END AS score
      FROM app.surveys a,
           survey.survey_whitelist w,
           survey.survey s,
           survey.survey_dcm_stats ds,
           survey.survey_dcm_choices_stats dcs
     WHERE a.id = s.survey_id
       AND ds.survey_id = s.id
       AND dcs.survey_id = s.id
       AND ds.dcm_2 = dcs.id
       AND w.survey_id = a.id
       AND w.guid = s.guid
       AND w.state = 'completed'
       AND a.id = %s
    UNION
    SELECT s.guid, ds.survey_id, w.group_id, ds.id AS card_id, 0 AS id, CASE WHEN ds.chosen = -1 THEN 100 ELSE 0 END as score
      FROM app.surveys a,
           survey.survey_whitelist w,
           survey.survey s,
           survey.survey_dcm_stats ds
     WHERE a.id = s.survey_id
       AND ds.survey_id = s.id
       AND w.survey_id = a.id
       AND w.guid = s.guid
       AND w.state = 'completed'
       AND a.id = %s
        ) x
        LEFT JOIN survey.survey_dcm_choices_stats ds ON ds.id = x.id
    ORDER BY x.guid ASC, x.card_id ASC
    """, [survey_id, survey_id, survey_id])

    dcm_dict = {}
    for row in rows_dcm:
        group_id = str(row['group_id'])
        d = process_survey_dcm_data(row, survey_def)
        if group_id not in dcm_dict:
            dcm_dict[group_id] = {}
            for it in d:
                dcm_dict[group_id][it] = []
        for it in d:
            dcm_dict[group_id][it].append(d[it])

    survey_data['dcm'] = {}
    for gr in dcm_dict:
        survey_data['dcm'][gr] = pd.DataFrame.from_dict(dcm_dict[gr])

    row_survey = conn.select_one("""
    SELECT survey_config FROM app.surveys WHERE guid = %s
    """, [survey_guid])

    print("SAVE")

    survey_config = json.loads(row_survey['survey_config'])
    for gr in survey_config['groups']:
        for it in survey_data:
            DmsModel.save_df_to_dms(conn, survey_data[it][gr], f"{survey_guid}__{gr}__{it}")

        if not skip_overlap:
            print("CALCULATE THEME OVERLAP - START")
            themes_overlap = calculate_dcm_themes_overlap(survey_data['panel'][gr], survey_data['dcm'][gr], survey_config['dcm_config'])
            DmsModel.save_json_to_dms(conn, themes_overlap, f"{survey_guid}__{gr}__themes_overlap")
            print("CALCULATE THEME OVERLAP - END")

    conn.query("""
    UPDATE app.surveys SET state = 'survey_data_processed' WHERE guid = %s
    """, [survey_guid])

    conn.commit(True)

    print("OK")


def default_method(*args, **kwargs):
    rr.ENV = GembaseUtils.get_arg("-e", "dev")
    def_from_sheet = GembaseUtils.has_arg("-sheet")
    only_update_def_from_sheet = GembaseUtils.has_arg("-only_sheet")
    survey_guid = GembaseUtils.get_arg("-sguid")
    skip_overlap = GembaseUtils.get_arg("-skip_overlap")

    if survey_guid is None:
        raise Exception(f"Missing -sguid argutment for survey guid")

    try:
        process(survey_guid, def_from_sheet=def_from_sheet, only_update_def_from_sheet=only_update_def_from_sheet, skip_overlap=skip_overlap)
    except Exception as e:
        print(str(e))
        raise e
