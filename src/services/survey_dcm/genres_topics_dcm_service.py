import pandas as pd

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.server.models.tags.tags_constants import TagsConstants
from src.utils.gembase_utils import GembaseUtils

import larch


class GenresTopicsDcmService:

    def __init__(self, conn: DbConnection, survey_id: int):
        self.conn = conn
        self.survey_id = survey_id

    def __prepare_data(self):
        rows_tags_def = self.conn.select_all("""
                SELECT p.tag_id_int, p.subcategory_int, p.parent_genre_for_core_tag_id_int
                  FROM app.def_sheet_platform_product p
                """)

        map_tags_per_subcategory = {}
        map_core_per_genre = {}
        map_genre_cores = {}
        for row in rows_tags_def:

            if row["subcategory_int"] not in map_tags_per_subcategory:
                map_tags_per_subcategory[row["subcategory_int"]] = []
            map_tags_per_subcategory[row["subcategory_int"]].append(row["tag_id_int"])

            if row["parent_genre_for_core_tag_id_int"] != 0:
                if row["parent_genre_for_core_tag_id_int"] not in map_genre_cores:
                    map_genre_cores[row["parent_genre_for_core_tag_id_int"]] = []
                map_genre_cores[row["parent_genre_for_core_tag_id_int"]].append(row["tag_id_int"])
                map_core_per_genre[row["tag_id_int"]] = row["parent_genre_for_core_tag_id_int"]

        rows_respondents = self.conn.select_all("""
                SELECT r.respondent_id, r.age, r.female, r.role, r.favorite_game, r.spending, r.loyalty,
                       r.playing_time
                  FROM survey_results.respondents r
                 WHERE r.survey_id = %s
                 LIMIT 50
                """, [self.survey_id])

        df_respondents = pd.DataFrame.from_dict(rows_respondents)

        rows_dcm_genres = self.conn.select_all("""
                SELECT r.respondent_id, r.dcm_order, r.genre_1, r.genre_2, r.core_1, r.core_2, r.chosen
                  FROM survey_results.dcm_genres r
                 WHERE r.survey_id = %s
                """, [self.survey_id])

        genres_dcm_df = pd.DataFrame.from_dict(rows_dcm_genres)

        rows_dcm_topics = self.conn.select_all("""
                SELECT r.respondent_id, r.dcm_order, r.topic, r.feature_1a, r.feature_1b, r.feature_2a,
                       r.feature_2b, r.chosen_a, r.chosen_b
                  FROM survey_results.dcm_topics r
                 WHERE r.survey_id = %s
                """, [self.survey_id])

        df_dcm_topics = pd.DataFrame.from_dict(rows_dcm_topics)

        rows_tags = self.conn.select_all("""
                SELECT r.respondent_id, CAST(r.tag_id_int as CHAR) as tag_id_int, r.tag_value
                  FROM survey_results.tags r
                 WHERE r.survey_id = %s
                """, [self.survey_id])

        df_tags = pd.DataFrame.from_dict(rows_tags)

        tags_pivot_df = df_tags.pivot(
            index="respondent_id",
            columns="tag_id_int",
            values="tag_value"
        ).reset_index()

        self.user_df = df_respondents.merge(tags_pivot_df, on='respondent_id')

        self.GENRE_COLS = [str(c) for c in map_tags_per_subcategory[TagsConstants.SUBCATEGORY_GENRE_ID]]
        self.TOPIC_COLS = [str(c) for c in map_tags_per_subcategory[TagsConstants.SUBCATEGORY_TOPICS_ID]]
        self.BEHAVIOUR_COLS = [str(c) for c in map_tags_per_subcategory[TagsConstants.SUBCATEGORY_BEHAVIORS_ID]]

        self.genres_with_na = self.user_df[self.GENRE_COLS].isna().sum().loc[lambda x: x > 0].index.to_list()
        self.GENRE_NA_COLS = [f'{x}_na' for x in self.genres_with_na]

        rows = self.conn.select_all("""
                SELECT r.respondent_id, r.feature_1a
                  FROM survey_results.dcm_topics r,
                       app.def_sheet_platform_product p
                 WHERE r.survey_id = %s
                   AND r.feature_1a = p.tag_id_int
                   AND p.subcategory_int = %s
                """, [self.survey_id, TagsConstants.SUBCATEGORY_ENTITIES_ID])

        topic_dcm_df1 = pd.DataFrame.from_dict(rows)

        rows = self.conn.select_all("""
                SELECT r.respondent_id, r.feature_1a, r.feature_1b
                  FROM survey_results.dcm_topics r,
                       app.def_sheet_platform_product p
                 WHERE r.survey_id = %s
                   AND r.feature_1a = p.tag_id_int
                   AND p.subcategory_int != %s
                """, [self.survey_id, TagsConstants.SUBCATEGORY_ENTITIES_ID])

        topic_dcm_df2 = pd.DataFrame.from_dict(rows)

        rows_tags_def = self.conn.select_all(f"""
                SELECT z1.tag_id_int,              
                       z1.subcategory_int,
                       ROW_NUMBER() over (PARTITION BY z1.subcategory_int ORDER BY z1.tag_id_int) as idx
                  FROM (
                        SELECT p.tag_id_int, 
                          CASE WHEN p.subcategory_int = {TagsConstants.SUBCATEGORY_ENVIRONMENT_ID} THEN {TagsConstants.SUBCATEGORY_ERAS_ID}
                               WHEN p.subcategory_int = {TagsConstants.SUBCATEGORY_FOCUS_ID} THEN {TagsConstants.SUBCATEGORY_DOMAINS_ID}
                               ELSE p.subcategory_int END as subcategory_int
                          FROM app.def_sheet_platform_product p
                       ) z1
                 ORDER BY z1.subcategory_int, z1.tag_id_int
                """)

        self.entity_idx_dict = {}
        self.role_idx_dict = {}
        self.era_enviro_idx_dict = {}
        self.domain_focus_idx_dict = {}

        for row in rows_tags_def:
            if row["subcategory_int"] == TagsConstants.SUBCATEGORY_ENTITIES_ID:
                self.entity_idx_dict[row["idx"]] = row["tag_id_int"]
            elif row["subcategory_int"] == TagsConstants.SUBCATEGORY_ROLES_ID:
                self.role_idx_dict[row["idx"]] = row["tag_id_int"]
            elif row["subcategory_int"] == TagsConstants.SUBCATEGORY_ERAS_ID:
                self.era_enviro_idx_dict[row["idx"]] = row["tag_id_int"]
            elif row["subcategory_int"] == TagsConstants.SUBCATEGORY_DOMAINS_ID:
                self.domain_focus_idx_dict[row["idx"]] = row["tag_id_int"]

        self.idx_entity_dict = {y: x for x, y in self.entity_idx_dict.items()}
        self.idx_role_dict = {y: x for x, y in self.role_idx_dict.items()}
        self.idx_era_enviro_dict = {y: x for x, y in self.era_enviro_idx_dict.items()}
        self.idx_domain_focus_dict = {y: x for x, y in self.domain_focus_idx_dict.items()}

        for col, col_named in zip(self.genres_with_na, self.GENRE_NA_COLS):
            if col_named not in self.user_df:
                self.user_df[col_named] = self.user_df[col].apply(lambda x: 1 if pd.isna(x) else 0)
            self.user_df[col] = self.user_df[col].apply(lambda x: 0 if pd.isna(x) else x)

        self.mechanic_idx_dict = {x: idx for idx, x in enumerate(map_core_per_genre)}
        self.idx_mechanic_dict = {y: x for x, y in self.mechanic_idx_dict.items()}

        self.df_mech = genres_dcm_df.merge(self.user_df, on='respondent_id')
        self.df_role = topic_dcm_df1.merge(self.user_df, on='respondent_id')
        self.df_ent = topic_dcm_df1.merge(self.user_df, on='respondent_id')

    def __process_roles(self):
        df_role = self.df_role
        idx_role_dict = self.idx_role_dict

        cols_used = ['age'] + self.GENRE_COLS + self.GENRE_NA_COLS + self.TOPIC_COLS + self.BEHAVIOUR_COLS  # 'gender_female',

        av_cols = []

        # availablity cols
        for idx, col in self.idx_role_dict.items():
            av_col = ((df_role.feature_1b == col).astype(int) + (df_role.feature_2b == col).astype(int))
            av_col.name = f'av_{idx}'

            av_cols.append(av_col)

        df_role = pd.concat([df_role] + av_cols, axis=1).copy()
        df_role[f'av_{len(idx_role_dict)}'] = 1
        df_role['choice'] = df_role['chosen_b'].apply(lambda x: len(idx_role_dict) if x is None else self.role_idx_dict[x])

        # combined cols
        new_dcm_cols = []
        idx_cols_dict = {x: [] for x in idx_role_dict}

        for user_col in cols_used:
            user_col_min = df_role[user_col].min()
            user_col_max = df_role[user_col].max()

            for idx, col in idx_role_dict.items():
                dcm_user_col = df_role[f'av_{idx}'] * df_role[user_col]
                # min-max scaling -> 0-1
                dcm_user_col = (dcm_user_col - user_col_min) / (user_col_max - user_col_min)
                dcm_user_col.name = f'{col}__{user_col}'

                idx_cols_dict[idx].append(f'{col}__{user_col}')
                new_dcm_cols.append(dcm_user_col)

        df_role = pd.concat([df_role] + new_dcm_cols, axis=1).copy()

        m = larch.Model()

        m.choice_co_code = 'choice'

        m.availability_co_vars = {idx: f'av_{idx}' for idx in range(len(idx_role_dict) + 1)}

        for idx, user_cols in idx_cols_dict.items():
            m.utility_co[idx] = larch.P(f'{idx_role_dict[idx]}_ASC')

            for user_col in user_cols:
                m.utility_co[idx] += larch.X(user_col) * larch.P(user_col)

        m.utility_co[len(idx_role_dict)] = 0

        dfs = larch.DataFrames(df_role, alt_codes=[i for i in range(len(idx_role_dict) + 1)])

        m.dataservice = dfs

        m.load_data()

        m.maximize_loglike(quiet=False)

    def __process_topics(self):
        df_ent = self.df_ent
        cols_used = ['age'] + self.GENRE_COLS + self.GENRE_NA_COLS + self.TOPIC_COLS + self.BEHAVIOUR_COLS  # 'gender_female',

        av_cols = []

        # availablity cols
        for idx, col in self.idx_entity_dict.items():
            av_col = ((df_ent.feature_1a == col).astype(int) + (df_ent.feature_2a == col).astype(int))
            av_col.name = f'av_{idx}'

            av_cols.append(av_col)

        df_ent = pd.concat([df_ent] + av_cols, axis=1).copy()
        df_ent[f'av_{len(self.idx_entity_dict)}'] = 1
        df_ent['choice'] = df_ent['chosen_a'].apply(lambda x: len(self.idx_entity_dict) if x is None else self.entity_idx_dict[x])

        # combined cols
        new_dcm_cols = []
        idx_cols_dict = {x: [] for x in self.idx_entity_dict}

        for user_col in cols_used:
            user_col_min = df_ent[user_col].min()
            user_col_max = df_ent[user_col].max()

            for idx, col in self.idx_entity_dict.items():
                dcm_user_col = df_ent[f'av_{idx}'] * df_ent[user_col]
                # min-max scaling -> 0-1
                dcm_user_col = (dcm_user_col - user_col_min) / (user_col_max - user_col_min)
                dcm_user_col.name = f'{col}__{user_col}'

                idx_cols_dict[idx].append(f'{col}__{user_col}')
                new_dcm_cols.append(dcm_user_col)

        df_ent = pd.concat([df_ent] + new_dcm_cols, axis=1).copy()

        m = larch.Model()

        m.choice_co_code = 'choice'

        m.availability_co_vars = {idx: f'av_{idx}' for idx in range(len(self.idx_entity_dict) + 1)}

        for idx, user_cols in idx_cols_dict.items():
            m.utility_co[idx] = larch.P(f'{self.idx_entity_dict[idx]}_ASC')

            for user_col in user_cols:
                m.utility_co[idx] += larch.X(user_col) * larch.P(user_col)

        m.utility_co[len(self.idx_entity_dict)] = 0

        dfs = larch.DataFrames(df_ent, alt_codes=[i for i in range(len(self.idx_entity_dict) + 1)])

        m.dataservice = dfs

        m.load_data()

        m.maximize_loglike(quiet=False)

    def __process_mechanics(self):
        av_cols = []

        cols_used = ['age'] + self.GENRE_COLS + self.GENRE_NA_COLS + self.TOPIC_COLS + self.BEHAVIOUR_COLS  # 'gender_female',

        df_mech = self.df_mech
        idx_mechanic_dict = self.idx_mechanic_dict
        mechanic_idx_dict = self.mechanic_idx_dict

        # availablity cols
        for idx, col in idx_mechanic_dict.items():
            av_col = ((df_mech.core_1 == col).astype(int) + (df_mech.core_2 == col).astype(int))
            av_col.name = f'av_{idx}'

            av_cols.append(av_col)

        df_mech = pd.concat([df_mech] + av_cols, axis=1).copy()
        df_mech[f'av_{len(idx_mechanic_dict)}'] = 1
        df_mech['choice'] = df_mech['chosen'].apply(
            lambda x: len(idx_mechanic_dict) if pd.isna(x) else mechanic_idx_dict[x])

        # combined cols
        new_dcm_cols = []
        idx_cols_dict = {x: [] for x in idx_mechanic_dict}

        for user_col in cols_used:
            user_col_min = df_mech[user_col].min()
            user_col_max = df_mech[user_col].max()

            for idx, col in idx_mechanic_dict.items():
                dcm_user_col = df_mech[f'av_{idx}'] * df_mech[user_col]
                # min-max scaling -> 0-1
                dcm_user_col = (dcm_user_col - user_col_min) / (user_col_max - user_col_min)
                dcm_user_col.name = f'{col}__{user_col}'

                idx_cols_dict[idx].append(f'{col}__{user_col}')
                new_dcm_cols.append(dcm_user_col)

        df_mech = pd.concat([df_mech] + new_dcm_cols, axis=1).copy()

        m = larch.Model()

        m.choice_co_code = 'choice'

        m.availability_co_vars = {idx: f'av_{idx}' for idx in range(len(idx_mechanic_dict) + 1)}

        for idx, user_cols in idx_cols_dict.items():
            m.utility_co[idx] = larch.P(f'{idx_mechanic_dict[idx]}_ASC')

            for user_col in user_cols:
                m.utility_co[idx] += larch.X(user_col) * larch.P(user_col)

        m.utility_co[len(idx_mechanic_dict)] = 0

        alt_codes = [i for i in range(len(idx_mechanic_dict) + 1)]
        dfs = larch.DataFrames(df_mech, alt_codes=alt_codes)

        m.dataservice = dfs

        m.load_data()

        m.maximize_loglike(quiet=False, maxiter=30)

        return m.get_values()

    def process(self):
        self.conn.select_one_or_none("""
        UPDATE survey_results.dcm_genres_topics_ml_state s
           SET s.state = 'working',
               s.t = NOW()
         WHERE s.survey_id = %s
        """, [self.survey_id])
        self.conn.commit()

        self.__prepare_data()
        self.__process_mechanics()
        self.__process_topics()
        self.__process_roles()


def default_method(*args, **kwargs):
    GembaseUtils.log_service(f"GenresTopicsDcmService START")
    GenresTopicsDcmService(
        conn=ServiceWrapperModel.create_conn(),
        survey_id=kwargs["input_data"]["survey_id"]
    )
    GembaseUtils.log_service(f"GenresTopicsDcmService START")
