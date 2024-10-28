class SurveyConstants:

    @staticmethod
    def get_prefix(n: str) -> str:
        return f"{n}__"

    CHARTS = 'charts'
    GENRES = 'genres'
    GENRES_SUB = 'genres_sub'
    THEMES = 'themes'
    TOPICS = 'topics'
    NEEDS = 'needs'
    BEHAVIORS = 'behaviors'
    COMPETITORS = 'competitors'
    ROUTINE = 'routine'
    ROUTINE_OPEN = 'routine_open'
    MOVIES = 'movies'
    MOVIES_OPEN = 'movies_open'
    HOBBIES = 'hobbies'
    HOBBIES_OPEN = 'hobbies_open'
    SOCIALS = 'socials'
    SOCIALS_OPEN = 'socials_open'
    DEVICES = 'devices'
    CONCEPTS = 'concepts'
    BEST_GAME = 'best_game'
    SPENDING = 'spending'
    SPENDING_GROUPS = 'spending_groups'
    ROLE = 'role'
    PLAYING = 'playing'
    AGE = 'age'
    DCM_TITLE = 'dcm_title'
    DCM_FEATURES = 'dcm_features'
    SLAVIC_THEME = 'slavic_theme'
    SINGLE_PLAYER = 'single_player'
    MULTI_PLAYER = 'multi_player'
    SINGLE_MULTI_PLAYER = 'single_multi_player'

    GENRES_GOAL_VALUE = 0.5
    NEEDS_GOAL_VALUE = 0.5
    BEHAVIOURS_GOAL_VALUE = 0.66


sc = SurveyConstants
