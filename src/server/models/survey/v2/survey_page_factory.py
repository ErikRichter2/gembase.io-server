from __future__ import annotations

from src.server.models.survey.v2.survey_page_model import SurveyAgePageModel, \
    SurveyDefaultPageModel, SurveyGenresDcmPageModel, SurveyTopicItemsPageModel, SurveyGenreItemsPageModel, \
    SurveyTopicDcmPageModel, SurveySpendingPageModel, SurveyPlayingPageModel, SurveyEndPageModel, \
    SurveyCompetitorsPageModel, SurveyLoyaltyPageModel, SurveyMultiplayerItemsPageModel, SurveyConceptsDcmPageModel, \
    SurveySpendingV2PageModel

__surveyPageModels = {
    "genres": SurveyGenreItemsPageModel,
    "topics": SurveyTopicItemsPageModel,
    "genres_dcm": SurveyGenresDcmPageModel,
    "concepts_dcm": SurveyConceptsDcmPageModel,
    "topics_dcm": SurveyTopicDcmPageModel,
    "age": SurveyAgePageModel,
    "spending": SurveySpendingPageModel,
    "playing_time": SurveyPlayingPageModel,
    "loyalty": SurveyLoyaltyPageModel,
    "end": SurveyEndPageModel,
    "competitors": SurveyCompetitorsPageModel,
    "multiplayer": SurveyMultiplayerItemsPageModel,
    "spending_v2": SurveySpendingV2PageModel
}


def create_page_model(page: str):
    for k in __surveyPageModels:
        if k == page:
            return __surveyPageModels[k]()
    return SurveyDefaultPageModel()
