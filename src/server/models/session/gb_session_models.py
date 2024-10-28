from __future__ import annotations
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from src.server.models.session.models.admin_session_model import AdminSessionModel
    from src.server.models.session.models.apps_session_model import AppsSessionModel
    from src.server.models.session.models.billing_session_model import BillingSessionModel
    from src.server.models.session.models.games_explorer_session_model import GamesExplorerSessionModel
    from src.server.models.session.models.platform_session_model import PlatformSessionModel
    from src.server.models.session.models.player_explorer_session_model import PlayerExplorerSessionModel
    from src.server.models.session.models.scraper_session_model import ScraperSessionModel
    from src.server.models.session.models.search_session_model import SearchSessionModel
    from src.server.models.session.models.studies_session_model import StudiesSessionModel
    from src.server.models.session.models.tags_session_model import TagsSessionModel
    from src.server.models.session.models.tutorial_session_model import TutorialSessionModel
    from src.session.session_instance import GbSessionInstance


class GbSessionModels:

    def __init__(self, session: GbSessionInstance):
        self.__session = session
        self.__models = {}

    def __create_if_not_exists(self, model) -> object:
        if model not in self.__models:
            self.__models[model] = model(self.__session)
        return self.__models[model]

    def admin(self) -> AdminSessionModel:
        from src.server.models.session.models.admin_session_model import AdminSessionModel
        return cast(AdminSessionModel, self.__create_if_not_exists(AdminSessionModel))

    def apps(self) -> AppsSessionModel:
        from src.server.models.session.models.apps_session_model import AppsSessionModel
        return cast(AppsSessionModel, self.__create_if_not_exists(AppsSessionModel))

    def billing(self) -> BillingSessionModel:
        from src.server.models.session.models.billing_session_model import BillingSessionModel
        return cast(BillingSessionModel, self.__create_if_not_exists(BillingSessionModel))

    def tags(self) -> TagsSessionModel:
        from src.server.models.session.models.tags_session_model import TagsSessionModel
        return cast(TagsSessionModel, self.__create_if_not_exists(TagsSessionModel))

    def platform(self) -> PlatformSessionModel:
        from src.server.models.session.models.platform_session_model import PlatformSessionModel
        return cast(PlatformSessionModel, self.__create_if_not_exists(PlatformSessionModel))

    def tutorial(self) -> TutorialSessionModel:
        from src.server.models.session.models.tutorial_session_model import TutorialSessionModel
        return cast(TutorialSessionModel, self.__create_if_not_exists(TutorialSessionModel))

    def scraper(self) -> ScraperSessionModel:
        from src.server.models.session.models.scraper_session_model import ScraperSessionModel
        return cast(ScraperSessionModel, self.__create_if_not_exists(ScraperSessionModel))

    def search(self) -> SearchSessionModel:
        from src.server.models.session.models.search_session_model import SearchSessionModel
        return cast(SearchSessionModel, self.__create_if_not_exists(SearchSessionModel))

    def games_explorer(self) -> GamesExplorerSessionModel:
        from src.server.models.session.models.games_explorer_session_model import GamesExplorerSessionModel
        return cast(GamesExplorerSessionModel, self.__create_if_not_exists(GamesExplorerSessionModel))

    def player_explorer(self) -> PlayerExplorerSessionModel:
        from src.server.models.session.models.player_explorer_session_model import PlayerExplorerSessionModel
        return cast(PlayerExplorerSessionModel, self.__create_if_not_exists(PlayerExplorerSessionModel))

    def studies(self) -> StudiesSessionModel:
        from src.server.models.session.models.studies_session_model import StudiesSessionModel
        return cast(StudiesSessionModel, self.__create_if_not_exists(StudiesSessionModel))
