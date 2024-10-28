from src.server.models.app_store_search.app_store_search import AppStoreSearch
from src.server.models.quota.user_quota_context import UserQuotaContext
from src.server.models.session.models.base.base_session_model import BaseSessionModel


class SearchSessionModel(BaseSessionModel):

    def get_developers_hints(
            self,
            title: str,
            include_concepts=False
    ) -> list:
        res = []

        quota_context = UserQuotaContext(
            user_id=self.user_id(),
            quota_type=UserQuotaContext.QUOTA_GOOGLE,
            context="search dev"
        )

        search = AppStoreSearch.search_dev_by_name(
            conn=self.conn(),
            dev_title=title,
            quota_context=quota_context,
            store=None,
            limit=3,
            include_concepts=include_concepts
        )

        for it in search:
            res.append({
                'dev_id_in_store': it['dev_id_in_store'],
                'title': it['title'],
                "store": it["store"]
            })

        return res

    def search_app_by_title(
            self,
            title: str,
            search_in_concepts=True
    ) -> list:
        context = UserQuotaContext(
            user_id=self.user_id(),
            quota_type=UserQuotaContext.QUOTA_GOOGLE,
            context="search app"
        )

        apps = AppStoreSearch.search_app_by_name(
            self.conn(),
            self.user_id(),
            title,
            context,
            search_in_concepts=search_in_concepts
        )

        return apps
