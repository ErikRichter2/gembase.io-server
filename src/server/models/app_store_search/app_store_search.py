from gembase_server_core.db.db_connection import DbConnection
from src.server.models.apps.app_model import AppModel
from src.external_api.google_search_model import GoogleSearchModel
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.scraper.store_scrapers.steam_scraper_model import SteamScraperModel
from src.server.models.quota.base_quota_context import BaseQuotaContext
from src.utils.web import WebParseUtils


class AppStoreSearch:

    @staticmethod
    def search_dev_internal_db(
            conn: DbConnection,
            dev_title: str,
            limit: int = 10,
            include_concepts=False
    ):
        dev_title = dev_title.lower().replace(" ", "").replace(":", "").replace("-", "")

        rows = conn.select_all("""
            SELECT d.dev_id_int, d.title, 0 as external, d.store, m.dev_id, d.dev_id_in_store
              FROM scraped_data.devs d
             INNER JOIN app.map_dev_id m ON m.id = d.dev_id_int
              LEFT JOIN scraped_data.devs_apps da ON da.dev_id_int = d.dev_id_int
              LEFT JOIN scraped_data.apps_valid a ON a.app_id_int = da.app_id_int
             WHERE REPLACE(REPLACE(REPLACE(LOWER(d.title), ' ', ''), ':', ''), '-', '') LIKE %s
               GROUP BY d.dev_id_int, d.dev_id_in_store, d.title, d.store
            ORDER BY sum(a.loyalty_installs) DESC
            LIMIT %s       
            """, [f"%{dev_title}%", limit])

        if include_concepts:
            rows_concepts = conn.select_all("""
            SELECT d.dev_id_int, d.title, 0 as external, 0 as store, m.dev_id, m.dev_id as dev_id_in_store
              FROM scraped_data.devs_concepts d
             INNER JOIN app.map_dev_id m ON m.id = d.dev_id_int
             WHERE REPLACE(REPLACE(REPLACE(LOWER(d.title), ' ', ''), ':', ''), '-', '') LIKE %s
             LIMIT %s       
            """, [f"%{dev_title}%", limit])

            rows = rows + rows_concepts

        return {
            "quota": 0,
            "data": rows
        }

    @staticmethod
    def __search_dev_google_store(quota_context: BaseQuotaContext, dev_name: str, limit: int = 10):
        res = []
        dev_ids = []
        search = GoogleSearchModel.search(
            f"{dev_name} site:play.google.com",
            quota_context.get_audit_context(),
            silent=True
        )
        if search is None:
            return None
        results = search["res"]
        dev_url_base_1 = 'https://play.google.com/store/apps/dev?id='
        dev_url_base_2 = 'https://play.google.com/store/apps/developer?id='

        developer_names_with_url_1 = []

        def get_title_from_item(item):
            return item['title'].replace('Android Apps by ', '').replace(' on Google Play', '')

        if 'items' in results:
            items = results['items']
            for item in items:
                if dev_url_base_1 in item['link']:
                    title = get_title_from_item(item)
                    developer_names_with_url_1.append(title.lower())
            for item in items:
                if dev_url_base_1 in item['link'] or dev_url_base_2 in item['link']:
                    if '&hl' in item['link']:
                        index = item['link'].rindex('&hl')
                        item['link'] = item['link'][:index]
                    developer_id: str = item['link'].replace(dev_url_base_1, '').replace(dev_url_base_2, '')
                    if developer_id in dev_ids:
                        continue
                    developer_name = get_title_from_item(item)

                    if dev_url_base_2 in item['link'] and developer_name.lower() in developer_names_with_url_1:
                        continue

                    dev_ids.append(developer_id)
                    res.append({
                        "dev_id_int": -1,
                        "dev_id": developer_id,
                        "dev_id_in_store": developer_id,
                        "title": developer_name,
                        "external": 1,
                        "store": AppModel.STORE__GOOGLE_PLAY,
                        "url": item['link']
                    })

                    if len(res) == limit:
                        break

        return {
            "quota": 1,
            "data": res,
            "audit_guid": search["audit_guid"]
        }

    @staticmethod
    def __search_dev_steam(quota_context: BaseQuotaContext, dev_name: str, limit: int = 10):
        res = []
        dev_ids = []
        search = GoogleSearchModel.search(
            f"{dev_name} site:store.steampowered.com",
            quota_context.get_audit_context(),
            silent=True
        )
        if search is None:
            return None
        results = search["res"]

        if 'items' in results:
            items = results['items']
            for item in items:
                dev_id_in_store = SteamScraperModel.get_dev_id_in_store_from_store_url(item["link"])
                if dev_id_in_store is None:
                    continue
                dev_id = SteamScraperModel.get_dev_id_from_dev_id_in_store(dev_id_in_store)
                if dev_id in dev_ids:
                    continue
                dev_ids.append(dev_id)
                developer_name = item['title'].replace('Steam Publisher: ', '').replace('Publisher: ', '')

                res.append({
                    "dev_id_int": -1,
                    "dev_id": dev_id,
                    "dev_id_in_store": dev_id_in_store,
                    "title": developer_name,
                    "external": 1,
                    "store": AppModel.STORE__STEAM,
                    "url": item["link"]
                })

                if len(res) == limit:
                    break

        return {
            "quota": 1,
            "data": res,
            "audit_guid": search["audit_guid"]
        }

    @staticmethod
    def search_app_internal_db(conn: DbConnection, user_id: int, app_name: str, limit: int = 10, search_in_concepts=True):
        app_name = app_name.lower().replace(" ", "").replace(":", "").replace("-", "")

        rows = conn.select_all("""
        SELECT a.app_id_int, a.app_id_in_store, a.title, a.icon, 0 as external, a.store, %s as app_type
          FROM scraped_data.apps_valid a,
               app.def_sheet_platform_values_install_tiers t
         WHERE REPLACE(REPLACE(REPLACE(LOWER(a.title), ' ', ''), ':', ''), '-', '') LIKE %s
           AND t.store_id = a.store
           AND t.value_from <= a.installs 
           AND a.installs < t.value_to
         ORDER BY t.tier DESC, a.loyalty_installs DESC
         LIMIT %s
        """, [AppModel.APP_TYPE__STORE, f"%{app_name}%", limit])

        rows_concept = []
        if search_in_concepts:
            rows_concept = conn.select_all("""
            SELECT a.app_id_int, '' as app_id_in_store, a.title, a.icon, 0 as external, a.store, %s as app_type
              FROM scraped_data.apps_concepts a
             WHERE REPLACE(REPLACE(REPLACE(LOWER(a.title), ' ', ''), ':', ''), '-', '') LIKE %s
               AND a.user_id = %s
             ORDER BY a.t DESC
             LIMIT %s
            """, [AppModel.APP_TYPE__CONCEPT, f"%{app_name}%", user_id, limit])

        return {
            "quota": 0,
            "data": rows + rows_concept
        }

    @staticmethod
    def search_app_google_store(quota_context: BaseQuotaContext, app_name: str, limit: int = 10):
        res = []
        app_ids = []
        search = GoogleSearchModel.search(
            f"{app_name} site:play.google.com",
            quota_context.get_audit_context(),
            silent=True
        )
        if search is None:
            return None
        results = search["res"]
        if "items" in results:
            for item in results['items']:
                if 'play.google.com/store/apps/details?id=' in item['link']:
                    if 'pagemap' in item:
                        if 'metatags' in item['pagemap']:
                            metatags = item['pagemap']['metatags'][0]
                            if 'appstore:store_id' in metatags and 'og:image' in metatags and 'og:title' in metatags:
                                if metatags['appstore:store_id'] in app_ids:
                                    continue
                                app_id_in_store = metatags['appstore:store_id']

                                if AppStoreSearch.store_page_exists(
                                        app_id_in_store=app_id_in_store,
                                        store=AppModel.STORE__GOOGLE_PLAY):
                                    app_ids.append(metatags['appstore:store_id'])
                                    res.append({
                                        "app_id_in_store": metatags['appstore:store_id'],
                                        "title": metatags['og:title'].replace(' - Apps on Google Play', ''),
                                        "icon": metatags['og:image'],
                                        "external": 1,
                                        "store": AppModel.STORE__GOOGLE_PLAY,
                                        "app_type": 'store'
                                    })
                                    if len(res) == limit:
                                        break
        return {
            "quota": 1,
            "data": res,
            "audit_guid": search["audit_guid"]
        }

    @staticmethod
    def __search_app_steam(quota_context: BaseQuotaContext, app_name: str, limit: int = 10):
        res = []
        app_ids = []
        search = GoogleSearchModel.search(
            f"{app_name} site:store.steampowered.com",
            quota_context.get_audit_context(),
            silent=True
        )
        if search is None:
            return None
        results = search["res"]
        if "items" in results:
            for item in results['items']:
                if 'store.steampowered.com/app/' in item['link']:
                    if 'pagemap' in item:
                        if "product" in item["pagemap"] and len(item["pagemap"]["product"]) > 0:
                            link: str = item["link"]
                            title = item["pagemap"]["product"][0]["name"]
                            icon = item["pagemap"]["product"][0]["image"]

                            steam_id = link.replace("https://store.steampowered.com/app/", "")
                            ix = steam_id.find("/")
                            if ix != -1:
                                steam_id = steam_id[:ix]
                            app_id = SteamScraperModel.get_app_id_from_steam_id(steam_id)
                            if app_id in app_ids:
                                continue
                            app_ids.append(app_id)
                            res.append({
                                "app_id_in_store": app_id,
                                "title": title,
                                "icon": icon,
                                "external": 1,
                                "store": AppModel.STORE__STEAM,
                                "app_type": 'store'
                            })
                            if len(res) == limit:
                                break
        return {
            "quota": 1,
            "data": res,
            "audit_guid": search["audit_guid"]
        }

    @staticmethod
    def search_app_by_name(conn: DbConnection, user_id: int, app_name: str, quota_context: BaseQuotaContext, store: int | None = None, limit: int = 10, search_in_concepts=True):

        def can_search_external(store: int, items: []):
            cnt = 0
            for it in items:
                if it["store"] == store:
                    cnt += 1
            return cnt < limit and quota_context.has()

        res = []
        app_ids_in_store = []

        search_data = AppStoreSearch.search_app_internal_db(conn, user_id, app_name, search_in_concepts=search_in_concepts)

        for it in search_data['data']:
            if it['app_id_in_store'] not in app_ids_in_store:
                app_ids_in_store.append(it['app_id_in_store'])
                res.append(it)

        if not can_search_external(AppModel.STORE__GOOGLE_PLAY, res):
            return res

        if store is None or store == AppModel.STORE__GOOGLE_PLAY:
            search_data = AppStoreSearch.search_app_google_store(quota_context, app_name, limit)
            if search_data is not None:
                if search_data['quota'] > 0:
                    quota_context.add(search_data['quota'], search_data['audit_guid'])

                for it in search_data['data']:
                    if it['app_id_in_store'] not in app_ids_in_store:
                        if not ScraperModel.is_app_ignored(
                            conn=conn,
                            app_id_in_store=it['app_id_in_store'],
                            store=AppModel.STORE__GOOGLE_PLAY
                        ):
                            app_ids_in_store.append(it['app_id_in_store'])
                            res.append(it)

        if not can_search_external(AppModel.STORE__STEAM, res):
            return res

        if store is None or store == AppModel.STORE__STEAM:
            search_data = AppStoreSearch.__search_app_steam(quota_context, app_name, limit)
            if search_data is not None:
                if search_data['quota'] > 0:
                    quota_context.add(search_data['quota'], search_data["audit_guid"])

                for it in search_data['data']:
                    if it['app_id_in_store'] not in app_ids_in_store:
                        if not ScraperModel.is_app_ignored(
                                conn=conn,
                                app_id_in_store=it['app_id_in_store'],
                                store=AppModel.STORE__STEAM
                        ):
                            app_ids_in_store.append(it['app_id_in_store'])
                            res.append(it)

        return res

    @staticmethod
    def search_dev_by_name(
            conn: DbConnection,
            dev_title: str,
            quota_context: BaseQuotaContext,
            store: int | None = None,
            limit: int = 10,
            include_concepts=False
    ):
        def can_search_external(store: int, items: []):
            cnt = 0
            for it in items:
                if it["store"] == store:
                    cnt += 1
            return cnt < limit and quota_context.has()

        res = []
        dev_ids = []

        if dev_title == "gmail":
            return []

        search_data = AppStoreSearch.search_dev_internal_db(
            conn,
            dev_title,
            limit=limit,
            include_concepts=include_concepts
        )

        for it in search_data['data']:
            if it['dev_id'].lower() not in dev_ids:
                dev_ids.append(it['dev_id'].lower())
                res.append(it)

        if not can_search_external(AppModel.STORE__GOOGLE_PLAY, res):
            return res

        if store is None or store == AppModel.STORE__GOOGLE_PLAY:
            search_data = AppStoreSearch.__search_dev_google_store(quota_context, dev_title, limit=limit)
            if search_data is not None:
                if search_data['quota'] > 0:
                    quota_context.add(search_data['quota'], search_data["audit_guid"])

                for it in search_data['data']:
                    if it['dev_id'].lower() not in dev_ids:
                        dev_ids.append(it['dev_id'].lower())
                        res.append(it)

        if not can_search_external(AppModel.STORE__STEAM, res):
            return res

        if store is None or store == AppModel.STORE__STEAM:
            search_data = AppStoreSearch.__search_dev_steam(quota_context, dev_title, limit=limit)
            if search_data is not None:
                if search_data['quota'] > 0:
                    quota_context.add(search_data['quota'], search_data["audit_guid"])

                for it in search_data['data']:
                    if it['dev_id'].lower() not in dev_ids:
                        dev_ids.append(it['dev_id'].lower())
                        res.append(it)

        return res

    @staticmethod
    def store_page_exists(app_id_in_store: str, store: int) -> bool:
        if store == AppModel.STORE__GOOGLE_PLAY:
            header = WebParseUtils.get_page_head_request(f"https://play.google.com/store/apps/details?id={app_id_in_store}&hl=en")

            if header is None or header.status_code == 404:
                return False

            return True

        return True
