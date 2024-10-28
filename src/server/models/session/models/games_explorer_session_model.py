from src.server.models.apps.app_data_model import AppDataModel
from src.server.models.apps.app_model import AppModel
from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.survey.survey_data_model import SurveyDataModel
from src.server.models.user.user_obfuscator import UserObfuscator
from src.utils.gembase_utils import GembaseUtils


class GamesExplorerSessionModel(BaseSessionModel):

    GAMES_EXPLORER_FILTER_STORES = [
        {"id": AppModel.STORE__GOOGLE_PLAY, "label": "Google Play"},
        {"id": AppModel.STORE__STEAM, "label": "Steam"}
    ]

    GAMES_EXPLORER_FILTER_PRICES = [
        {"tier": 1, "from": 0, "to": 1, "label": "$0 - $1"},
        {"tier": 2, "from": 2, "to": 3, "label": "$2 - $3"},
        {"tier": 3, "from": 4, "to": 5, "label": "$4 - $5"},
        {"tier": 4, "from": 6, "to": 10, "label": "$6 - $10"},
        {"tier": 5, "from": 11, "to": 15, "label": "$11 - $15"},
        {"tier": 6, "from": 16, "to": 20, "label": "$16 - $20"},
        {"tier": 7, "from": 21, "to": 30, "label": "$21 - $30"},
        {"tier": 8, "from": 31, "to": 40, "label": "$31 - $40"},
        {"tier": 9, "from": 41, "to": 50, "label": "$41 - $50"},
        {"tier": 10, "from": 51, "to": 100, "label": "$51 - $100"},
        {"tier": 11, "from": 101, "to": 999999, "label": "$100+"},
    ]

    def get_games_explorer_filters_def(self):
        return {
            "prices": self.GAMES_EXPLORER_FILTER_PRICES,
            "stores": self.GAMES_EXPLORER_FILTER_STORES
        }

    def get_games_explorer_compare_apps_data(
            self,
            app_ids_int: list[int]
    ):
        if len(app_ids_int) <= 0:
            return []

        app_ids_int_db = self.conn().values_arr_to_db_in(app_ids_int, int_values=True)

        user_dev_id_int = self.session().user().get_dev_id_int()

        rows_apps = self.conn().select_all(f"""
        SELECT a.app_id_int,
               a.store,
               a.installs,
               scraped_data.get_app_growth(a.app_id_int) as growth,
               a.rating,
               a.title,
               a.icon,
               a.app_id_in_store,
               IF(d.dev_id_int IS NULL, dc.title, d.title) as dev_title,
               d.dev_id_int,
               YEAR(a.released) as released_year,
               UNIX_TIMESTAMP(a.scraped_t) as scraped_t,
               UNIX_TIMESTAMP(ts.t) as tagged_t,
               a.app_type,
               a.removed_from_store
          FROM (
                     SELECT a.app_id_int, a.title, a.icon, a.store, a.app_id_in_store, IF(a.released IS NULL, NOW(), a.released) as released, 
                     a.price, a.installs, a.scraped_t, a.rating, da.dev_id_int, 
                     '{AppDataModel.APP_TYPE__STORE}' as app_type, IF(a.removed_from_store IS NULL, 0, 1) as removed_from_store
                       FROM scraped_data.devs_apps da,
                            scraped_data.apps_valid a
                      WHERE da.app_id_int = a.app_id_int
                       AND da.primary_dev = 1
                       AND a.app_id_int IN ({app_ids_int_db})
                       UNION ALL
                    SELECT a.app_id_int, a.title, IF(i.app_id_int IS NULL, '', '[LOCAL_ICON_URL]') as icon, a.store, a.app_id_in_store, NOW() as released, 
                    0 as price, 0 as installs, NOW() as scrapred_t, 0 as rating, 
                    {user_dev_id_int} as dev_id_int, '{AppDataModel.APP_TYPE__CONCEPT}' as app_type,
                    0 as removed_from_store
                       FROM scraped_data.apps_concepts a
                       LEFT JOIN scraped_data.apps_icons i ON i.app_id_int = a.app_id_int
                      WHERE a.user_id = {self.user_id()}
                        AND a.app_id_int IN ({app_ids_int_db})
                 ) a
            LEFT JOIN scraped_data.devs d ON d.dev_id_int = a.dev_id_int
            LEFT JOIN scraped_data.devs_concepts dc ON dc.dev_id_int = a.dev_id_int
          LEFT JOIN tagged_data.platform_tagged ts ON ts.app_id_int = a.app_id_int
        """)

        rows_tam = self.conn().select_all(f"""
        SELECT tam.app_id_int,
               tam.tam, 
               tam.total_audience 
          FROM platform.audience_angle_tam_per_app tam
         WHERE tam.app_id_int IN ({app_ids_int_db})
           AND tam.angle_cnt = tam.max_angle_cnt
        """)

        rows_revenues = self.conn().select_all(f"""
        SELECT r.app_id_int, 
               r.revenue
          FROM scraped_data.ext_data_2_revenues_per_app r
         WHERE r.app_id_int IN ({app_ids_int_db})
         UNION
        SELECT a.app_id_int, 
               FLOOR(a.installs * a.price * 0.35) as revenue
          FROM scraped_data.apps a
         WHERE a.app_id_int IN ({app_ids_int_db})
           AND a.store = %s
           AND a.price > 0
        """, [AppDataModel.STORE__STEAM])

        # tam for concepts
        rows_apps_concepts = self.conn().select_all(f"""
        SELECT a.app_id_int FROM scraped_data.apps_concepts a WHERE a.app_id_int IN ({app_ids_int_db})
        """)
        for row in rows_apps_concepts:
            app_id_int = row[UserObfuscator.APP_ID_INT]
            rows_tags = self.conn().select_all("""
            SELECT t.tag_id_int FROM tagged_data.tags_v t WHERE t.app_id_int = %s
            AND t.tag_rank != 0
            ORDER BY t.tag_id_int
            """, [app_id_int])

            audience_angle_ids = []
            for i in range(len(rows_tags)):
                if rows_tags[i][UserObfuscator.TAG_ID_INT] not in audience_angle_ids:
                    audience_angle_ids.append(rows_tags[i][UserObfuscator.TAG_ID_INT])
                for j in range(i + 1, len(rows_tags)):
                    aa_id = PlatformValuesHelper.create_audience_angle_2_comb_id(
                        rows_tags[i][UserObfuscator.TAG_ID_INT],
                        rows_tags[j][UserObfuscator.TAG_ID_INT]
                    )
                    if aa_id not in audience_angle_ids:
                        audience_angle_ids.append(aa_id)

            if len(audience_angle_ids) > 0:
                survey_id = SurveyDataModel.get_survey_meta_id(
                    conn=self.conn(),
                    survey_control_guid=self.session().models().platform().SURVEY_CONTROL_GUID
                )
                audience_angle_ids_db = self.conn().values_arr_to_db_in(audience_angle_ids, int_values=True)
                query = f"""
                SELECT z2.total_audience,
                       z2.tam
                  FROM (
                        SELECT z1.total_audience,
                               platform.calc_tam(
                                   z1.total_audience,
                                   z1.arpu
                               ) as tam
                        FROM (
                            SELECT platform.calc_total_audience(
                                aap.loved_cnt,
                                aap.total_cnt,
                                concept.rejected_cnt,
                                platform.get_potential_downloads(concept.b, aa.id),
                                platform.get_loyalty_installs(
                                    aap.loyalty_installs, 
                                    platform.get_potential_downloads(concept.b, aa.id)),
                                aap.loved_ratio_ext,
                                aap.rejected_ratio_ext
                            ) as total_audience,
                            aa.arpu
                            FROM platform.audience_angle aa,
                                 platform.audience_angle_potential aap,
                                 (
                                     SELECT aa.id, count(1) as rejected_cnt, my_tags.b
                                      FROM platform.audience_angle aa,
                                           platform.platform_values_survey_tags_bin stb,
                                           (
                                             SELECT BIT_OR(z1.b) as b
                                               FROM (
                                             SELECT db.b
                                               FROM platform.def_tags_bin db,
                                                    tagged_data.tags_v t
                                              WHERE t.app_id_int = {app_id_int}
                                                AND t.tag_id_int = db.tag_id_int
                                              UNION
                                             SELECT z.b FROM platform.zero_bin_value z ) z1
                                         ) my_tags
                                     WHERE BIT_COUNT(stb.b_loved & aa.b) = aa.angle_cnt
                                       AND BIT_COUNT(stb.b_rejected & my_tags.b) > 0
                                       AND stb.survey_id = {survey_id}
                                       AND aa.id IN ({audience_angle_ids_db})
                                       AND aa.valid_combination = 1
                                     group by aa.id, my_tags.b
                                 ) concept
                            WHERE aa.id IN ({audience_angle_ids_db})
                              AND aa.id = aap.id
                              AND aa.valid_combination = 1
                              AND concept.id = aa.id
                        ) z1  
                ) z2
                ORDER BY z2.tam DESC
                LIMIT 1
                """

                row_tam = self.conn().select_one_or_none(query)

                if row_tam is not None:
                    rows_tam.append({
                        UserObfuscator.APP_ID_INT: app_id_int,
                        "tam": float(row_tam["tam"]),
                        "total_audience": float(row_tam["total_audience"])
                    })

        data_per_app = {}

        for row in rows_apps:
            data_per_app[row[UserObfuscator.APP_ID_INT]] = {
                "item": {
                    "type": 0,
                    "id": row[UserObfuscator.APP_ID_INT]
                },
                UserObfuscator.APP_ID_INT: row[UserObfuscator.APP_ID_INT],
                "store": row["store"],
                "installs": GembaseUtils.format_number(row["installs"]),
                "installs_raw": row["installs"],
                "growth": GembaseUtils.format_number(row["growth"]),
                "growth_raw": row["growth"],
                "rating": str(round(row["rating"] / 20, 2)),
                "rating_raw": round(row["rating"] / 20, 2),
                "tam": "",
                "tam_raw": 0,
                "total_audience": "",
                "total_audience_raw": 0,
                "tags": [],
                "title": row["title"],
                "icon": row["icon"],
                "app_id_in_store": row["app_id_in_store"],
                "dev_title": row["dev_title"],
                UserObfuscator.DEV_ID_INT: row[UserObfuscator.DEV_ID_INT],
                "released_year": str(row["released_year"]),
                "novelty_raw": row["released_year"],
                "scraped_t": row["scraped_t"],
                "tagged_t": row["tagged_t"],
                "app_type": row["app_type"],
                "removed_from_store": row["removed_from_store"]
            }

        for row in rows_tam:
            app_id_int = row[UserObfuscator.APP_ID_INT]
            if app_id_int in data_per_app:
                d = data_per_app[app_id_int]
                d["tam"] = f"${GembaseUtils.format_number(int(row['tam']))}"
                d["tam_raw"] = int(row['tam'])
                d["total_audience"] = f"{GembaseUtils.format_number(int(row['total_audience']))}"
                d["total_audience_raw"] = int(row['total_audience'])

        for row in rows_revenues:
            if row[UserObfuscator.APP_ID_INT] in data_per_app:
                d = data_per_app[row[UserObfuscator.APP_ID_INT]]
                d["revenue"] = int(row["revenue"])

        query = f"""
        SELECT t.app_id_int, 
               p.tag_id_int,
               t.tag_rank
          FROM app.def_sheet_platform_product p
         INNER JOIN tagged_data.platform_tagged pt
            ON pt.app_id_int IN ({app_ids_int_db})
         INNER JOIN tagged_data.tags_v t
            ON t.app_id_int IN ({app_ids_int_db})
           AND t.tag_id_int = p.tag_id_int
           AND t.app_id_int = pt.app_id_int
         WHERE p.is_prompt = 1
           AND p.is_survey = 1
           AND (p.unlocked = 1 OR %s = 0)
        """

        rows_tags_apps = self.conn().select_all(query, [
            self.session().models().billing().is_module_locked(
                BillingUtils.BILLING_MODULE_INSIGHT
            )
        ])

        for row in rows_tags_apps:
            if row[UserObfuscator.APP_ID_INT] in data_per_app:
                data_per_app[row[UserObfuscator.APP_ID_INT]]["tags"].append({
                    UserObfuscator.TAG_ID_INT: row[UserObfuscator.TAG_ID_INT],
                    "tag_rank": row["tag_rank"]
                })

        res = []
        for k in data_per_app:
            data_per_app[k]["item"]["id"] = self.session().user().obfuscator().server_to_client(
                data_per_app[k]["item"]["id"]
            )
            res.append(data_per_app[k])

        return res

    def scrap_apps_for_devs(self, filters: []) -> bool:
        dev_ids_int: list[int] = []
        for filter in filters:
            if UserObfuscator.DEV_IDS_INT in filter and len(filter[UserObfuscator.DEV_IDS_INT]) > 0:
                for dev_id_int in filter[UserObfuscator.DEV_IDS_INT]:
                    if dev_id_int not in dev_ids_int:
                        dev_ids_int.append(dev_id_int)

        if len(dev_ids_int) == 0:
            return True

        apps_ids_per_dev_id = AppModel.get_devs_apps_ids_int(
            conn=self.conn(),
            devs_ids_int=dev_ids_int,
            user_id=self.user_id()
        )

        all_apps_ids = []
        for dev_id_int in apps_ids_per_dev_id:
            all_apps_ids = all_apps_ids + apps_ids_per_dev_id[dev_id_int]

        if len(dev_ids_int) == 0:
            return True

        return self.session().models().apps().scrap_apps_if_not_scraped(
            app_ids_int=all_apps_ids
        )

    def get_games_explorer_filter_apps(
            self,
            filters: list
    ):
        res = []

        for filter in filters:

            tags = []
            tags_query = ""

            if UserObfuscator.TAG_IDS_INT in filter:

                tags = filter[UserObfuscator.TAG_IDS_INT]

                if len(tags) > 0 and self.session().models().billing().is_module_locked(
                        BillingUtils.BILLING_MODULE_INSIGHT
                ):
                    allowed_tags_def = self.session().models().billing().get_allowed_tags(
                        BillingUtils.BILLING_MODULE_INSIGHT
                    )

                    tags_new = []
                    for it in allowed_tags_def:
                        if it["is_changeable"] == 1 and it[UserObfuscator.TAG_ID_INT] in tags:
                            tags_new.append(it[UserObfuscator.TAG_ID_INT])
                    tags = tags_new

                if len(tags) > 0:
                    tags_db = self.conn().values_arr_to_db_in(tags, int_values=True)
                    tags_query = f"""
                        INNER JOIN (
                         SELECT tb.app_id_int, tb.b as app_b, t.b as my_b
                           FROM platform.platform_values_tags_bin tb,
                                (
                                    SELECT BIT_OR(dtb.b) as b, COUNT(1) as cnt
                                      FROM platform.def_tags_bin dtb
                                     WHERE dtb.tag_id_int IN ({tags_db})
                                ) t
                          WHERE BIT_COUNT(tb.b & t.b) = t.cnt
                        ) t ON t.app_id_int = a.app_id_int
                        """

            prices_query = ""
            if "prices" in filter:
                def get_price_by_tier(tier: int):
                    for it in self.GAMES_EXPLORER_FILTER_PRICES:
                        if it["tier"] == tier:
                            return it
                    raise Exception(f"Price tier {tier} not found")

                tier_from = filter["prices"]["from"]
                tier_to = filter["prices"]["to"]
                tier_min = filter["prices"]["min"]
                tier_max = filter["prices"]["max"]
                tiers_q = []
                if tier_from != tier_min:
                    tiers_q.append(f" a.price >= {get_price_by_tier(tier_from)['from']} ")
                if tier_to != tier_max:
                    tiers_q.append(f" a.price < {get_price_by_tier(tier_to)['to']} ")
                if len(tiers_q) > 0:
                    prices_query = f" AND {' AND '.join(tiers_q)}"

            store_query = ""
            if "stores" in filter and len(filter["stores"]) > 0:
                arr = []
                for store in filter["stores"]:
                    arr.append(f"(a.store = {store})")
                if len(arr) > 0:
                    store_query = f" AND ({' OR '.join(arr)})"

            tiers_query = ""
            if "tier" in filter:
                tier_from = filter["tier"]["from"]
                tier_to = filter["tier"]["to"]
                tier_min = filter["tier"]["min"]
                tier_max = filter["tier"]["max"]
                tiers_q = []
                if tier_from != tier_min:
                    tiers_q.append(
                        f" tiers.tier >= {tier_from} AND a.store = tiers.store_id AND a.installs >= tiers.value_from AND a.installs < tiers.value_to ")
                if tier_to != tier_max:
                    tiers_q.append(
                        f" tiers.tier <= {tier_to} AND a.store = tiers.store_id AND a.installs >= tiers.value_from AND a.installs < tiers.value_to ")
                if len(tiers_q) > 0:
                    tiers_query = f" INNER JOIN app.def_sheet_platform_values_install_tiers tiers ON {' AND '.join(tiers_q)}"

            user_dev_id_int = self.session().user().get_dev_id_int()

            app_ids = [-1]
            if "my_apps" in filter and filter["my_apps"]:
                my_apps = self.session().user().get_my_apps()[0]
                if len(my_apps) > 0:
                    app_ids = my_apps
                else:
                    app_ids = [-2]
            elif UserObfuscator.DEV_IDS_INT in filter and len(filter[UserObfuscator.DEV_IDS_INT]) > 0:
                apps_ids_per_dev_id = AppModel.get_devs_apps_ids_int(
                    conn=self.conn(),
                    devs_ids_int=filter[UserObfuscator.DEV_IDS_INT],
                    user_id=self.user_id(),
                    include_concepts=True
                )
                tmp_app_ids = []
                for dev_id_int in apps_ids_per_dev_id:
                    for app_id_int in apps_ids_per_dev_id[dev_id_int]:
                        if app_id_int not in tmp_app_ids:
                            tmp_app_ids.append(app_id_int)

                if len(tmp_app_ids) > 0:
                    app_ids = tmp_app_ids
                else:
                    app_ids = [-2]

            app_ids_db = self.conn().values_arr_to_db_in(app_ids, int_values=True)

            top_competitors = 100

            sorting_enum = ["INSTALLS", "SIMILARITY", "TAM", "GROWTH", "NOVELTY", "QUALITY"]
            sorting = sorting_enum[0]
            if "advanced_filter_data" in filter and filter["advanced_filter_data"] is not None:
                advanced_filter_data = filter["advanced_filter_data"]
                if "sorting" in advanced_filter_data and advanced_filter_data["sorting"] is not None:
                    sorting = advanced_filter_data["sorting"]
                    if sorting not in sorting_enum:
                        sorting = sorting_enum[0]
                if "top_competitors" in advanced_filter_data and advanced_filter_data["top_competitors"] is not None:
                    assert GembaseUtils.is_int(advanced_filter_data["top_competitors"])
                    top_competitors = advanced_filter_data["top_competitors"]
                    if top_competitors <= 0:
                        top_competitors = 1
                    elif top_competitors > 100:
                        top_competitors = 100

            similarity_q = ""
            if sorting == "SIMILARITY":
                if len(tags) > 0:
                    PlatformValuesHelper.recreate_table(
                        conn=self.conn(),
                        table_name="tmp_games_explorer_competitors_pool_w",
                        query=f"""
                                CREATE TABLE platform_values.x__table_name__x (
                                subcategory_int int unsigned not null,
                                competitors_pool_w int unsigned not null,
                                constraint x__table_name__x_pk
                                    primary key (subcategory_int)
                                )
                                SELECT DISTINCT 
                                       d.subcategory_int,
                                       d.competitors_pool_w
                                  FROM platform.def_tags d
                                """
                    )

                    query = f"""
                                CREATE TABLE platform_values.x__table_name__x (
                                    app_id_int int UNSIGNED NOT NULL,
                                    score int NOT NULL,
                                    CONSTRAINT x__table_name__x_pk 
                                        PRIMARY KEY (app_id_int)
                                )
                                SELECT z2.app_id_int, round(z2.score) as score
                      FROM (
                    SELECT z1.app_id_int, z1.installs,
                    SUM(CAST(z1.score1 as SIGNED) - CAST((z1.score2 + z1.score3) as SIGNED)) AS score
                    FROM (

                    SELECT pool.app_id_int, pool.installs,   
                           3 * BIT_COUNT(pool.app_b & pool.my_b & d.b) * pool_w.competitors_pool_w AS score1,
                           BIT_COUNT(pool.app_b & d.b) * pool_w.competitors_pool_w AS score2,
                           BIT_COUNT(pool.my_b & d.b) * pool_w.competitors_pool_w AS score3
                      FROM (
                            SELECT a.app_id_int, a.installs, t.app_b, t.my_b
                              FROM platform.platform_values_apps a
                             {tags_query}
                             WHERE (a.app_id_int in ({app_ids_db}) OR -1 IN ({app_ids_db}))
                            ) pool,
                    platform.def_tags_bin d,
                    app.def_sheet_platform_product p,
                    platform_values.tmp_games_explorer_competitors_pool_w pool_w
                    WHERE p.tag_id_int = d.tag_id_int
                    AND pool_w.subcategory_int = p.subcategory_int
                    ) z1
                    GROUP BY z1.app_id_int
                    ) z2
                    ORDER BY z2.score DESC, z2.installs DESC
                    """

                    # todo add survey w
                    PlatformValuesHelper.recreate_table(
                        conn=self.conn(),
                        table_name="tmp_games_explorer_similarity_pool",
                        query=query
                    )

                    similarity_q = """ 
                    INNER JOIN platform_values.tmp_games_explorer_similarity_pool pool
                    ON pool.app_id_int = da.app_id_int
                    """

            sorting_q = ""
            if sorting == "INSTALLS":
                sorting_q = "p.installs DESC"
            elif sorting == "GROWTH":
                sorting_q = "p.growth DESC"
            elif sorting == "NOVELTY":
                sorting_q = "p.released_years ASC"
            elif sorting == "QUALITY":
                sorting_q = "p.rating DESC"
            elif sorting == "TAM":
                sorting_q = "tam.tam DESC"
            elif sorting == "SIMILARITY" and similarity_q != "":
                sorting_q = "pool.score DESC"

            sorting_q = f"{sorting_q}{'' if sorting_q == '' else ','} p.installs DESC"

            final_apps = []

            query_apps = f"""
                SELECT a.app_id_int,
                       a.title,
                       a.icon,
                       '{AppDataModel.APP_TYPE__STORE}' as app_type,
                       a.store,
                       a.app_id_in_store,
                       IF(d.dev_id_int is null, dc.title, d.title) as dev_title,
                       p.growth,
                       p.rating,
                       p.installs,
                       p.released_years,
                       tam.tam,
                       IF(a.removed_from_store IS NULL, 0, 1) as removed_from_store
                  FROM scraped_data.devs_apps da
                  INNER JOIN scraped_data.apps_valid a ON da.app_id_int = a.app_id_int
                  AND (a.app_id_int in ({app_ids_db}) OR -1 IN ({app_ids_db}))
                  INNER JOIN platform.platform_values_apps p ON da.app_id_int = p.app_id_int
                  {similarity_q}
                  LEFT JOIN platform.audience_angle_tam_per_app tam
                    ON tam.app_id_int = da.app_id_int
                    AND tam.angle_cnt = tam.max_angle_cnt
                 LEFT JOIN scraped_data.devs d ON d.dev_id_int = da.dev_id_int
                 LEFT JOIN scraped_data.devs_concepts dc ON dc.dev_id_int = da.dev_id_int
                  {tags_query}
                  {tiers_query}
                  WHERE da.primary_dev = 1

                    {prices_query}
                    {store_query}
                  ORDER BY {sorting_q}
                  LIMIT {top_competitors}
                """

            rows_apps = self.conn().select_all(query_apps)

            for i in range(len(rows_apps)):
                rows_apps[i]["item"] = {
                    "type": 0,
                    "id": self.session().user().obfuscator().server_to_client(rows_apps[i][UserObfuscator.APP_ID_INT])
                }
                final_apps.append(rows_apps[i])

            query_apps_concepts = f"""
                            SELECT a.app_id_int,
                                   a.title,
                                   IF(i.app_id_int IS NULL, '', '[LOCAL_ICON_URL]') as icon,
                                   '{AppDataModel.APP_TYPE__CONCEPT}' as app_type,
                                   a.store,
                                   a.app_id_in_store,
                                   IF(d.dev_id_int is null, dc.title, d.title) as dev_title
                              FROM scraped_data.apps_concepts a
                              LEFT JOIN scraped_data.apps_icons i ON i.app_id_int = a.app_id_int
                             LEFT JOIN scraped_data.devs d ON d.dev_id_int = {user_dev_id_int}
                             LEFT JOIN scraped_data.devs_concepts dc ON dc.dev_id_int = {user_dev_id_int}
                              {tags_query}
                              WHERE (a.app_id_int in ({app_ids_db}) OR -1 IN ({app_ids_db}))
                              AND a.user_id = {self.user_id()}
                                {store_query}
                              ORDER BY a.app_id_int DESC
                              LIMIT 100
                            """

            rows_apps_concepts = self.conn().select_all(query_apps_concepts)

            for i in range(len(rows_apps_concepts)):
                rows_apps_concepts[i]["item"] = {
                    "type": 1,
                    "id": self.session().user().obfuscator().server_to_client(rows_apps_concepts[i][UserObfuscator.APP_ID_INT])
                }
                final_apps.append(rows_apps_concepts[i])

            res.append({
                "id": filter["id"],
                "apps": final_apps
            })

        return {
            "state": "ok",
            "data": res
        }
