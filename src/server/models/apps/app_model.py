import json

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.private_data.private_data_model import PrivateDataModel
from src.server.models.apps.app_data_model import AppDataModel
from src.server.models.platform_values.cache.platform_values_cache import PlatformValuesCache
from src.server.models.user.user_constants import uc
from src.server.models.user.user_obfuscator import UserObfuscator
from src.server.models.tags.tags_constants import TagsConstants
from src.server.models.user.user_model import UserModel
from src.utils.gembase_utils import GembaseUtils


class AppModel:

    APP_TYPE__STORE = AppDataModel.APP_TYPE__STORE
    APP_TYPE__CONCEPT = AppDataModel.APP_TYPE__CONCEPT

    PLATFORM__MOBILE = AppDataModel.PLATFORM__MOBILE
    PLATFORM__PC = AppDataModel.PLATFORM__PC

    STORE__UNKNOWN = AppDataModel.STORE__UNKNOWN
    STORE__CONCEPT = AppDataModel.STORE__CONCEPT
    STORE__GOOGLE_PLAY = AppDataModel.STORE__GOOGLE_PLAY
    STORE__STEAM = AppDataModel.STORE__STEAM

    TAG_RANK_NONE = TagsConstants.TAG_RANK_NONE
    TAG_RANK_PRIMARY = TagsConstants.TAG_RANK_PRIMARY
    TAG_RANK_SECONDARY = TagsConstants.TAG_RANK_SECONDARY
    TAG_RANK_TERTIARY = TagsConstants.TAG_RANK_TERTIARY

    CHANGED_TITLE = AppDataModel.CHANGED_TITLE
    CHANGED_DESC = AppDataModel.CHANGED_DESC

    @staticmethod
    def is_dev_concept(conn: DbConnection, dev_id_int: int) -> bool:
        return AppDataModel.is_dev_concept(conn=conn, dev_id_int=dev_id_int)

    @staticmethod
    def is_concept(conn: DbConnection, app_id_int: int, check_owner: int | None = None) -> bool:
        row = conn.select_one_or_none("""
        SELECT a.app_id_int, 
               a.user_id
          FROM scraped_data.apps_concepts a
         WHERE a.app_id_int = %s
        """, [app_id_int])

        if row is None:
            return False

        if check_owner is not None and check_owner != row["user_id"]:
            return False

        return True

    @staticmethod
    def get_platform_for_store(store: int) -> int:
        return AppDataModel.get_platform_for_store(store=store)

    @staticmethod
    def get_store_url(
            url: str | None,
            app_id_in_store: str,
            app_type: int,
            store: int) -> str:
        if url is not None:
            return url

        app_store_url = ""
        if store == AppModel.STORE__STEAM:
            app_id_in_store = app_id_in_store.replace("steam__", "")
        if app_type != AppModel.APP_TYPE__CONCEPT:
            if store == AppModel.STORE__GOOGLE_PLAY:
                app_store_url = f"https://play.google.com/store/apps/details?id={app_id_in_store}&hl=en"
            elif store == AppModel.STORE__STEAM:
                app_store_url = f"https://store.steampowered.com/app/{app_id_in_store}"
        else:
            app_store_url = app_id_in_store

        return app_store_url

    @staticmethod
    def get_dev_store_url(dev_id_in_store: str, store: int) -> str:
        return AppDataModel.get_dev_store_url(dev_id_in_store=dev_id_in_store, store=store)

    @staticmethod
    def get_platform_str(platform: int) -> str:
        if platform == AppModel.PLATFORM__MOBILE:
            return "mobile"
        elif platform == AppModel.PLATFORM__PC:
            return "pc"

        raise Exception(f"Unknown platform {platform}")

    @staticmethod
    def __get_app_detail_data_from_db_row(row, app_type):
        if "app_id_in_store" not in row:
            row["app_id_in_store"] = ""
        if "description" not in row:
            row["description"] = ""
        if "concept_counter" not in row:
            row["concept_counter"] = 0
        if "tier" not in row:
            row["tier"] = 0
        if "growth" not in row:
            row["growth"] = 0
        if "removed_from_store" not in row:
            row["removed_from_store"] = 0
        app_store_url = AppModel.get_store_url(
            url=row["url"] if "url" in row else None,
            app_id_in_store=row["app_id_in_store_raw"],
            app_type=app_type,
            store=row["store"]
        )

        res = {
            "app_id_int": row["app_id_int"],
            'app_id_in_store': row["app_id_in_store"],
            'app_type': app_type,
            'title': row['title'],
            'icon': row['icon'],
            'description': row['description'],
            "store": row["store"],
            "platform": row["platform"],
            "installs": row["installs"],
            "rating": row["rating"],
            "concept_counter": row["concept_counter"],
            "tier": row["tier"],
            "growth": row["growth"],
            "app_store_url": app_store_url,
            "locked": False,
            "tagged_t": row["tagged_t"] if "tagged_t" in row else None,
            "scraped_t": row["scraped_t"] if "scraped_t" in row else None,
            "tagged_by_user": row["tagged_by_user"] if "tagged_by_user" in row else 0,
            "released_year": row["released_year"] if "released_year" in row else 0,
            "tam": row["tam"] if "tam" in row else 0,
            "tags": [],
            "removed_from_store": row["removed_from_store"],
            "premium": row["premium"] if "premium" in row else 0,
            "iap": row["iap"] if "iap" in row else 0,
            "ads": row["ads"] if "ads" in row else 0
        }

        if "dev_id_int" in row:
            res["dev_id_int"] = row["dev_id_int"]
            if "dev_id_in_store" in row:
                res["dev_store_url"] = AppModel.get_dev_store_url(
                    dev_id_in_store=row["dev_id_in_store"],
                    store=row["store"]
                )
        if "dev_title" in row:
            res["dev_title"] = row["dev_title"]

        return res

    @staticmethod
    def get_gallery(
            conn: DbConnection,
            app_ids_int: [],
            user_id: int
    ):
        res = []

        if len(app_ids_int) == 0:
            return res

        app_ids_int_db = conn.values_arr_to_db_in(app_ids_int, int_values=True)

        rows = conn.select_all(f"""
        SELECT ag.id, ag.app_id_int, ag.img_order, ag.store_url, dms.guid
          FROM scraped_data.apps_gallery ag
         inner join (
             SELECT a.app_id_int
               FROM scraped_data.apps a
             WHERE a.app_id_int IN ({app_ids_int_db})
             UNION
             SELECT ac.app_id_int
               FROM scraped_data.apps_concepts ac
             WHERE ac.app_id_int IN ({app_ids_int_db})
             AND ac.user_id = %s
         ) a ON a.app_id_int = ag.app_id_int
         LEFT JOIN app.dms dms
         ON dms.id = ag.dms_id
        """, [user_id])

        urls_per_app = {}
        for row in rows:
            app_id_int = row["app_id_int"]
            if app_id_int not in urls_per_app:
                d = {
                    "app_id_int": app_id_int,
                    "gallery": []
                }
                urls_per_app[app_id_int] = d
                res.append(d)
            img_data = {
                "id": row["id"],
                "img_order": row["img_order"]
            }
            dms_guid = row["guid"]
            if dms_guid is not None:
                img_data["dms_guid"] = dms_guid
            else:
                img_data["store_url"] = row["store_url"]
            d = urls_per_app[app_id_int]
            d["gallery"].append(img_data)

        return res

    @staticmethod
    def obfuscate_app_detail(app_detail: {}, obfuscator: UserObfuscator) -> {}:
        obfuscator.server_to_client_id(app_detail)
        app_detail["app_id_in_store"] = "locked-by-demo"
        app_detail["concept_counter"] = 0
        app_detail["tier"] = 0
        app_detail["title"] = "Game locked in DEMO"
        app_detail["description"] = ""
        app_detail["installs"] = 0
        app_detail["rating"] = 0
        app_detail["app_store_url"] = ""
        app_detail["locked"] = True
        app_detail["gallery"] = []

    @staticmethod
    def get_app_detail(
            conn: DbConnection,
            app_id_int: int,
            user_id=-1,
            include_gallery=False,
            remove_desc=False,
            include_tags=False
    ):
        app_details = AppModel.get_app_detail_bulk(
            conn=conn,
            app_ids_int=[app_id_int],
            user_id=user_id,
            include_gallery=include_gallery,
            remove_desc=remove_desc,
            remove_from_app_ids_if_not_found=False,
            return_array=False,
            include_tags=include_tags
        )
        if app_id_int in app_details:
            return app_details[app_id_int]
        return None

    @staticmethod
    def get_app_detail_bulk(
            conn: DbConnection,
            app_ids_int: [],
            user_id=-1,
            include_gallery=False,
            remove_desc=False,
            remove_from_app_ids_if_not_found=False,
            return_array=False,
            include_tags=False
    ):
        if len(app_ids_int) == 0:
            if return_array:
                return []
            return {}

        app_ids_db = conn.values_arr_to_db_in(app_ids_int, int_values=True)

        app_details = {}
        apps_found = []

        desc_query = "" if remove_desc else "a.description,"

        rows = conn.select_all(f"""
        SELECT {desc_query} a.app_id_int, a.app_id_in_store, a.title, a.icon, a.store, 
        a.platform, a.installs, a.rating, scraped_data.get_app_tier(a.app_id_int) as tier,
        d.dev_id_int, d.title as dev_title, d.dev_id_in_store, m.app_id_in_store as app_id_in_store_raw,
        UNIX_TIMESTAMP(a.scraped_t) as scraped_t, UNIX_TIMESTAMP(pt.t) as tagged_t,
        scraped_data.get_app_growth(a.app_id_int) as growth,
        YEAR(a.released) as released_year, tam.tam, 
        IF(a.removed_from_store IS NULL, 0, 1) as removed_from_store, IF(a.price > 0, 1, 0) as premium,
        a.iap as iap, a.ads as ads
          FROM scraped_data.apps a
         INNER JOIN app.map_app_id_to_store_id m
            ON m.app_id_int = a.app_id_int
         INNER JOIN scraped_data.devs_apps da
            ON da.app_id_int = a.app_id_int
           AND da.primary_dev = 1
         INNER JOIN scraped_data.devs d
            ON d.dev_id_int = da.dev_id_int
          LEFT JOIN tagged_data.platform_tagged pt
            ON pt.app_id_int = a.app_id_int
          LEFT JOIN platform.platform_values_apps p
            ON p.app_id_int = a.app_id_int
          LEFT JOIN platform.audience_angle_tam_per_app tam
            ON tam.app_id_int = a.app_id_int
           AND tam.angle_cnt = tam.max_angle_cnt
         WHERE a.app_id_int IN ({app_ids_db})
         """)

        for row in rows:
            app_id_int = row["app_id_int"]
            apps_found.append(app_id_int)
            app_details[app_id_int] = AppModel.__get_app_detail_data_from_db_row(row, AppModel.APP_TYPE__STORE)

        rows = conn.select_all(f"""
        SELECT {desc_query} a.app_id_int, a.app_id_in_store, a.title, IF(i.app_id_int IS NULL, '', '[LOCAL_ICON_URL]') as icon, a.store, 
        a.platform, 0 as installs, 
        0 as rating, a.concept_counter, a.app_id_in_store as app_id_in_store_raw,
        UNIX_TIMESTAMP(ts.t) as tagged_t, a.tagged_by_user, 0 as premium, 0 as iap, 0 as ads
          FROM scraped_data.apps_concepts a
          LEFT JOIN app.users u
            ON u.id = %s
          LEFT JOIN tagged_data.platform_tagged ts
            ON ts.app_id_int = a.app_id_int
          LEFT JOIN scraped_data.apps_icons i
          ON i.app_id_int = a.app_id_int
         WHERE a.app_id_int IN ({app_ids_db})
           AND (a.user_id = %s OR %s = -1 OR u.guid = %s)
        """, [user_id, user_id, user_id, uc.ADMIN_USER_GUID])

        for row in rows:
            app_id_int = row["app_id_int"]
            apps_found.append(app_id_int)
            app_details[app_id_int] = AppModel.__get_app_detail_data_from_db_row(row, AppModel.APP_TYPE__CONCEPT)

        if remove_from_app_ids_if_not_found:
            app_ids_int.clear()
            for it in apps_found:
                app_ids_int.append(it)

        if include_gallery:
            gallery = AppModel.get_gallery(
                conn=conn,
                app_ids_int=app_ids_int,
                user_id=user_id
            )
            for it in gallery:
                for app_id_int in app_details:
                    if it["app_id_int"] == app_id_int:
                        app_details[app_id_int]["gallery"] = it["gallery"]

        if include_tags:
            tags_per_app_id_int = AppModel.get_tags_details_bulk(
                conn=conn,
                apps_ids_int=app_ids_int
            )
            tagging_state_per_app_id_int = AppModel.get_tagging_state_bulk(
                conn=conn,
                app_ids_int=app_ids_int,
                user_id=user_id
            )
            for app_id_int in tags_per_app_id_int:
                if app_id_int in app_details:
                    app_details[app_id_int]["tags"] = tags_per_app_id_int[app_id_int]
            for app_id_int in tagging_state_per_app_id_int:
                if app_id_int in app_details:
                    app_details[app_id_int]["tagging_state"] = tagging_state_per_app_id_int[app_id_int]

        if return_array:
            arr = []
            for app_id in app_details:
                arr.append(app_details[app_id])
            return arr

        return app_details

    @staticmethod
    def create_concept_as_copy(conn: DbConnection, user_id: int, from_app_id_int: int, concept_counter: int):

        app_detail = AppModel.get_app_detail(
            conn=conn,
            app_id_int=from_app_id_int,
            user_id=user_id,
            include_tags=True
        )

        copy_app_id_int = AppModel.create_next_id_atomic()

        AppModel.create_concept_app(
            conn=conn,
            user_id=user_id,
            app_id_int=copy_app_id_int,
            app_detail_changes=app_detail,
            concept_counter=concept_counter
        )

        conn.query("""
        UPDATE scraped_data.apps_concepts
           SET copied_from_app_id_int = %s
         WHERE app_id_int = %s
        """, [from_app_id_int, copy_app_id_int])

        conn.query("""
        INSERT INTO scraped_data.apps_gallery
        (app_id_int, img_order, dms_id, store_url) 
        SELECT %s as app_id_int, ag.img_order, ag.dms_id, ag.store_url
          FROM scraped_data.apps_gallery ag
         WHERE ag.app_id_int = %s
        """, [copy_app_id_int, from_app_id_int])

        return copy_app_id_int

    @staticmethod
    def create_concept_app(
            conn: DbConnection,
            user_id: int,
            app_id_int: int,
            app_detail_changes: {},
            concept_counter: int
    ):
        conn.query("""
        INSERT INTO scraped_data.apps_concepts (app_id_int, app_id_in_store, user_id, title, description, icon, copied_from_app_id_int, platform, concept_counter) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [app_id_int, GembaseUtils.get_guid(), user_id, "Concept", "", "", app_id_int, AppModel.PLATFORM__MOBILE, concept_counter])

        if app_detail_changes is not None:
            AppModel.update_concept_app(
                conn=conn,
                app_id_int=app_id_int,
                app_detail_changes=app_detail_changes,
                user_id=user_id
            )

        return app_id_int

    @staticmethod
    def set_tags(
            conn: DbConnection,
            user_id: int,
            app_id_int: int,
            tags_details: [],
            context: str,
            manual=False,
            remove_tags: list | None = None,
            skip_cache_calc=False
    ):
        overriden_by_user = 1 if conn.select_one_or_none("""
        SELECT 1 FROM tagged_data.tags_override_from_users_apps WHERE app_id_int = %s
        """, [app_id_int]) is not None else 0

        hist_id = conn.insert("""
        INSERT INTO tagged_data.platform_tagged_history (app_id_int, context)
        VALUES (%s, %s)
        """, [app_id_int, context])
        conn.query("""
        INSERT INTO tagged_data.tags_history (hist_id, prompt_row_id, app_id_int, tag_id_int, tag_rank,
        added_from_store, removed_from_store, is_tag_rank_override, tag_rank_override)
        SELECT %s as hist_id, t.prompt_row_id, t.app_id_int, t.tag_id_int, t.tag_rank,
        t.added_from_store, t.removed_from_store, t.is_tag_rank_override, t.tag_rank_override
          FROM tagged_data.tags t
         WHERE t.app_id_int = %s
        """, [hist_id, app_id_int])

        if remove_tags is not None:
            remove_tags_db = conn.values_arr_to_db_in(remove_tags, int_values=True)
            conn.query(f"""
            DELETE FROM tagged_data.tags t
             WHERE t.app_id_int = %s
               AND t.tag_id_int IN ({remove_tags_db})
            """, [app_id_int])
        else:
            conn.query("""
            DELETE FROM tagged_data.tags t
             WHERE t.app_id_int = %s
            """, [app_id_int])

        conn.query("""
        DELETE FROM tagged_data.platform_tagged t
         WHERE t.app_id_int = %s
        """, [app_id_int])

        q_data = []
        for it in tags_details:
            added_from_store = it["added_from_store"] if "added_from_store" in it else 0
            removed_from_store = it["removed_from_store"] if "removed_from_store" in it else 0
            is_tag_rank_override = it["is_tag_rank_override"] if "is_tag_rank_override" in it else 0
            tag_rank_override = it["tag_rank_override"] if "tag_rank_override" in it else 0
            prompt_row_id = it["prompt_row_id"] if "prompt_row_id" in it else 0
            q_data.append((app_id_int, it["tag_id_int"], prompt_row_id, it["tag_rank"], added_from_store,
                           removed_from_store, is_tag_rank_override, tag_rank_override, overriden_by_user))
        if len(q_data) > 0:
            conn.bulk("""
            INSERT INTO tagged_data.tags (app_id_int, tag_id_int, prompt_row_id, tag_rank, 
            added_from_store, removed_from_store, is_tag_rank_override, tag_rank_override, overriden_by_user) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, q_data)

        conn.query("""
        INSERT INTO tagged_data.platform_tagged (app_id_int, manual, prompts_b)
        SELECT %s as app_id_int, %s as manual, b.b
          FROM tagged_data.active_prompts_b b
        """, [app_id_int, manual])

        if not skip_cache_calc:
            PlatformValuesCache.start_service_for_single_app(app_id_int=app_id_int)

    @staticmethod
    def set_manual_tags(conn: DbConnection, user_id: int, app_id_int: int, tags_details: []):

        if not AppModel.is_concept(conn, app_id_int):
            raise Exception(f"Setting tags allowed only for concept app")

        AppModel.set_tags(
            conn=conn,
            user_id=user_id,
            app_id_int=app_id_int,
            tags_details=tags_details,
            context="concept_manual_tags",
            manual=True
        )

        return AppModel.get_tags(conn, user_id, app_id_int)

    @staticmethod
    def get_tagging_state(
            conn: DbConnection,
            app_id_int: int,
            user_id: int
    ):
        return AppModel.get_tagging_state_bulk(
            conn=conn,
            user_id=user_id,
            app_ids_int=[app_id_int]
        )[app_id_int]

    @staticmethod
    def get_tagging_state_bulk(
            conn: DbConnection,
            app_ids_int: list[int],
            user_id: int
    ):
        data_per_app_id = {}

        rows_request = conn.select_all(f"""
        SELECT app_id_int, state, UNIX_TIMESTAMP(request_t) as request_t, 
               UNIX_TIMESTAMP(update_t) as update_t, progress,
               UNIX_TIMESTAMP(next_retry_t) as next_retry_t,
               error_data
          FROM tagged_data.platform_tagging_request
         WHERE app_id_int IN ({conn.values_arr_to_db_in(app_ids_int, int_values=True)})
        """)

        for row in rows_request:
            state = row["state"]
            retry_countdown = 0
            if state == "retry":
                retry_countdown = row["next_retry_t"] - GembaseUtils.timestamp_int()
            data_per_app_id[row["app_id_int"]] = {
                "state": row["state"],
                "request_t": row["request_t"],
                "update_t": row["update_t"],
                "progress": row["progress"],
                "retry_countdown": retry_countdown,
                "error_data": row["error_data"]
            }

        not_found = []
        for app_id_int in app_ids_int:
            if app_id_int not in data_per_app_id:
                not_found.append(app_id_int)

        if len(not_found) > 0:
            rows_tagged = conn.select_all(f"""
                SELECT t.app_id_int, UNIX_TIMESTAMP(t.t) as tagged_t
                  FROM tagged_data.platform_tagged t,
                       tagged_data.active_prompts_b b
                 WHERE t.app_id_int IN ({conn.values_arr_to_db_in(not_found, int_values=True)})
                   AND t.prompts_b = b.b
                """)

            for row in rows_tagged:
                data_per_app_id[row["app_id_int"]] = {
                    "state": "done",
                    "tagged_t": row["tagged_t"]
                }

        for app_id_int in app_ids_int:
            if app_id_int not in data_per_app_id:
                data_per_app_id[app_id_int] = {
                    "state": "not_tagged"
                }

        rows_pending_users_tags_override_request = conn.select_all(f"""
        SELECT DISTINCT app_id_int,
               FIRST_VALUE(state) over (PARTITION BY app_id_int ORDER BY request_id DESC) as state
          FROM app.users_tags_override_requests
         WHERE user_id = %s 
           AND app_id_int IN ({conn.values_arr_to_db_in(app_ids_int, int_values=True)})
        """, [user_id])

        for row in rows_pending_users_tags_override_request:
            data_per_app_id[row["app_id_int"]]["users_tags_override_request"] = {
                "state": row["state"]
            }

        return data_per_app_id

    @staticmethod
    def get_tags(
            conn: DbConnection,
            user_id: int,
            app_id_int: int
    ):

        tags = []
        tagged_t = None

        tagging_state = AppModel.get_tagging_state(
            conn=conn,
            app_id_int=app_id_int,
            user_id=user_id
        )

        if tagging_state["state"] == "done":

            tagged_t = tagging_state["tagged_t"]

            tags = AppModel.get_tags_details_bulk(
                conn=conn,
                apps_ids_int=[app_id_int]
            )[app_id_int]

        return {
            "app_id_int": app_id_int,
            "tagging": tagging_state,
            "tags": tags,
            "tagged_t": tagged_t
        }

    @staticmethod
    def get_tags_details_bulk(conn: DbConnection, apps_ids_int: [int]):

        app_ids_db = conn.values_arr_to_db_in(apps_ids_int, int_values=True)

        rows = conn.select_all(f"""
        SELECT a.app_id_int,
               a.tag_id_int, 
               a.tag_rank
          FROM tagged_data.tags_v a 
         WHERE a.app_id_int IN ({app_ids_db})
         ORDER BY a.app_id_int, a.tag_rank
        """)

        res = {}
        for app_id_int in apps_ids_int:
            res[app_id_int] = []
        for row in rows:
            res[row["app_id_int"]].append({
                "tag_id_int": row["tag_id_int"],
                "tag_rank": row["tag_rank"]
            })

        return res

    @staticmethod
    def update_concept_app(conn: DbConnection, user_id: int, app_id_int: int, app_detail_changes: {}):

        current_app_detail = AppModel.get_app_detail(
            conn=conn,
            app_id_int=app_id_int,
            user_id=user_id
        )

        for p in ["title", "description", "store", "platform"]:
            if p in app_detail_changes:
                current_app_detail[p] = app_detail_changes[p]

        conn.query("""
        UPDATE scraped_data.apps_concepts a
           SET a.title = %s,
               a.description = %s,
               a.store = %s,
               a.platform = %s,
               a.t = NOW()
         WHERE a.app_id_int = %s
        """, [current_app_detail["title"],
              current_app_detail["description"],
              current_app_detail["store"],
              current_app_detail["platform"],
              app_id_int])

        if "tags" in app_detail_changes and app_detail_changes["tags"] is not None:
            AppModel.set_manual_tags(
                conn=conn,
                user_id=user_id,
                app_id_int=app_id_int,
                tags_details=app_detail_changes["tags"]
            )

    @staticmethod
    def delete_concept_app(conn: DbConnection, app_id_int: int):

        row = conn.select_one_or_none("""
        SELECT *
          FROM scraped_data.apps_concepts
         WHERE app_id_int = %s
        """, [app_id_int])

        if row is not None:
            conn.query("""
            INSERT INTO archive.archive_data (user_id, type, data, expire_days)  VALUES (%s, %s, %s, %s)
            """, [row["user_id"], "scraped_data.apps_concepts", json.dumps(row, default=str), 30])

            conn.query("""
            INSERT INTO archive.apps_concepts
            SELECT * FROM scraped_data.apps_concepts WHERE app_id_int = %s
            """, [app_id_int])

            conn.query("""
            DELETE FROM scraped_data.apps_concepts
             WHERE app_id_int = %s
            """, [app_id_int])

            conn.query("""
            DELETE FROM scraped_data.devs_apps
             WHERE app_id_int = %s
            """, [app_id_int])

            conn.query("""
            DELETE FROM tagged_data.tags t
             WHERE t.app_id_int = %s
            """, [app_id_int])

            conn.query("""
            DELETE FROM tagged_data.platform_tagged t
             WHERE t.app_id_int = %s
            """, [app_id_int])

            conn.query("""
            DELETE FROM tagged_data.platform_tagging_request t
             WHERE t.app_id_int = %s
            """, [app_id_int])

            conn.query("""
            DELETE FROM app.users_apps WHERE app_id_int = %s
            """, [app_id_int])

    @staticmethod
    def get_devs_apps_ids_int(
            conn: DbConnection,
            user_id: int,
            devs_ids_int: list[int],
            include_concepts=False
    ):
        devs_ids_int_db = conn.values_arr_to_db_in(devs_ids_int, int_values=True)

        res = {}

        rows = conn.select_all(f"""
        SELECT da.dev_id_int, da.app_id_int
          FROM scraped_data.devs_apps da,
               scraped_data.apps_valid a
         WHERE da.dev_id_int IN ({devs_ids_int_db})
           AND a.app_id_int = da.app_id_int
        """)
        for row in rows:
            dev_id_int = row["dev_id_int"]
            if dev_id_int not in res:
                res[dev_id_int] = []
            res[dev_id_int].append(row["app_id_int"])

        if include_concepts:
            user_dev_id_int = UserModel(conn=conn, user_id=user_id).get_dev_id_int()
            if user_dev_id_int in devs_ids_int:
                rows_concepts = conn.select_all("""
                SELECT %s as dev_id_int, a.app_id_int
                  FROM scraped_data.apps_concepts a
                 WHERE a.user_id = %s
                """, [user_dev_id_int, user_id])
                for row in rows_concepts:
                    dev_id_int = row["dev_id_int"]
                    if dev_id_int not in res:
                        res[dev_id_int] = []
                    res[dev_id_int].append(row["app_id_int"])

        return res

    @staticmethod
    def update_loyalty_installs(conn: DbConnection, app_id_int: int):
        AppDataModel.update_loyalty_installs(conn=conn, app_id_int=app_id_int)

    @staticmethod
    def update_loyalty_installs_bulk(conn: DbConnection, only_where_null=False):
        if only_where_null:
            where_q = """
            loyalty_installs_t IS NULL
            """
        else:
            where_q = """
            (  
               loyalty_installs_t IS NULL 
            OR DATE_ADD(loyalty_installs_t, INTERVAL 1 MONTH ) <= NOW()
            )
            """

        conn.query(f"""
        UPDATE scraped_data.apps a
           SET a.loyalty_installs = platform.calc_loyalty_installs(a.installs, TIMESTAMPDIFF(MONTH , released, NOW()), 0.1),
               a.loyalty_installs_t = NOW()
          WHERE a.installs > 10000
            AND a.released is not null
            AND {where_q}
        """)

        conn.commit()

    @staticmethod
    def get_app_store_url(app_id_in_store: str) -> str:
        return f"https://play.google.com/store/apps/details?id={app_id_in_store}&hl=en"

    @staticmethod
    def get_app_id_int(conn: DbConnection, app_id_in_store: str) -> int | None:
        return AppDataModel.get_app_id_int(conn=conn, app_id_in_store=app_id_in_store)

    @staticmethod
    def get_or_create_app_id_int(
            conn: DbConnection,
            app_id_in_store: str,
            store: int
    ) -> int:
        return AppDataModel.get_or_create_app_id_int(conn=conn, app_id_in_store=app_id_in_store, store=store)

    @staticmethod
    def get_dev_id_int(conn: DbConnection, dev_id: str) -> int | None:
        return AppDataModel.get_dev_id_int(conn=conn, dev_id=dev_id)

    @staticmethod
    def create_next_id_atomic():
        return AppDataModel.create_next_id_atomic()

    @staticmethod
    def save_app_to_db_atomic(
            app_id_in_store: str,
            store: int,
            scraped_data: {}
    ):
        return AppDataModel.save_app_to_db_atomic(app_id_in_store=app_id_in_store, store=store, scraped_data=scraped_data)

    parsed_prompts_def = None

    @staticmethod
    def parse_prompts_def(sheet_data):
        areas = PrivateDataModel.get_private_data()["google"]["google_docs"]["prompts"]["areas"]
        parsed_def = []
        for area_name in areas:
            area = {
                'name': area_name,
                'categories': []
            }
            parsed_def.append(area)
            categories = {}
            for it in sheet_data[area_name]:
                c_id = it["ID"]
                if c_id is None or c_id == "":
                    continue
                c_name = it['Category']
                t_name = it['Tag']
                if c_name not in categories:
                    c_obj = {
                        'name': c_name,
                        'tags': []
                    }
                    categories[c_name] = c_obj
                    area['categories'].append(c_obj)
                else:
                    c_obj = categories[c_name]

                tag_obj = {
                    'id': c_id,
                    'name': t_name,
                    'examples': it['Examples']
                }
                c_obj['tags'].append(tag_obj)

        return parsed_def

    @staticmethod
    def is_tagged(conn: DbConnection, app_id_int: int):
        return AppModel.get_tagging_state(
            conn=conn,
            app_id_int=app_id_int,
            user_id=-1
        )["state"] == "done"

    @staticmethod
    def get_tag_details_bulk(conn: DbConnection, app_ids: []):
        app_ids_db = conn.values_arr_to_db_in(app_ids)

        rows = conn.select_all(f"""
        SELECT t.app_id_int, t.tag_id_int, t.tag_rank
          FROM tagged_data.tags_v t,
               tagged_data.platform_tagged pt
         WHERE t.app_id_int IN ({app_ids_db})
           AND pt.app_id_int = t.app_id_int
        """)

        res = {}
        for app_id in app_ids:
            res[app_id] = []

        for row in rows:
            res[row["app_id_int"]].append({
                "tag_id_int": row["tag_id_int"],
                "tag_rank": row["tag_rank"]
            })

        return res

    @staticmethod
    def get_tags_list(conn: DbConnection, user_id: int, app_id_int: int) -> []:
        tags = AppModel.get_tags(conn, user_id, app_id_int)
        tags_list = []
        for it in tags["tags"]:
            tags_list.append(it["tag_id_int"])
        return tags_list

    @staticmethod
    def is_primary_tag(tag_rank: int) -> bool:
        return tag_rank == 1

    @staticmethod
    def has_tag(tag_details: [], tag_id_int: int) -> bool:
        for tag_detail in tag_details:
            if tag_detail["tag_id_int"] == tag_id_int:
                return True
        return False

    @staticmethod
    def get_tier_2(conn: DbConnection, app_id_int: int) -> int:
        return conn.select_one("""
        SELECT scraped_data.get_app_tier(%s) as tier FROM dual
        """, [app_id_int])["tier"]

    @staticmethod
    def get_devs_details(conn: DbConnection, devs_ids_int: []):
        return AppDataModel.get_devs_details(
            conn=conn,
            devs_ids_int=devs_ids_int
        )

    @staticmethod
    def get_primary_dev(conn: DbConnection, app_id_int: int) -> {}:
        row = conn.select_one_or_none("""
        SELECT da.dev_id_int, d.store, d.dev_id_in_store
          FROM scraped_data.devs_apps da,
               scraped_data.devs d
         WHERE da.app_id_int = %s
           AND da.primary_dev = 1
           AND da.dev_id_int = d.dev_id_int
        """, [app_id_int])
        return row

    @staticmethod
    def get_primary_dev_id_int(conn: DbConnection, app_id_int: int) -> int | None:
        res = AppModel.get_primary_dev(conn=conn, app_id_int=app_id_int)
        if res is not None:
            return res[UserObfuscator.DEV_ID_INT]
        return None

    @staticmethod
    def create_dev_concept(conn: DbConnection, title: str | None):
        dev_id = GembaseUtils.get_guid()
        if title is None:
            title = dev_id
        dev_id_int = AppDataModel.get_or_create_dev_id_int(
            conn=conn,
            dev_id=dev_id
        )
        conn.query("""
        INSERT INTO scraped_data.devs_concepts (dev_id_int, title)
        VALUES (%s, %s)
        """, [dev_id_int, title])

        conn.query("""
        INSERT INTO scraped_data.devs_devs (parent_dev_id_int, child_dev_id_int) VALUES (%s, %s)
        """, [dev_id_int, dev_id_int])

        return {
            "dev_id_int": dev_id_int,
            "dev_id": dev_id
        }

    @staticmethod
    def get_tag_id_for_store(store: int) -> int | None:
        if store == AppModel.STORE__GOOGLE_PLAY:
            return TagsConstants.PLATFORM_MOBILE
        elif store == AppModel.STORE__STEAM:
            return TagsConstants.PLATFORM_PC
        return None

