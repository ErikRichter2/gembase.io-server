import json
import os

from gembase_server_core.db.db_connection import DbConnection
from src.server.models.apps.app_model import AppModel
from src.server.models.apps.app_tag_model import AppTagModel
from src.server.models.tags.prompts.default_prompt_handler import DefaultPromptHandler
from src.server.models.tags.prompts.priority_tags_prompt_handler import PriorityTagsPromptHandler
from src.server.models.tags.prompts.prompts_def import PromptsDef
from src.server.models.tags.tags_mapper import TagsMapper
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.server.models.tags.tags_constants import TagsConstants
from src.server.models.quota.internal_batch_quota_context import InternalBatchQuotaContext
from src.server.models.quota.user_quota_context import UserQuotaContext
from src.server.models.user.user_constants import uc
from src.server.models.user.user_model import UserModel
from src.utils.gembase_utils import GembaseUtils


class TaggerServiceModel:

    def __init__(self, conn: DbConnection):
        self.conn = conn
        self.__prompts_helper: PromptsDef | None = None
        self.__map_initialized = False
        self.__override_s2t = {}
        self.__override_t2s = {}
        self.__append_s2t = {}
        self.__append_t2s = {}
        self.__map_tags = None

    def apply_store_tags(
            self,
            tags_details: [],
            app_id_int: int
    ):
        row_app_store = self.conn.select_one_or_none("""
                SELECT a.store
                  FROM scraped_data.apps a
                 WHERE a.app_id_int = %s
                """, [app_id_int])

        if row_app_store is None or row_app_store["store"] != AppModel.STORE__GOOGLE_PLAY:
            return {
                "tags_details": tags_details
            }

        app_store = row_app_store["store"]

        rows = self.conn.select_all("""
        SELECT ast.store_tag_id
          FROM scraped_data.apps_store_tags ast
         WHERE ast.app_id_int = %s
        """, [app_id_int])
        app_store_tags = [row["store_tag_id"] for row in rows]
        has_store_tags = len(app_store_tags) > 0

        if not self.__map_initialized:
            self.__map_initialized = True
            self.__map_tags = TagsMapper(conn=self.conn).map_tags

            rows_map_store_tags = self.conn.select_all("""
                SELECT d.tag_id_int, d.store_tag_id, d.type, dd.store
                  FROM scraped_data.def_map_store_tags d,
                       scraped_data.def_store_tags dd
                 WHERE dd.id = d.store_tag_id
                """)

            # TagsConstants.SUBCATEGORY_GENRE_ID, TagsConstants.SUBCATEGORY_TOPICS_ID

            for row in rows_map_store_tags:
                tag_id_int = row["tag_id_int"]
                store_tag_id = row["store_tag_id"]
                store_tag_type = row["type"]
                store = row["store"]

                if store not in self.__override_t2s:
                    self.__override_t2s[store] = {}
                if store not in self.__override_s2t:
                    self.__override_s2t[store] = {}
                if store not in self.__append_t2s:
                    self.__append_t2s[store] = {}
                if store not in self.__append_s2t:
                    self.__append_s2t[store] = {}

                if store_tag_type == "override":
                    if tag_id_int not in self.__override_t2s[store]:
                        self.__override_t2s[store][tag_id_int] = []
                    self.__override_t2s[store][tag_id_int].append(store_tag_id)
                    if store_tag_id not in self.__override_s2t[store]:
                        self.__override_s2t[store][store_tag_id] = []
                    self.__override_s2t[store][store_tag_id].append(tag_id_int)
                elif store_tag_type == "append":
                    if tag_id_int not in self.__append_t2s[store]:
                        self.__append_t2s[store][tag_id_int] = []
                    self.__append_t2s[store][tag_id_int].append(store_tag_id)
                    if store_tag_id not in self.__append_s2t[store]:
                        self.__append_s2t[store][store_tag_id] = []
                    self.__append_s2t[store][store_tag_id].append(tag_id_int)

        def sort_rank(e):
            sort_val = 4 if e["tag_rank"] == 0 else e["tag_rank"]
            if has_store_tags and e["tag_id_int"] not in self.__override_t2s[app_store] and e["tag_id_int"] not in self.__append_t2s[app_store]:
                sort_val += 100
            return sort_val

        genres_tags = self.__map_tags["subci2i"][TagsConstants.SUBCATEGORY_GENRE_ID]
        topics_tags = self.__map_tags["subci2i"][TagsConstants.SUBCATEGORY_TOPICS_ID]

        genre_ranks_orig = [tag_detail for tag_detail in tags_details if tag_detail["tag_rank"] != 0 and tag_detail["tag_id_int"] in genres_tags]
        genre_ranks_orig.sort(key=sort_rank)
        genre_ranks_orig = [tag_detail["tag_id_int"] for tag_detail in genre_ranks_orig]

        topics_ranks_orig = [tag_detail for tag_detail in tags_details if tag_detail["tag_rank"] != 0 and tag_detail["tag_id_int"] in topics_tags]
        topics_ranks_orig.sort(key=sort_rank)
        topics_ranks_orig = [tag_detail["tag_id_int"] for tag_detail in topics_ranks_orig]

        tag_ranks_orig = {}
        for tag_detail in tags_details:
            if tag_detail["tag_rank"] != 0:
                tag_ranks_orig[tag_detail["tag_id_int"]] = tag_detail["tag_rank"]

        app_tags_ids = [it["tag_id_int"] for it in tags_details]

        tags_added = []
        tags_removed = []

        if has_store_tags:

            for store_tag_id in app_store_tags:
                if app_store in self.__override_s2t:
                    if store_tag_id in self.__override_s2t[app_store]:
                        for tag_id_int in self.__override_s2t[app_store][store_tag_id]:
                            if tag_id_int not in tags_added and tag_id_int not in app_tags_ids:
                                tags_added.append(tag_id_int)
                if app_store in self.__append_s2t:
                    if store_tag_id in self.__append_s2t[app_store]:
                        for tag_id_int in self.__append_s2t[app_store][store_tag_id]:
                            if tag_id_int not in tags_added and tag_id_int not in app_tags_ids:
                                tags_added.append(tag_id_int)

            if app_store in self.__override_t2s:
                for tag_id_int in app_tags_ids:
                    if tag_id_int in self.__override_t2s[app_store]:
                        found = False
                        for store_tag_id in app_store_tags:
                            if store_tag_id in self.__override_t2s[app_store][tag_id_int]:
                                found = True
                                break
                        if not found and tag_id_int not in tags_removed:
                            tags_removed.append(tag_id_int)

        tags_per_subci = {}
        for tag_detail in tags_details:
            tag_id_int = tag_detail["tag_id_int"]
            if tag_id_int in self.__map_tags["i2subci"]:
                subci = self.__map_tags["i2subci"][tag_id_int]
                if subci not in tags_per_subci:
                    tags_per_subci[subci] = []
                tags_per_subci[subci].append(tag_id_int)

        for tag_id_int in tags_added:
            if tag_id_int in self.__map_tags["i2subci"]:
                subci = self.__map_tags["i2subci"][tag_id_int]
                if subci not in tags_per_subci:
                    tags_per_subci[subci] = []
                tags_per_subci[subci].append(tag_id_int)

        for tag_id_int in tags_removed:
            if tag_id_int in self.__map_tags["i2subci"]:
                subci = self.__map_tags["i2subci"][tag_id_int]
                if subci in tags_per_subci and tag_id_int in tags_per_subci[subci]:
                    tags_per_subci[subci].remove(tag_id_int)

        for tag_id_int in tags_added:
            if tag_id_int not in app_tags_ids:
                if tag_id_int in self.__map_tags["i2subci"]:
                    subci = self.__map_tags["i2subci"][tag_id_int]
                    if subci in tags_per_subci and len(tags_per_subci[subci]) == 0:
                        continue
                app_tags_ids.append(tag_id_int)
                tags_details.append({
                    "tag_id_int": tag_id_int,
                    "tag_rank": 0,
                    "added_from_store": 1,
                })

        for tag_detail in tags_details:
            tag_id_int = tag_detail["tag_id_int"]
            if tag_id_int in tags_removed:
                if tag_id_int in self.__map_tags["i2subci"]:
                    subci = self.__map_tags["i2subci"][tag_id_int]
                    if subci in tags_per_subci and len(tags_per_subci[subci]) == 0:
                        continue
                app_tags_ids.remove(tag_id_int)
                tag_detail["removed_from_store"] = 1

        genre_ranks = [tag_detail for tag_detail in tags_details if tag_detail["tag_id_int"] in app_tags_ids and tag_detail["tag_rank"] != 0 and tag_detail["tag_id_int"] in genres_tags]
        topics_ranks = [tag_detail for tag_detail in tags_details if tag_detail["tag_id_int"] in app_tags_ids and tag_detail["tag_rank"] != 0 and tag_detail["tag_id_int"] in topics_tags]

        for tag_detail in tags_details:
            def has_tag(t: int, a: []):
                for it in a:
                    if it["tag_id_int"] == t:
                        return True
                return False

            tag_id_int = tag_detail["tag_id_int"]
            if tag_id_int in app_tags_ids:
                if app_store in self.__override_t2s and tag_id_int in self.__override_t2s[app_store] or app_store in self.__append_t2s and tag_id_int in self.__append_t2s[app_store]:
                    if tag_id_int in genres_tags:
                        if not has_tag(tag_id_int, genre_ranks):
                            genre_ranks.append(tag_detail)
                    if tag_id_int in topics_tags:
                        if not has_tag(tag_id_int, topics_ranks):
                            topics_ranks.append(tag_detail)

        all_genres = [tag_id_int for tag_id_int in app_tags_ids if tag_id_int in genres_tags]
        all_topics = [tag_id_int for tag_id_int in app_tags_ids if tag_id_int in topics_tags]

        genre_ranks.sort(key=sort_rank)
        genre_ranks = genre_ranks[:3]
        genre_ranks = [tag_detail["tag_id_int"] for tag_detail in genre_ranks]

        if len(genre_ranks) < 3:
            for tag_id_int in all_genres:
                if tag_id_int not in genre_ranks:
                    genre_ranks.append(tag_id_int)
                    if len(genre_ranks) >= 3:
                        break

        topics_ranks.sort(key=sort_rank)
        topics_ranks = topics_ranks[:3]
        topics_ranks = [tag_detail["tag_id_int"] for tag_detail in topics_ranks]

        if len(topics_ranks) < 3:
            for tag_id_int in all_topics:
                if tag_id_int not in topics_ranks:
                    topics_ranks.append(tag_id_int)
                    if len(topics_ranks) >= 3:
                        break

        tag_ranks_store = {
            "genres": genre_ranks,
            "topics": topics_ranks
        }
        tag_ranks_prev = {
            "genres": genre_ranks_orig,
            "topics": topics_ranks_orig
        }

        for tag_detail in tags_details:
            tag_id_int = tag_detail["tag_id_int"]
            tag_rank_orig = tag_ranks_orig[tag_id_int] if tag_id_int in tag_ranks_orig else 0

            tag_rank_store = 0
            if tag_id_int in genre_ranks:
                tag_rank_store = genre_ranks.index(tag_id_int) + 1
            if tag_id_int in topics_ranks:
                tag_rank_store = topics_ranks.index(tag_id_int) + 1

            tag_detail["tag_rank"] = tag_rank_orig
            tag_detail["tag_rank_override"] = tag_rank_store
            tag_detail["is_tag_rank_override"] = 1

        return {
            "tags_details": tags_details,
            "tag_ranks_orig": tag_ranks_prev,
            "tag_ranks_store": tag_ranks_store,
            "all_genres": all_genres,
            "all_topics": all_topics
        }

    def get_helper(self) -> PromptsDef:
        if self.__prompts_helper is None:
            self.__prompts_helper = PromptsDef(conn=self.conn)
        return self.__prompts_helper

    def process(self):
        while True:
            self.conn.rollback()
            self.conn.query("""
            UPDATE app_temp_data.server_services
            SET heartbeat_child = NOW()
            WHERE pid = %s
            """, [os.getpid()])
            self.conn.commit()
            if not self.__finalize_done_requests():
                if not self.__process_request():
                    break

    def __finalize_done_requests(self):
        rows_requests = self.conn.select_all("""
        SELECT r.id, r.app_id_int, p.prompt_id, p.parsed_result, d.rank_prompt, r.remove_only_prompts_tags
          FROM tagged_data.platform_tagging_request r,
               tagged_data.platform_tagging_prompts p,
               tagged_data.def_prompts d
         WHERE r.state = 'working'
           AND r.id = p.request_id
           AND p.prompt_id = d.prompt_id
           AND NOT EXISTS (
               SELECT 1
                 FROM tagged_data.platform_tagging_prompts pp
                WHERE pp.request_id = r.id
                  AND pp.state != 'done'
           )
        """)

        if len(rows_requests) == 0:
            return False

        results_per_app = {}
        requests_ids = []
        for row in rows_requests:
            if row["id"] not in requests_ids:
                requests_ids.append(row["id"])
            app_id_int = row["app_id_int"]
            if app_id_int not in results_per_app:
                results_per_app[app_id_int] = {
                    "prompts": {},
                    "remove_only_prompts_tags": row["remove_only_prompts_tags"]
                }
            prompt_tags = []
            if row["rank_prompt"] != 1:
                prompt_tags = self.get_helper().get_prompt_tags(row["prompt_id"])
            results_per_app[app_id_int]["prompts"][row["prompt_id"]] = {
                "parsed": json.loads(row["parsed_result"]),
                "prompt_tags": prompt_tags,
                "rank_prompt": row["rank_prompt"],
            }

        for app_id_int in results_per_app:
            all_tags = []
            tags = {}
            remove_tags = []
            for prompt_id in results_per_app[app_id_int]["prompts"]:
                r = results_per_app[app_id_int]["prompts"][prompt_id]
                if r["rank_prompt"] == 1:
                    for tag_detail in r["parsed"]:
                        tag_id_int = tag_detail["tag_id_int"]
                        remove_tags.append(tag_id_int)
                        tags[tag_id_int] = tag_detail["tag_rank"]
                        if tag_id_int not in all_tags:
                            all_tags.append(tag_id_int)
                else:
                    remove_tags += r["prompt_tags"]
                    for tag_id_int in r["parsed"]:
                        if tag_id_int not in tags:
                            tags[tag_id_int] = 0
                            if tag_id_int not in all_tags:
                                all_tags.append(tag_id_int)

            tags_details = []
            for tag_id_int in tags:
                tags_details.append({
                    "tag_id_int": tag_id_int,
                    "tag_rank": tags[tag_id_int]
                })

            self.apply_store_tags(
                tags_details=tags_details,
                app_id_int=app_id_int
            )

            if results_per_app[app_id_int]["remove_only_prompts_tags"] == 0:
                remove_tags = None

            app_detail = AppModel.get_app_detail(
                conn=self.conn,
                app_id_int=app_id_int
            )
            if app_detail is not None:

                # platform tags

                platform_tag_id = AppModel.get_tag_id_for_store(store=app_detail["store"])
                if platform_tag_id is None:
                    row_user_id = self.conn.select_one_or_none("""
                    SELECT user_id FROM scraped_data.apps_concepts WHERE app_id_int = %s
                    """, [app_id_int])
                    if row_user_id is not None:
                        user = UserModel(conn=self.conn, user_id=row_user_id["user_id"])
                        dev_id_int = user.get_dev_id_int()
                        row_store = self.conn.select_one_or_none("""
                        SELECT z1.store FROM (
                        SELECT a.store, count(1) as cnt
                          FROM scraped_data.devs_apps da,
                               scraped_data.apps a
                         WHERE da.app_id_int = a.app_id_int
                           AND da.dev_id_int = %s
                         GROUP BY a.store ) z1
                         ORDER BY z1.cnt, z1.store
                         LIMIT 1
                        """, [dev_id_int])
                        if row_store is not None:
                            platform_tag_id = AppModel.get_tag_id_for_store(store=row_store["store"])

                if platform_tag_id is None:
                    platform_tag_id = TagsConstants.PLATFORM_MOBILE

                if platform_tag_id is not None:
                    tags_details.append({
                        "tag_id_int": platform_tag_id,
                        "tag_rank": 0
                    })

                # monetization tags

                if app_detail["premium"] == 1:
                    tags_details.append({
                        "tag_id_int": TagsConstants.MONETIZATION_PREMIUM,
                        "tag_rank": 0
                    })
                else:
                    tags_details.append({
                        "tag_id_int": TagsConstants.MONETIZATION_FREE,
                        "tag_rank": 0
                    })

                if app_detail["ads"] == 1:
                    tags_details.append({
                        "tag_id_int": TagsConstants.MONETIZATION_ADS,
                        "tag_rank": 0
                    })

                if app_detail["iap"] == 1:
                    tags_details.append({
                        "tag_id_int": TagsConstants.MONETIZATION_IAP,
                        "tag_rank": 0
                    })

            AppModel.set_tags(
                conn=self.conn,
                user_id=0,
                app_id_int=app_id_int,
                tags_details=tags_details,
                context="tagger_service",
                remove_tags=remove_tags
            )

        requests_ids_db = self.conn.values_arr_to_db_in(requests_ids, int_values=True)
        self.conn.query(f"""
        DELETE FROM tagged_data.platform_tagging_request r
        WHERE r.id IN ({requests_ids_db})
        """)
        self.conn.commit()

        return True

    def __process_request(self) -> bool:

        if self.conn.select_one("""
        SELECT count(1) as cnt FROM tagged_data.platform_tagging_request
        """)["cnt"] == 0 and self.conn.select_one("""
        SELECT count(1) as cnt FROM tagged_data.platform_tagging_batch
        """)["cnt"] == 0:
            return False

        row = self.conn.select_one_or_none("""
        SELECT r.id, r.app_id_int, r.state, r.from_batch, r.only_priority_prompt
          FROM tagged_data.platform_tagging_request r
         WHERE r.state = 'queue'
         ORDER BY r.request_t
         LIMIT 1
        """)

        if row is None:
            row = self.conn.select_one_or_none("""
            SELECT r.id, r.app_id_int, r.state, r.from_batch, r.only_priority_prompt
              FROM tagged_data.platform_tagging_request r
             WHERE ((r.state = 'retry' AND r.next_retry_t < NOW()) 
                OR (r.state = 'working' AND DATE_ADD(r.update_t, INTERVAL 15 MINUTE ) < NOW()))
             ORDER BY r.update_t
             LIMIT 1
            """)

        if row is None:
            row = self.conn.select_one_or_none("""
            SELECT b.app_id_int, b.only_priority_prompt
              FROM tagged_data.platform_tagging_batch b
             LIMIT 1
            """)

            if row is not None:
                app_id_int = row["app_id_int"]
                only_priority_prompt = row["only_priority_prompt"]

                self.conn.query("""
                DELETE FROM tagged_data.platform_tagging_batch b
                 WHERE b.app_id_int = %s
                """, [app_id_int])
                if only_priority_prompt == 1 or self.conn.select_one_or_none("""
                SELECT 1 FROM tagged_data.platform_tagged p,
                tagged_data.active_prompts_b b
                WHERE p.app_id_int = %s
                  AND p.prompts_b = b.b
                """, [app_id_int]) is None:
                    self.conn.query("""
                    LOCK TABLE tagged_data.platform_tagging_request WRITE
                    """)
                    if self.conn.select_one_or_none("""
                    SELECT 1
                      FROM tagged_data.platform_tagging_request
                     WHERE app_id_int = %s
                    """, [app_id_int]) is None:
                        self.conn.query("""
                        INSERT INTO tagged_data.platform_tagging_request (app_id_int, state, from_batch, only_priority_prompt) 
                        VALUES (%s, 'queue', 1, %s)
                        """, [app_id_int, only_priority_prompt])
                    self.conn.unlock_tables()
                self.conn.commit()
                return True

        if row is None:
            return False

        request_id = row["id"]
        app_id_int = row["app_id_int"]
        state = row["state"]
        from_batch = row["from_batch"]
        only_priority_prompt = row["only_priority_prompt"]

        if state == "queue":

            row_app = self.conn.select_one_or_none("""
            SELECT m.app_id_in_store, m.store,
            IF(a.scraped_t IS NULL, 0, 1) as was_scraped
              FROM app.map_app_id_to_store_id m,
                   scraped_data.apps a
             WHERE m.app_id_int = %s
               AND a.app_id_int = m.app_id_int
            """, [app_id_int])

            if row_app is not None:
                res = ScraperModel.scrap_app(
                    conn=self.conn,
                    app_id_in_store=row_app["app_id_in_store"],
                    store=row_app["store"]
                )
                if res["state"] != 1:

                    if res["state"] == -1:
                        self.conn.query("""
                        UPDATE scraped_data.apps a
                           SET a.removed_from_store = NOW()
                         WHERE a.app_id_int = %s
                        """, [app_id_int])
                        self.conn.commit()

                    if row_app["was_scraped"] == 0:
                        self.conn.query("""
                        UPDATE tagged_data.platform_tagging_request r
                           SET r.state = 'error',
                               r.update_t = NOW(),
                               r.next_retry_t = NULL,
                               r.error_data = 'scrap_error'
                         WHERE r.id = %s
                        """, [request_id])

                        self.conn.commit()
                        return True

            self.conn.query("""
            DELETE FROM tagged_data.platform_tagging_prompts p
             WHERE p.request_id = %s
            """, [request_id])

            current_app_b = 0

            if from_batch:
                row_b = self.conn.select_one_or_none("""
                SELECT pt.prompts_b
                  FROM tagged_data.platform_tagged pt
                 WHERE pt.app_id_int = %s
                """, [app_id_int])
                if row_b is not None:
                    current_app_b = row_b["prompts_b"]

                if current_app_b != 0:
                    self.conn.query("""
                    UPDATE tagged_data.platform_tagging_request
                       SET remove_only_prompts_tags = 1
                     WHERE id = %s
                    """, [request_id])
                    self.conn.commit()

            rows_def = self.conn.select_all("""
            SELECT d.id, d.prompt_id, d.order_by, d.b, d.active
              FROM tagged_data.def_prompts d
            """)

            def get_b_for_prompt_id(prompt_id: str):
                for row in rows_def:
                    if row["prompt_id"] == prompt_id:
                        return row["b"]
                return None

            if only_priority_prompt:
                self.conn.query("""
                INSERT INTO tagged_data.platform_tagging_prompts (request_id, prompt_id, state, order_by) 
                VALUES (%s, %s, 'queue', %s)
                """, [request_id, AppTagModel.PROMPT_PRIORITY_TAGS, 0])
            else:
                bulk_data = []
                for row in rows_def:
                    b = row["b"]
                    prompt_id = row["prompt_id"]
                    if current_app_b & b == 0 and row["active"] == 1:
                        if prompt_id == "gpt4_themes":
                            gpt4_topics_only_b = get_b_for_prompt_id("gpt4_topics_only")
                            if gpt4_topics_only_b is not None and current_app_b & gpt4_topics_only_b != 0:
                                prompt_id = "gpt4_themes_no_topics"
                        bulk_data.append((request_id, prompt_id, row["order_by"]))
                self.conn.bulk("""
                INSERT INTO tagged_data.platform_tagging_prompts (request_id, prompt_id, state, order_by) 
                VALUES (%s, %s, 'queue', %s)
                """, bulk_data)

        self.conn.query("""
        UPDATE tagged_data.platform_tagging_request r
           SET r.state = 'working',
               r.update_t = NOW(),
               r.next_retry_t = NULL
         WHERE r.id = %s
        """, [request_id])
        self.conn.commit()

        rows_prompts = self.conn.select_all("""
        SELECT p.prompt_id, p.retry_prompt_row_id, p.order_by
          FROM tagged_data.platform_tagging_prompts p
         WHERE p.request_id = %s
           AND (p.state = 'working' OR p.state = 'queue' OR p.state = 'retry')
         ORDER BY p.order_by
        """, [request_id])

        if len(rows_prompts) == 0:
            error_cnt = self.conn.select_one("""
                SELECT count(1) as cnt
                  FROM tagged_data.platform_tagging_prompts p
                 WHERE p.request_id = %s
                   AND p.state = 'error'
                """, [request_id])["cnt"]
            if error_cnt > 0:
                self.conn.query("""
                UPDATE tagged_data.platform_tagging_request r
                   SET r.state = 'error',
                       r.update_t = NOW()
                 WHERE r.id = %s
                """, [request_id])
            else:
                self.conn.query("""
                UPDATE tagged_data.platform_tagging_request r
                   SET r.state = 'working',
                       r.update_t = NOW()
                 WHERE r.id = %s
                """, [request_id])
            self.conn.commit()

            return True

        # todo quota
        quota_context = InternalBatchQuotaContext(UserQuotaContext.QUOTA_OPENAI, "tag app")
        batch_user = uc.get_system_batch_user_id()

        for row in rows_prompts:

            prompt_id = row["prompt_id"]
            retry_prompt_row_id = row["retry_prompt_row_id"]
            order_by = row["order_by"]
            prompt_row_id = None

            progress = round((order_by / 7) * 100)

            self.conn.query("""
            UPDATE tagged_data.platform_tagging_request r
               SET r.state = 'working',
                   r.update_t = NOW(),
                   r.progress = %s
             WHERE r.id = %s
            """, [progress, request_id])

            self.conn.query("""
            UPDATE tagged_data.platform_tagging_prompts p
               SET p.state = 'working',
                   p.update_t = NOW()
             WHERE p.request_id = %s
               AND p.prompt_id = %s
            """, [request_id, prompt_id])
            self.conn.commit()

            error_data = None
            max_retry_count_exceeded = False

            if prompt_id == AppTagModel.PROMPT_PRIORITY_TAGS:

                if only_priority_prompt:
                    rows_tags = self.conn.select_all("""
                    SELECT tag_id_int
                      FROM tagged_data.tags_v
                     WHERE app_id_int = %s
                    """, [app_id_int])
                    tags_ids = [row["tag_id_int"] for row in rows_tags]
                else:
                    row_genres = self.conn.select_one_or_none("""
                    SELECT p.parsed_result
                      FROM tagged_data.platform_tagging_prompts p
                     WHERE p.request_id = %s
                       AND p.prompt_id = %s
                       AND p.state = 'done'
                    """, [request_id, AppTagModel.PROMPT_GENRES])

                    row_topics = self.conn.select_one_or_none("""
                    SELECT p.parsed_result
                      FROM tagged_data.platform_tagging_prompts p
                     WHERE p.request_id = %s
                       AND p.prompt_id = %s
                       AND p.state = 'done'
                    """, [request_id, AppTagModel.PROMPT_THEMES])

                    if row_genres is None or row_topics is None:
                        self.conn.query("""
                        UPDATE tagged_data.platform_tagging_request r
                           SET r.state = 'error',
                               r.update_t = NOW()
                         WHERE r.id = %s
                        """, [request_id])
                        self.conn.commit()
                        return True

                    self.conn.rollback()

                    parsed_genres = json.loads(row_genres["parsed_result"])
                    parsed_topics = json.loads(row_topics["parsed_result"])

                    tags_ids = parsed_topics + parsed_genres

                prompt_handler = PriorityTagsPromptHandler(
                    conn=self.conn,
                    user_id=batch_user,
                    quota_context=quota_context,
                    app_id_int=app_id_int,
                    tags_ids_int=tags_ids,
                    retry_prompt_row_id=retry_prompt_row_id,
                    prompt_id=prompt_id
                )
                res = prompt_handler.run()
                prompt_row_id = prompt_handler.prompt_row_id

                if res["state"] == "done":
                    parsed_prompt_result = prompt_handler.parse_result()
                    if parsed_prompt_result["state"] == "done":
                        self.conn.query("""
                        UPDATE tagged_data.platform_tagging_prompts p
                           SET p.state = 'done',
                               p.parsed_result = %s,
                               p.update_t = NOW(),
                               p.prompt_row_id = %s
                         WHERE p.request_id = %s
                           AND p.prompt_id = %s
                        """, [json.dumps(parsed_prompt_result["tags_details"]), prompt_row_id, request_id, prompt_id])
                    else:
                        error_data = parsed_prompt_result
                else:
                    error_data = res
                    if prompt_handler.max_retry_count_exceeded:
                        max_retry_count_exceeded = True
            else:

                self.conn.rollback()

                prompt_handler = DefaultPromptHandler(
                    conn=self.conn,
                    user_id=batch_user,
                    quota_context=quota_context,
                    app_id_int=app_id_int,
                    prompt_id=prompt_id,
                    retry_prompt_row_id=retry_prompt_row_id
                )
                res = prompt_handler.run()
                prompt_row_id = prompt_handler.prompt_row_id
                if res["state"] == "done":
                    parsed_prompt_result = prompt_handler.parse_result()
                    if parsed_prompt_result["state"] == "done":
                        tags = parsed_prompt_result["tags"]
                        self.conn.query("""
                        UPDATE tagged_data.platform_tagging_prompts p
                           SET p.state = 'done',
                               p.parsed_result = %s,
                               p.update_t = NOW(),
                               p.prompt_row_id = %s
                         WHERE p.request_id = %s
                           AND p.prompt_id = %s
                        """, [json.dumps(tags), prompt_row_id, request_id, prompt_id])
                    else:
                        error_data = parsed_prompt_result
                else:
                    error_data = res
                    if prompt_handler.max_retry_count_exceeded:
                        max_retry_count_exceeded = True

            self.conn.commit()

            if error_data is not None:
                if prompt_row_id is None:
                    prompt_row_id = retry_prompt_row_id
                if max_retry_count_exceeded:
                    self.conn.query("""
                    UPDATE tagged_data.platform_tagging_prompts p
                       SET p.state = 'error',
                           p.error_data = %s,
                           p.update_t = NOW(),
                           p.prompt_row_id = %s
                     WHERE p.request_id = %s
                       AND p.prompt_id = %s
                    """, [json.dumps(error_data), prompt_row_id, request_id, prompt_id])
                    self.conn.query("""
                    UPDATE tagged_data.platform_tagging_request r
                       SET r.state = 'error',
                           r.update_t = NOW()
                     WHERE r.id = %s
                    """, [request_id])
                else:
                    self.conn.query("""
                    UPDATE tagged_data.platform_tagging_prompts p
                       SET p.state = 'retry',
                           p.error_data = %s,
                           p.update_t = NOW(),
                           p.prompt_row_id = %s,
                           p.retry_prompt_row_id = %s
                     WHERE p.request_id = %s
                       AND p.prompt_id = %s
                    """, [json.dumps(error_data), prompt_row_id, prompt_row_id, request_id, prompt_id])
                    self.conn.query("""
                    UPDATE tagged_data.platform_tagging_request r
                       SET r.state = 'retry',
                           r.update_t = NOW(),
                           r.next_retry_t = DATE_ADD(NOW(), INTERVAL 1 HOUR)
                     WHERE r.id = %s
                    """, [request_id])
                self.conn.commit()
                return True

        return True


def gpt_tagger_service_process():

    GembaseUtils.log_service(f"Gpt tagger service START")
    conn = ServiceWrapperModel.create_conn()

    TaggerServiceModel(conn=conn).process()

    ServiceWrapperModel.close_conn(conn_id=conn.connection_id(), conn=conn)
    GembaseUtils.log_service(f"Gpt tagger service END")


def default_method(*args, **kwargs):
    gpt_tagger_service_process()
