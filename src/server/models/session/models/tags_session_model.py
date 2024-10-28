from src.external_api.gmail import GbEmailService
from src.server.models.apps.app_model import AppModel
from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.platform_values.cache.platform_values_cache import PlatformValuesCache
from src.server.models.services.service_wrapper_model import ServiceWrapperModel
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.tags.tags_def import TagsDef
from src.server.models.user.user_obfuscator import UserObfuscator
from src.utils.gembase_utils import GembaseUtils


class TagsSessionModel(BaseSessionModel):

    TAGGING_CONTEXT_ENUM = ["auditor", "games_explorer"]

    def __init__(self, session):
        super(TagsSessionModel, self).__init__(session)
        self.__tags_def: TagsDef | None = None

    def create_user_override_request(
            self,
            app_id_int: int,
            tags_details: [],
            auto_confirm=False
    ):
        self.conn().query("""
        DELETE FROM app.users_tags_override_requests
         WHERE user_id = %s 
           AND app_id_int = %s
           AND state = 'pending'
        """, [self.user_id(), app_id_int])

        request_id = self.conn().insert("""
        INSERT INTO app.users_tags_override_requests (user_id, app_id_int, state)
        VALUES (%s, %s, 'pending')
        """, [self.user_id(), app_id_int])

        self.conn().query("""
        INSERT INTO app.users_tags_override_requests_tags (request_id, tags_before, tag_id_int, tag_rank)
        SELECT %s as request_id, 1 as tags_before, t.tag_id_int, t.tag_rank 
          FROM tagged_data.tags_v t
        WHERE t.app_id_int = %s
        """, [request_id, app_id_int])

        bulk_data = []
        for o in tags_details:
            bulk_data.append((request_id, 0, o["tag_id_int"], o["tag_rank"]))
        self.conn().bulk("""
        INSERT INTO app.users_tags_override_requests_tags (request_id, tags_before, tag_id_int, tag_rank)
        VALUES (%s, %s, %s, %s)
        """, bulk_data)

        if auto_confirm:
            self.confirm_user_tags_override_request(
                request_id=request_id,
                tags=tags_details
            )

        app_detail = AppModel.get_app_detail(conn=self.conn(), app_id_int=app_id_int)
        if app_detail is not None:
            body = f"""
            App: {app_detail["title"]} <br>
            User: {self.session().user().get_name()} {self.session().user().get_email()} <br>
            Detail: {GembaseUtils.client_url_root()}/admin/requested-app-labels-changes?requestId={request_id}
            """

            GbEmailService.send_mail(
                subject=f"Request for app labels override{' [AUTO-CONFIRMED]' if auto_confirm else ''}",
                body=body,
                is_html=True,
                to_system=True,
                to_address=[]
            )

        row = self.conn().select_one_or_none("""
        SELECT r.state
          FROM app.users_tags_override_requests r
         WHERE r.request_id = %s
        """, [request_id])

        return row

    def tag_concept_app(
            self,
            app_id_int: int,
            tagging_context: str
    ):
        if not AppModel.is_concept(
            conn=self.conn(),
            app_id_int=app_id_int,
            check_owner=self.user_id()
        ):
            raise Exception(f"Cannot tag store app")

        if self.session().models().billing().is_module_locked(BillingUtils.BILLING_MODULE_AUDITOR):
            row = self.conn().select_one_or_none("""
            SELECT tagged_by_user
              FROM scraped_data.apps_concepts a,
                   app.users_apps ua
             WHERE a.app_id_int = %s
               AND a.user_id = %s
               AND ua.user_id = a.user_id
               AND ua.app_id_int = a.app_id_int
               AND ua.unlocked_in_demo = 1
            """, [app_id_int, self.user_id()])
            if row is None or row["tagged_by_user"] > 0:
                raise Exception(f"Cannot tag concept more than once in DEMO")

            self.conn().query("""
            UPDATE scraped_data.apps_concepts
               SET tagged_by_user = tagged_by_user + 1
             WHERE app_id_int = %s
            """, [app_id_int])

        return self.tag_app(
            app_id_int=app_id_int,
            tagging_context=tagging_context
        )

    def tag_app(
            self,
            app_id_int: int,
            tagging_context: str
    ):
        self.conn().query("""
        LOCK TABLE tagged_data.platform_tagging_request WRITE,
                   tagged_data.platform_tagging_users WRITE
        """)

        row_request = self.conn().select_one_or_none("""
        SELECT id
          FROM tagged_data.platform_tagging_request
         WHERE app_id_int = %s
        """, [app_id_int])

        if row_request is None:
            request_id = self.conn().insert("""
            INSERT INTO tagged_data.platform_tagging_request (app_id_int, state)
            VALUES (%s, 'queue')
            """, [app_id_int])
        else:
            request_id = row_request["id"]

        self.conn().query("""
        INSERT INTO tagged_data.platform_tagging_users (user_id, app_id_int, context, request_id)
        VALUES (%s, %s, %s, %s)
        """, [self.user_id(), app_id_int, tagging_context, request_id])
        self.conn().unlock_tables()

        ServiceWrapperModel.run(
            d=ServiceWrapperModel.SERVICE_GPT_TAGGER,
            t=True
        )

        return self.get_tagging_state(
            app_id_int=app_id_int
        )

    def get_tagging_state(
            self,
            app_id_int: int
    ):
        res = AppModel.get_tagging_state(
            conn=self.conn(),
            app_id_int=app_id_int,
            user_id=self.user_id()
        )

        if res["state"] in ["queue", "working", "retry"]:
            if not ServiceWrapperModel.is_running(
                conn=self.conn(),
                d=ServiceWrapperModel.SERVICE_GPT_TAGGER
            ):
                ServiceWrapperModel.run(
                    d=ServiceWrapperModel.SERVICE_GPT_TAGGER,
                    t=True
                )

        res[UserObfuscator.APP_ID_INT] = app_id_int

        return res

    def tag_store_app_if_not_tagged(
            self,
            app_id_int: int,
            tagging_context: str,
            admin_force=False
    ):
        if AppModel.is_concept(
            conn=self.conn(),
            app_id_int=app_id_int
        ):
            raise Exception(f"Cannot tag concept app")

        tagging_state = self.get_tagging_state(
            app_id_int=app_id_int
        )

        if admin_force or tagging_state["state"] == "not_tagged":
            tagging_state = self.tag_app(
                app_id_int=app_id_int,
                tagging_context=tagging_context
            )

        return tagging_state

    def tags_def(self) -> TagsDef:
        if self.__tags_def is None:
            self.__tags_def = TagsDef(
                conn=self.conn()
            )
        return self.__tags_def

    def __assert_tags_details(self, tags_details: list[{}]):
        return self.tags_def().check_tags_details(
            tags_details=tags_details
        )

    def get_tags(self, app_id_int: int):
        res = AppModel.get_tags(
            conn=self.conn(),
            user_id=self.user_id(),
            app_id_int=app_id_int
        )

        if self.session().models().apps().is_app_locked(
            app_id_int=app_id_int
        ):
            res["tags"] = []
        else:
            final_tags = []
            unlocked_tags = None
            if self.session().models().billing().is_module_locked(
                    module_id=BillingUtils.BILLING_MODULE_AUDITOR
            ):
                rows_unlocked_tags = self.conn().select_all("""
                SELECT p.tag_id_int
                  FROM app.def_sheet_platform_product p
                 WHERE p.unlocked = 1
                """)
                unlocked_tags = []
                for row in rows_unlocked_tags:
                    unlocked_tags.append(row[UserObfuscator.TAG_ID_INT])
            for tag_detail in res["tags"]:
                if unlocked_tags is None or tag_detail[UserObfuscator.TAG_ID_INT] in unlocked_tags:
                    final_tags.append(tag_detail)
            res["tags"] = final_tags
        return res

    def set_manual_tags(
            self,
            app_id_int: int,
            tags_details: []
    ):
        assert self.__assert_tags_details(tags_details=tags_details)

        AppModel.set_manual_tags(
            conn=self.conn(),
            user_id=self.user_id(),
            app_id_int=app_id_int,
            tags_details=tags_details
        )

        return self.get_tags(app_id_int=app_id_int)

    def confirm_user_tags_override_request(
            self,
            request_id: int,
            tags: []
    ):
        self.conn().query("""
            UPDATE app.users_tags_override_requests 
               SET state = 'accepted' 
             WHERE request_id = %s
            """, [request_id])

        app_id_int = self.conn().select_one("""
            SELECT app_id_int 
              FROM app.users_tags_override_requests 
             WHERE request_id = %s
            """, [request_id])["app_id_int"]

        self.conn().query("""
            DELETE FROM tagged_data.tags_override_from_users_apps 
             WHERE app_id_int = %s
            """, [app_id_int])

        self.conn().query("""
            DELETE FROM tagged_data.tags_override_from_users 
             WHERE app_id_int = %s
            """, [app_id_int])

        self.conn().query("""
            INSERT INTO tagged_data.tags_override_from_users_apps (app_id_int)
            VALUES (%s)
            """, [app_id_int])

        bulk_data = []
        for row in tags:
            bulk_data.append((app_id_int, row["tag_id_int"], row["tag_rank"], request_id))

        self.conn().bulk("""
            INSERT INTO tagged_data.tags_override_from_users (app_id_int, tag_id_int, tag_rank, request_id)
            VALUES (%s, %s, %s, %s)
            """, bulk_data)

        self.conn().query("""
            UPDATE tagged_data.tags 
               SET overriden_by_user = 1 
             WHERE app_id_int = %s
            """, [app_id_int])

        PlatformValuesCache.start_service_for_single_app(app_id_int=app_id_int)
