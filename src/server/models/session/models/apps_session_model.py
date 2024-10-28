from src.server.models.apps.app_model import AppModel
from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.dms.dms_model import DmsModel
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.user.user_helper import UserHelper
from src.server.models.user.user_obfuscator import UserObfuscator


class AppsSessionModel(BaseSessionModel):

    def create_concept_as_copy(self, app_id_int: int):

        if not self.session().user().can_create_concept():
            raise Exception(f"Cannot create more than {self.session().user().get_concepts_counter()} concepts in DEMO")

        concept_counter = self.session().user().get_concepts_counter() + 1

        copy_app_id_int = AppModel.create_concept_as_copy(
            conn=self.conn(),
            user_id=self.user_id(),
            from_app_id_int=app_id_int,
            concept_counter=concept_counter
        )

        self.session().user().set_concepts_counter(
            value=concept_counter
        )

        self.__add_app_to_my_apps(
            app_id_int=copy_app_id_int
        )

        self.session().user().track_action("create_concept_from_app", str(app_id_int))

        return self.get_app_detail(
            app_id_int=copy_app_id_int,
            include_gallery=True
        )

    def save_concept_app_icon(self, app_id_int: int, icon_bytes: bytes | None):

        self.conn().query("""
        DELETE FROM scraped_data.apps_icons
         WHERE app_id_int = %s
        """, [app_id_int])

        if icon_bytes is not None:
            self.conn().query("""
            INSERT INTO scraped_data.apps_icons (app_id_int, icon) 
            VALUES (%s, %s)
            """, [app_id_int, icon_bytes])

    def is_app_locked(self, app_id_int: int) -> bool:
        return UserHelper.is_app_locked(
            conn=self.conn(),
            user_id=self.user_id(),
            app_id_int=app_id_int
        )

    def save_concept_app(
            self,
            app_id_int: int,
            app_detail_changes: {},
            files: []
    ):
        if self.session().models().billing().is_module_locked(BillingUtils.BILLING_MODULE_AUDITOR):
            raise Exception(f"Cannot save concept in demo")

        row = self.conn().select_one_or_none("""
        SELECT 1
          FROM scraped_data.apps_concepts a
         WHERE a.user_id = %s
           AND a.app_id_int = %s
        """, [self.user_id(), app_id_int])

        if row is None:
            raise Exception(f"App {app_id_int} does not exist")

        if app_detail_changes is not None:
            AppModel.update_concept_app(
                conn=self.conn(),
                app_id_int=app_id_int,
                app_detail_changes=app_detail_changes,
                user_id=self.user_id()
            )

            if "removed_images" in app_detail_changes:
                bulk_data = []
                for image_id in app_detail_changes["removed_images"]:
                    bulk_data.append((app_id_int, image_id))
                self.conn().bulk("""
                DELETE FROM scraped_data.apps_gallery ag
                WHERE ag.app_id_int = %s
                  AND ag.id = %s
                """, bulk_data)

        if len(files) > 0:
            max_order = 1
            row_max_order = self.conn().select_one_or_none("""
            SELECT MAX(img_order) as max_order
              FROM scraped_data.apps_gallery
             WHERE app_id_int = %s
            """, [app_id_int])

            if row_max_order["max_order"] is not None:
                max_order = row_max_order["max_order"] + 1

            for file in files:
                mime = file.content_type
                dms_row = DmsModel.save_file_to_dms(
                    conn=self.conn(),
                    mime=mime,
                    file_type="image",
                    is_binary=True,
                    file=file,
                    user_id=self.user_id()
                )

                self.conn().query("""
                INSERT INTO scraped_data.apps_gallery
                (app_id_int, img_order, dms_id) 
                VALUES (%s, %s, %s) 
                """, [app_id_int, max_order, dms_row["id"]])
                max_order += 1

        return self.get_app_detail(
            app_id_int=app_id_int,
            include_gallery=True
        )

    def create_concept_app_from_temp(
            self,
            app_detail_changes: {}
    ):
        if not self.session().user().can_create_concept():
            raise Exception(f"Cannot create more than {self.session().user().get_concepts_counter()} concepts in DEMO")

        concepts_counter = self.session().user().get_concepts_counter() + 1

        app_id_int = AppModel.create_next_id_atomic()

        AppModel.create_concept_app(
            conn=self.conn(),
            user_id=self.user_id(),
            app_id_int=app_id_int,
            app_detail_changes=app_detail_changes,
            concept_counter=concepts_counter
        )

        self.session().user().set_concepts_counter(
            value=concepts_counter
        )

        self.__add_app_to_my_apps(
            app_id_int=app_id_int,
            unlocked_in_demo=True
        )

        app_detail = self.get_app_detail(
            app_id_int=app_id_int
        )

        return app_detail

    def get_my_apps(self, load_app_details=False) -> []:

        my_apps, is_demo_app = self.session().user().get_my_apps()

        if load_app_details and len(my_apps) > 0:
            unlocked_apps_details = []
            locked_apps_detail = []
            my_apps_details = self.get_apps_details(app_ids_int=my_apps)
            for app_detail in my_apps_details:
                if is_demo_app is not None and app_detail[UserObfuscator.APP_ID_INT] == is_demo_app:
                    app_detail["is_demo_app"] = True
                if "locked" in app_detail and app_detail["locked"]:
                    locked_apps_detail.append(app_detail)
                else:
                    unlocked_apps_details.append(app_detail)
            return unlocked_apps_details + locked_apps_detail
        else:
            return my_apps

    def get_dev_detail(self, dev_id_int: int) -> dict:
        return self.get_devs_details(
            dev_ids_int=[dev_id_int]
        )[0]

    def get_devs_details(self, dev_ids_int: list[int]) -> list[dict]:
        devs_details = AppModel.get_devs_details(
            conn=self.conn(),
            devs_ids_int=dev_ids_int
        )
        return [devs_details[k] for k in devs_details]

    def remove_app_from_my_apps(self, app_id_int: int):

        self.conn().query("""
        DELETE FROM app.users_apps ua
         WHERE ua.user_id = %s
           AND ua.app_id_int = %s
        """, [self.user_id(), app_id_int])

        if app_id_int == BillingUtils.UNLOCKED_DEFAULT_APP_ID_INT:
            self.session().user().set_removed_initial_app()

        if AppModel.is_concept(
                conn=self.conn(),
                app_id_int=app_id_int,
                check_owner=self.user_id()
        ):
            AppModel.delete_concept_app(
                conn=self.conn(),
                app_id_int=app_id_int
            )

    def add_app_from_store_to_my_apps(
        self,
        app_id_in_store: str,
        store: int
    ) -> dict:
        # if self.session().models().billing().is_module_locked(BillingUtils.BILLING_MODULE_AUDITOR):
        #     added_to_my_apps = self.conn().select_one("""
        #     SELECT added_to_my_apps
        #       FROM app.users
        #      WHERE id = %s
        #     """, [self.user_id()])["added_to_my_apps"]
        #     if added_to_my_apps >= 3:
        #         raise Exception(f"Cannot add app, limit exceeded")

        app_id_int = AppModel.get_app_id_int(
            conn=self.conn(),
            app_id_in_store=app_id_in_store
        )

        if app_id_int is None or not ScraperModel.is_app_scraped(
                conn=self.conn(),
                app_id_int=app_id_int
        ):
            res = ScraperModel.scrap_app(
                conn=self.conn(),
                app_id_in_store=app_id_in_store,
                store=store
            )

            app_id_int = res["app_id_int"]

        return self.__add_app_to_my_apps(
            app_id_int=app_id_int,
            unlocked_in_demo=True
        )

    def get_unlocked_apps_ids_from_list(self, app_ids_int: list[int]) -> list[int]:
        unlocked = UserHelper.get_unlocked_apps(
            conn=self.conn(),
            user_id=self.session().user().get_id()
        )
        if unlocked is None:
            return app_ids_int
        res = []
        for app_id_int in app_ids_int:
            if unlocked is None or app_id_int in unlocked:
                res.append(app_id_int)

        return res

    def get_app_detail(
            self,
            app_id_int: int,
            include_gallery=False,
            include_tags=True
    ) -> dict:
        return self.get_apps_details(
            app_ids_int=[app_id_int],
            include_gallery=include_gallery,
            include_tags=include_tags
        )[0]

    """
    Returns True if all apps from app_ids_int list are scraped.
    
    If at least one app is not scraped - will scrap first un-scraped app and return False.
    """
    def scrap_apps_if_not_scraped(self, app_ids_int: list[int]):
        if len(app_ids_int) == 0:
            return True

        app_rows = self.conn().select_all(f"""
        SELECT app_id_int 
          FROM scraped_data.apps 
         WHERE app_id_int IN ({self.conn().values_arr_to_db_in(app_ids_int, int_values=True)})
        """)

        for row in app_rows:
            app_id_int = row["app_id_int"]
            if not ScraperModel.is_app_scraped(
                    conn=self.conn(),
                    app_id_int=app_id_int
            ):
                self.session().models().scraper().scrap_app_by_app_id(
                    app_id_int=app_id_int
                )

                ix = app_ids_int.index(app_id_int)
                if ix < len(app_ids_int) - 1:
                    return False

        return True

    def get_apps_details(
            self,
            app_ids_int: list[int],
            include_gallery=False,
            include_tags=True
    ) -> list[dict]:
        unlocked_apps = self.get_unlocked_apps_ids_from_list(app_ids_int=app_ids_int)

        app_details = AppModel.get_app_detail_bulk(
            conn=self.conn(),
            app_ids_int=app_ids_int,
            user_id=self.session().user().get_id(),
            include_gallery=include_gallery,
            return_array=True,
            include_tags=include_tags
        )

        for app_detail in app_details:
            if app_detail["app_id_int"] not in unlocked_apps:
                AppModel.obfuscate_app_detail(app_detail, self.session().user().obfuscator())

        return app_details

    def __add_app_to_my_apps(
            self,
            app_id_int: int,
            unlocked_in_demo=False
    ) -> dict:
        row = self.conn().select_one_or_none("""
        SELECT ua.user_id
          FROM app.users_apps ua
         WHERE ua.user_id = %s
           AND ua.app_id_int = %s
        """, [self.user_id(), app_id_int])

        if row is None:
            self.conn().query("""
            INSERT INTO app.users_apps (user_id, app_id_int, unlocked_in_demo) 
            VALUES (%s, %s, %s)
            """, [self.user_id(), app_id_int, unlocked_in_demo])
        else:
            self.conn().query("""
            UPDATE app.users_apps
               SET unlocked_in_demo = %s 
             WHERE user_id = %s
               AND app_id_int = %s
            """, [unlocked_in_demo, self.user_id(), app_id_int])

        self.session().user().track_action("add_to_my_apps", str(app_id_int))

        self.conn().query("""
        UPDATE app.users
           SET added_to_my_apps = added_to_my_apps + 1
         WHERE id = %s
        """, [self.user_id()])

        app_details = self.get_apps_details(
            app_ids_int=[app_id_int],
            include_gallery=False
        )

        assert len(app_details) > 0
        return app_details[0]

    def get_apps_history_kpis(
            self,
            app_ids_int: list[int],
            kpi: str,
            interval: str
    ) -> dict:

        if len(app_ids_int) == 0:
            return {}

        rows = self.conn().select_all(f"""
        SELECT UNIX_TIMESTAMP(h.t) as t, 
               h.installs,
               h.score,
               h.app_id_int
          FROM scraped_data.apps_hist h
         WHERE h.app_id_int IN ({self.conn().values_arr_to_db_in(app_ids_int, int_values=True)})
           AND (
                ('{interval}' = '6m' AND DATE_ADD(h.t, INTERVAL 6 MONTH ) > NOW()) OR
                ('{interval}' = '12m' AND DATE_ADD(h.t, INTERVAL 12 MONTH ) > NOW()) OR
                ('{interval}' = 'all')
               )
         ORDER BY h.app_id_int, h.t
        """)

        rows_per_app_id_int = {}
        for row in rows:
            app_id_int = row["app_id_int"]
            if app_id_int not in rows_per_app_id_int:
                rows_per_app_id_int[app_id_int] = []
            rows_per_app_id_int[app_id_int].append(row)

        # fix installs
        for app_id_int in rows_per_app_id_int:
            cnt = len(rows_per_app_id_int[app_id_int])
            for i in range(0, cnt - 1):
                v1 = rows_per_app_id_int[app_id_int][cnt - 1 - i]["installs"]
                v2 = rows_per_app_id_int[app_id_int][cnt - 1 - i - 1]["installs"]
                if v2 > v1:
                    v2 = v1
                rows_per_app_id_int[app_id_int][cnt - 1 - i - 1]["installs"] = v2

        data_per_app_id_int = {}
        for app_id_int in rows_per_app_id_int:
            if kpi == "quality":
                data_per_app_id_int[app_id_int] = [{
                    "app_id_int": app_id_int,
                    "t": row["t"],
                    "v": round(row["score"] / 20, 1)
                } for row in rows_per_app_id_int[app_id_int]]
            elif kpi == "growth":
                res = []
                prev_t = None
                prev_i = None
                for i in range(0, len(rows_per_app_id_int[app_id_int])):
                    if i == 0:
                        prev_t = rows_per_app_id_int[app_id_int][i]["t"]
                        prev_i = rows_per_app_id_int[app_id_int][i]["installs"]
                        continue
                    t = rows_per_app_id_int[app_id_int][i]["t"]
                    if t - prev_t >= 7 * 24 * 60 * 60:
                        v = rows_per_app_id_int[app_id_int][i]["installs"] - prev_i
                        res.append({
                            "app_id_int": app_id_int,
                            "t": t,
                            "v": v
                        })
                        prev_t = t
                        prev_i = rows_per_app_id_int[app_id_int][i]["installs"]
                data_per_app_id_int[app_id_int] = res
            else:
                data_per_app_id_int[app_id_int] = [{
                    "app_id_int": app_id_int,
                    "t": row["t"],
                    "v": row["installs"]
                } for row in rows_per_app_id_int[app_id_int]]

        return data_per_app_id_int

    def get_app_history_kpis(
            self,
            app_id_int: int,
            kpi: str,
            interval: str
    ) -> list:
        apps = self.get_apps_history_kpis(
            app_ids_int=[app_id_int],
            kpi=kpi,
            interval=interval
        )
        if app_id_int not in apps:
            return []
        return apps[app_id_int]
