from src.server.models.apps.app_model import AppModel
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.user.user_obfuscator import UserObfuscator


class ScraperSessionModel(BaseSessionModel):

    def scrap_app_by_app_id(
            self,
            app_id_int: int
    ):
        row = self.conn().select_one_or_none("""
        SELECT app_id_in_store, store
          FROM scraped_data.apps
         WHERE app_id_int = %s
        """, [app_id_int])

        if row is None:
            return None

        return self.scrap_app(
            app_id_in_store=row["app_id_in_store"],
            store=row["store"]
        )

    def scrap_app(
            self,
            app_id_in_store: str,
            store: int,
            force=False
    ):
        app_id_int = AppModel.get_app_id_int(
            conn=self.conn(),
            app_id_in_store=app_id_in_store
        )

        if app_id_int is not None and ScraperModel.is_app_scraped(
            conn=self.conn(),
            app_id_int=app_id_int
        ):
            if not force:
                return self.session().models().apps().get_app_detail(
                    app_id_int=app_id_int
                )

        app_data = ScraperModel.scrap_app(
            conn=self.conn(),
            app_id_in_store=app_id_in_store,
            store=store
        )

        if app_data["state"] != 1:
            raise Exception(f"App {app_id_in_store} scrape error")

        return self.session().models().apps().get_app_detail(
            app_id_int=app_data[UserObfuscator.APP_ID_INT]
        )

    def scrap_dev(
            self,
            dev_id_in_store: str,
            store: int,
            scrap_apps=False
    ):
        dev_id_int = AppModel.get_dev_id_int(
            conn=self.conn(),
            dev_id=ScraperModel.get_dev_id_from_dev_id_in_store(
                dev_id_in_store=dev_id_in_store,
                store=store
            )
        )

        if dev_id_int is not None and ScraperModel.is_dev_scraped(
            conn=self.conn(),
            dev_id_int=dev_id_int
        ):
            if scrap_apps:
                apps_ids_per_dev_id = AppModel.get_devs_apps_ids_int(
                    conn=self.conn(),
                    devs_ids_int=[dev_id_int],
                    user_id=self.user_id()
                )

                app_ids_int = []
                if dev_id_int in apps_ids_per_dev_id:
                    for app_id_int in apps_ids_per_dev_id[dev_id_int]:
                        app_ids_int.append(app_id_int)

                if not self.session().models().apps().scrap_apps_if_not_scraped(
                    app_ids_int=app_ids_int
                ):
                    return {
                        "state": "scraping"
                    }

            return {
                "state": "ok",
                "dev_detail": self.session().models().apps().get_dev_detail(
                    dev_id_int=dev_id_int
                )
            }

        dev_data = ScraperModel.scrap_dev(
            conn=self.conn(),
            dev_id_in_store=dev_id_in_store,
            store=store
        )

        if dev_data["state"] != 1:
            raise Exception(f"Dev {dev_id_in_store} scrape error")

        if scrap_apps:
            if not self.session().models().apps().scrap_apps_if_not_scraped(
                app_ids_int=dev_data["app_ids_int"]
            ):
                return {
                    "state": "scraping"
                }

        return {
            "state": "ok",
            "dev_detail": self.session().models().apps().get_dev_detail(
                dev_id_int=dev_data[UserObfuscator.DEV_ID_INT]
            )
        }
