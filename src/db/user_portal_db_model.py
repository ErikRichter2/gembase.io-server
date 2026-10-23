from server_core.utils.utils import Utils
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.games_db_model import GamesDbModel
from src.db.user_billing_db_model import UserBillingDbModel
from src.db.user_db_model import UserDbModel
from src.models.scraper.scraper_model import ScraperModel


class UserPortalDbModel(BaseUserDbModel):
    
    def __init__(self, user_db: UserDbModel):
        super(UserPortalDbModel, self).__init__(user_db=user_db)
        
    def get_my_apps(self, load_app_details=False) -> list[dict]:

        rows = self.conn.select_all("""
        SELECT ua.app_id_int
          FROM gb_app.users_apps ua
         WHERE ua.user_id = %s
        """, [self.user_id])

        my_apps: list[int] = []
        my_apps_details: list[dict] = []

        for row in rows:
            my_apps.append(row["app_id_int"])

        if load_app_details and len(my_apps) > 0:
            my_apps_details = self.get_apps_details(apps_ids_int=my_apps)
            for app_detail in my_apps_details:
                my_apps_details.append(app_detail)

        return my_apps_details
        
    def get_apps_details(
            self,
            apps_ids_int: list[int],
            include_gallery=False,
            include_tags=True
    ) -> list[dict]:
        app_details = GamesDbModel(conn=self.conn).get_app_detail_bulk(
            apps_ids_int=apps_ids_int,
            user_id=self.user_id,
            include_gallery=include_gallery,
            include_tags=include_tags,
        )
        return Utils.dict_to_list(app_details)  

    def add_app_from_store_to_my_apps(
        self,
        app_id_in_store: str,
        store: int
    ) -> dict:

        app_id_int = GamesDbModel.get_app_id_int(
            conn=self.conn,
            app_id_in_store=app_id_in_store
        )

        if app_id_int is None or not ScraperModel.is_app_scraped(
                conn=self.conn,
                app_id_int=app_id_int
        ):
            res = ScraperModel.scrap_app(
                conn=self.conn,
                app_id_in_store=app_id_in_store,
                store=store
            )

            app_id_int = res["app_id_int"]

        return self.add_app_to_my_apps(
            app_id_int=app_id_int,
            unlocked_in_demo=True
        )
        
    def add_app_to_my_apps(
            self,
            app_id_int: int,
            unlocked_in_demo=False
    ) -> dict:
        row = self.conn.select_one_or_none("""
        SELECT ua.user_id
          FROM app.users_apps ua
         WHERE ua.user_id = %s
           AND ua.app_id_int = %s
        """, [self.user_id, app_id_int])

        if row is None:
            self.conn.query("""
            INSERT INTO app.users_apps (user_id, app_id_int, unlocked_in_demo) 
            VALUES (%s, %s, %s)
            """, [self.user_id, app_id_int, unlocked_in_demo])
        else:
            self.conn.query("""
            UPDATE app.users_apps
               SET unlocked_in_demo = %s 
             WHERE user_id = %s
               AND app_id_int = %s
            """, [unlocked_in_demo, self.user_id, app_id_int])

        self.user_db.track_action("add_to_my_apps", str(app_id_int))

        self.conn.query("""
        UPDATE app.users
           SET added_to_my_apps = added_to_my_apps + 1
         WHERE id = %s
        """, [self.user_id])

        app_details = self.get_apps_details(
            apps_ids_int=[app_id_int],
            include_gallery=False
        )

        assert len(app_details) > 0
        return app_details[0]
    
    @staticmethod
    def is_app_detail_changes_input(data: dict):
        return True


    



    def get_app_detail(self, app_id_int: int, icon_bytes: bytes | None):
        row = self.conn.select_one_or_none("""
                             SELECT user_id
                               FROM scraped_data.apps_concepts 
                              WHERE app_id_int = %s
                             """, [app_id_int])
        
        
        
        
    
