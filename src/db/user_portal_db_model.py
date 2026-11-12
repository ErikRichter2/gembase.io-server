from server_core.db.db_connection import DbConnection
from server_core.utils.utils import Utils
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.calc.factory.calc_interface_gaps_opportunities import CalcInterfaceGapsOpportunities
from src.db.games_db_model import GamesDbModel
from src.db.surveys_db_model import SurveysDbModel


class UserPortalDbModel(BaseUserDbModel):
    
    def __init__(self, conn: DbConnection, user_id: int):
        super(UserPortalDbModel, self).__init__(conn=conn, user_id=user_id)
        
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
        
        games_db = GamesDbModel(conn=self.conn)

        app_id_int = games_db.get_app_id_int(app_id_in_store=app_id_in_store)

        if app_id_int is None or not games_db.is_app_scraped(app_id_int=app_id_int):
            res = games_db.scrap_app(
                app_id_in_store=app_id_in_store,
                store=store
            )
            
            if res is None:
                raise Exception(f'add_app_from_store_to_my_apps: scrap_app returns none, app id: {app_id_in_store}')
            
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
          FROM gb_app.users_apps ua
         WHERE ua.user_id = %s
           AND ua.app_id_int = %s
        """, [self.user_id, app_id_int])

        if row is None:
            self.conn.query("""
            INSERT INTO gb_app.users_apps (user_id, app_id_int, unlocked_in_demo) 
            VALUES (%s, %s, %s)
            """, [self.user_id, app_id_int, unlocked_in_demo])
        else:
            self.conn.query("""
            UPDATE gb_app.users_apps
               SET unlocked_in_demo = %s 
             WHERE user_id = %s
               AND app_id_int = %s
            """, [unlocked_in_demo, self.user_id, app_id_int])

        self.user_db.track_action("add_to_my_apps", str(app_id_int))

        self.conn.query("""
        UPDATE gb_app.users
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
                               FROM gb_scraped_data.apps_concepts 
                              WHERE app_id_int = %s
                             """, [app_id_int])
        
        
    def remove_app_from_my_apps(self, app_id_int: int):

        self.conn.query("""
        DELETE FROM gb_app.users_apps ua
         WHERE ua.user_id = %s
           AND ua.app_id_int = %s
        """, [self.user_id, app_id_int])

        if self.user_db.games_db.is_concept(app_id_int=app_id_int):
            self.user_db.games_db.delete_concept_app(
                app_id_int=app_id_int
            )        

    def create_concept_as_copy(self, app_id_int: int):
        
        concept_counter = self.user_db.games_db.get_concepts_counter()

        if not self.user_db.games_db.can_create_concept():
            raise Exception(f"Cannot create more than {concept_counter} concepts in DEMO")

        concept_counter += 1

        app_detail = self.user_db.games_db.get_app_detail(
            app_id_int=app_id_int,
            include_tags=True
        )

        copy_app_id_int = GamesDbModel.create_next_id_atomic()

        self.user_db.games_db.create_concept_app(
            app_id_int=copy_app_id_int,
            app_detail_changes=app_detail,
            concept_counter=concept_counter
        )

        self.conn.query("""
        UPDATE gb_scraped_data.apps_concepts
           SET copied_from_app_id_int = %s
         WHERE app_id_int = %s
        """, [app_id_int, copy_app_id_int])

        self.conn.query("""
        INSERT INTO gb_scraped_data.apps_gallery
        (app_id_int, img_order, dms_id, store_url) 
        SELECT %s as app_id_int, ag.img_order, ag.dms_id, ag.store_url
          FROM gb_scraped_data.apps_gallery ag
         WHERE ag.app_id_int = %s
        """, [copy_app_id_int, app_id_int])
        
        self.user_db.games_db.set_concepts_counter(
            value=concept_counter
        )

        self.add_app_to_my_apps(
            app_id_int=copy_app_id_int
        )

        self.user_db.track_action("create_concept_from_app", str(app_id_int))

        return self.user_db.games_db.get_app_detail(
            app_id_int=copy_app_id_int,
            include_gallery=True
        )

    def get_opportunity_detail(
            self,
            opportunity_guid: str
    ):
        survey_id = SurveysDbModel(conn=self.conn).get_survey_meta_id(SurveysDbModel.SURVEY_CONTROL_GUID)
        if survey_id is None:
            raise Exception('Unknown survey_id')
        return CalcInterfaceGapsOpportunities(
            conn=self.conn,
            user_id=self.user_id,
            survey_id=survey_id
        ).generate_client_data(
            obfuscator=self.user_db.obfuscator,
            opportunity_guid=opportunity_guid
        )