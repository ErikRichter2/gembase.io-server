from server_core.utils.utils import Utils
from src.db.base.base_user_db_model import BaseUserDbModel
from src.db.games_db_model import GamesDbModel
from src.db.portal_db_model import PortalDbModel
from src.db.user_db_model import UserDbModel


class UserPortalDbModel(BaseUserDbModel):
    
    def __init__(self, user_db: UserDbModel):
        super(UserPortalDbModel, self).__init__(user_db=user_db)
        
    def get_my_apps(self, load_app_details=False) -> list[dict]:

        rows = self.conn().select_all("""
        SELECT ua.app_id_int
          FROM gb_app.users_apps ua
         WHERE ua.user_id = %s
        """, [self.user_id()])

        my_apps: list[int] = []
        my_apps_details: list[dict] = []

        for row in rows:
            my_apps.append(row["app_id_int"])

        if load_app_details and len(my_apps) > 0:
            my_apps_details = self.get_apps_details(app_ids_int=my_apps)
            for app_detail in my_apps_details:
                my_apps_details.append(app_detail)

        return my_apps_details
        
    def get_apps_details(
            self,
            apps_ids_int: list[int],
            include_gallery=False,
            include_tags=True
    ) -> list[dict]:
        app_details = GamesDbModel(conn=self.conn()).get_app_detail_bulk(
            apps_ids_int=apps_ids_int,
            user_id=self.user_id(),
            include_gallery=include_gallery,
            include_tags=include_tags,
        )
        return Utils.dict_to_list(app_details)  
