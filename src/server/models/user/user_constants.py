from gembase_server_core.db.db_connection import DbConnection


class UserConstantsClass:

    USER_ROLE_ADMIN = 1
    USER_ROLE_DEMO = 2
    USER_ROLE_SUPER_ADMIN = 3
    USER_ROLE_SYSTEM_BATCH = 5
    USER_ROLE_DEMO_BATCH = 6

    ADMIN_USER_GUID = "00000000-0000-0000-0000-000000000001"
    DEMO_USER_GUID = "00000000-0000-0000-0000-000000000002"
    SUPER_ADMIN_USER_GUID = "00000000-0000-0000-0000-000000000003"
    SYSTEM_CHAT_USER_GUID = "00000000-0000-0000-0000-000000000004"
    SYSTEM_BATCH_USER_GUID = "00000000-0000-0000-0000-000000000005"
    DEMO_BATCH_USER_GUID = "00000000-0000-0000-0000-000000000006"

    __SYSTEM_BATCH_USER_ID = None

    @staticmethod
    def get_user_id_for_guid(conn: DbConnection, guid: str) -> int:
        row = conn.select_one("""
            SELECT u.id
              FROM app.users u
             WHERE u.guid = %s
            """, [guid])
        return row["id"]

    @staticmethod
    def get_user_id_for_role(conn: DbConnection, role: int) -> int:
        row = conn.select_one("""
        SELECT u.id
          FROM app.users u
         WHERE u.role = %s
        """, [role])
        return row["id"]

    @staticmethod
    def get_system_batch_user_id():
        if UserConstantsClass.__SYSTEM_BATCH_USER_ID is None:
            conn = DbConnection()
            UserConstantsClass.__SYSTEM_BATCH_USER_ID = conn.select_one("""
            SELECT u.id
              FROM app.users u
             WHERE u.role = %s
            """, [UserConstantsClass.USER_ROLE_SYSTEM_BATCH])["id"]

        return UserConstantsClass.__SYSTEM_BATCH_USER_ID


uc = UserConstantsClass()
