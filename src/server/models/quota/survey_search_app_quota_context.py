from gembase_server_core.db.db_connection import DbConnection
from src.server.models.quota.base_quota_context import BaseQuotaContext


class SurveySearchAppQuotaContext(BaseQuotaContext):

    def __init__(self, survey_guid: str):
        super(SurveySearchAppQuotaContext, self).__init__({
            "source": "survey",
            "context": survey_guid
        })
        self.__survey_guid = survey_guid

    def has(self) -> bool:
        current_quota = 0
        max_quota = 25

        conn = DbConnection()
        row = conn.select_one_or_none("""
        SELECT i.search_store_limit
          FROM survey_v2.survey_instance i
         WHERE i.guid = %s
        """, [self.__survey_guid])
        conn.close()

        if row is not None:
            current_quota = row["search_store_limit"]

        return current_quota < max_quota

    def add(self, count: int, audit_guid: str):
        DbConnection.s_query("""
        UPDATE survey_v2.survey_instance
           SET search_store_limit = search_store_limit + %s
         WHERE guid = %s
        """, [count, self.__survey_guid])