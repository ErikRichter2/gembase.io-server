import json
from typing import Optional

import flask

from server_core.db.db_connection import DbConnection
from src.db.base.base_db_model import BaseDbModel
from src.db.survey.survey_db_model import SurveyDbModel
from src.db.survey.survey_utils import SurveyUtils


class SurveyPublicDbModel(BaseDbModel):

    def __init__(self, conn: DbConnection):
        super(SurveyPublicDbModel, self).__init__(conn=conn)
        
    def return_end_response(self, state: int, config_guid: str):
        if state == 0 or state == 1:
            return None

        row = self.conn.select_one_or_none("""
            SELECT cfg.config
            FROM survey_v2.survey_config cfg
            WHERE cfg.guid = %s
            """, [config_guid])

        if row is None:
            flask.abort(404)

        if state == 2:
            export_data = SurveyUtils.get_export_for_end_page("quota", json.loads(row["config"]))
        elif state == 3:
            export_data = SurveyUtils.get_export_for_end_page("screenout", json.loads(row["config"]))
        elif state == 4:
            export_data = SurveyUtils.get_export_for_end_page("completed", json.loads(row["config"]))
        elif state == 5:
            export_data = SurveyUtils.get_export_for_end_page("trap", json.loads(row["config"]))
        else:
            raise Exception(f"Unknow state: {state}")

        return {
            "state": 1,
            "data": export_data
        }
        
    def check_for_end_type_in_export(self, guid: str, export):
        if "end_type" in export["config_data"]:
            end_type = export["config_data"]["end_type"]
            if end_type == "screenout":
                state = 3
            elif end_type == "completed":
                state = 4
            elif end_type == "trap":
                state = 5
            else:
                raise Exception(f"Unknown end_type: {end_type}")

            self.conn.query("""
            UPDATE survey_v2.survey_whitelist w
            SET w.state = %s,
                w.t = NOW()
            WHERE w.gid = %s
            """, [state, guid])

    def get(self, guid: str, cid: str, gender: Optional[str], age: Optional[str], year_of_birth: Optional[str]):

        row = self.conn.select_one_or_none("""
        SELECT w.state, c.survey_config
        FROM survey_v2.survey_whitelist w,
            survey_v2.survey_control c
        WHERE w.gid = %s
        AND w.survey_control = c.id
        """, [guid])

        if row is not None:
            state = row["state"]
            if state != 0 and state != 1:
                return self.return_end_response(state, row["survey_config"])

        model = SurveyDbModel.load_instance(conn=self.conn, survey_instance_guid=guid)

        if model is None:

            row_w = self.conn.select_one_or_none("""
            SELECT w.id, w.gid as instance_guid, c.pool, w.pool_id, c.survey_config, w.state
            FROM survey_v2.survey_whitelist w,
                survey_v2.survey_control c
            WHERE w.gid = %s
            AND w.survey_control = c.id
            AND c.state != -1
            """, [guid])

            if row_w is None:
                flask.abort(404)

            pool = json.loads(row_w["pool"])
            pool_item = None
            for it in pool:
                if it["id"] == row_w["pool_id"]:
                    pool_item = it
                    break

            if pool_item is None:
                flask.abort(404)

            quota = pool_item["quota"]
            lang = pool_item["lang"]

            row_quota = self.conn.select_one("""
            SELECT count(1) as cnt
            FROM survey_v2.survey_whitelist w
            WHERE w.pool_id = %s
            AND w.state = 4
            """, [row_w["pool_id"]])

            url_params = {
                "a": age,
                "y": year_of_birth,
                "s": gender
            }

            self.conn.query("""
            UPDATE survey_v2.survey_whitelist w
            SET w.state = 1, 
                w.cid = %s,
                w.url_params = %s
            WHERE w.id = %s
            """, [cid, json.dumps(url_params), row_w["id"]])

            if row_quota["cnt"] >= quota:
                self.conn.query("""
                UPDATE survey_v2.survey_whitelist w 
                SET w.state = 2,
                    w.t = NOW()
                WHERE w.id = %s
                """, [row_w["id"]])

                return self.return_end_response(2, row_w["survey_config"])

            model = SurveyDbModel.create_instance(
                conn=self.conn,
                survey_config=row_w["survey_config"],
                guid=row_w["instance_guid"],
                lang=lang
            )

        try:
            assert model is not None
            export = model.export()
            self.check_for_end_type_in_export(model.survey_instance_guid, export)
            return {
                "state": 1,
                "data": export
            }
        except Exception as e:
            conn = DbConnection()
            conn.query("""
            UPDATE survey_v2.survey_instance si
            SET si.error_text = %s
            WHERE si.guid = %s
            """, [str(e), guid])
            conn.commit()
            conn.close()
            return {
                "state": -1
            }        
                        
    def submit(self, guid: str, client_data: dict):

        row = self.conn.select_one_or_none("""
        SELECT w.state
        FROM survey_v2.survey_whitelist w
        WHERE w.gid = %s
        """, [guid])

        if row is None or row["state"] != 1:
            flask.abort(404)

        model = SurveyDbModel.load_instance(conn=self.conn, survey_instance_guid=guid)
        if model is None:
            flask.abort(404)

        try:
            model.submit(client_data)
            export = model.export()
            self.check_for_end_type_in_export(model.survey_instance_guid, export)
            return {
                "state": 1,
                "data": export
            }
        except Exception as e:
            conn = DbConnection()
            conn.query("""
            UPDATE survey_v2.survey_instance si
            SET si.error_text = %s
            WHERE si.guid = %s
            """, [str(e), guid])
            conn.commit()
            conn.close()
            return {
                "state": -1,
            }            