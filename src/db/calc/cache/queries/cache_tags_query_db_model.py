from typing import Optional

from server_core.db.db_connection import DbConnection
from src.db.calc_db_model import CalcDbModel


class CacheTagsQueryDbModel(CalcDbModel):

    def __init__(self, conn: DbConnection):
        super(CacheTagsQueryDbModel, self).__init__(conn=conn)

    def run_tags_bin(self, app_ids_int: Optional[list[int]] = None, commit=True):
        if app_ids_int is None:
            bin_bytes_cnt = self.conn.select_one("""
            SELECT cnt FROM gb_platform.bin_bytes_cnt
            """)["cnt"]
            self.conn.query("DROP TABLE if exists gb_platform.platform_values_tags_bin")
            self.conn.query(f"""
            CREATE TABLE gb_platform.platform_values_tags_bin (
                app_id_int MEDIUMINT NOT NULL,
                tags_md5 BINARY(32) not null,
                b BINARY({bin_bytes_cnt}) NOT NULL,
                CONSTRAINT platform_values_tags_bin_pk
                    PRIMARY KEY (app_id_int)
            )
            SELECT z1.app_id_int, MD5(z1.b) as tags_md5, z1.b
              FROM (
            SELECT t.app_id_int, 
                   BIT_OR(tb.b) AS b
              FROM gb_platform.def_tags_bin tb,
                   gb_platform.platform_values_tags t
             WHERE tb.tag_id_int = t.tag_id_int
             GROUP BY t.app_id_int
             ) z1
            """)
            if commit:
                self.conn.commit()
        else:
            app_ids_int_db = self.conn.values_arr_to_db_in(app_ids_int, int_values=True)
            self.conn.query(f"""
            DELETE FROM gb_platform.platform_values_tags_bin WHERE app_id_int IN ({app_ids_int_db})
            """)
            self.conn.query(f"""
            INSERT INTO gb_platform.platform_values_tags_bin (
                app_id_int,
                tags_md5,
                b
            )
            SELECT z1.app_id_int, MD5(z1.b) as tags_md5, z1.b
              FROM (
            SELECT t.app_id_int, 
                   BIT_OR(tb.b) AS b
              FROM gb_platform.def_tags_bin tb,
                   gb_platform.platform_values_tags t
             WHERE tb.tag_id_int = t.tag_id_int
               AND t.app_id_int IN ({app_ids_int_db})
             GROUP BY t.app_id_int
             ) z1
            """)
            if commit:
                self.conn.commit()

        if commit:
            self.conn.analyze("platform.platform_values_tags_bin")

    def run_tags(self, app_ids_int: Optional[list[int]] = None, commit=True):

        if app_ids_int is None:
            query = f"""
                    CREATE TABLE gb_platform.platform_values_tags
                    (
                        app_id_int mediumint not null,
                        tag_id_int smallint unsigned not null,
                        tag_rank   tinyint unsigned not null,
                        constraint platform_values_tags_pk
                            primary key (app_id_int, tag_id_int)
                    )
                    SELECT a.app_id_int, 
                           t.tag_id_int, 
                           t.tag_rank
                      FROM gb_platform.platform_values_apps a,
                           gb_tagged_data.platform_tagged pt,
                           gb_tagged_data.tags_v t,
                           gb_platform.def_tags p
                     WHERE a.app_id_int = t.app_id_int
                       AND t.tag_id_int = p.tag_id_int
                       AND pt.app_id_int = a.app_id_int
                    """
            self.conn.query_safe("DROP TABLE IF EXISTS gb_platform.platform_values_tags")
            self.conn.query(query)
        else:
            app_ids_int_db = self.conn.values_arr_to_db_in(app_ids_int, int_values=True)
            self.conn.query(f"""
            DELETE FROM gb_platform.platform_values_tags WHERE app_id_int IN ({app_ids_int_db})
            """)
            query = f"""
            INSERT INTO gb_platform.platform_values_tags(app_id_int, tag_id_int, tag_rank)
            SELECT a.app_id_int, 
                   t.tag_id_int, 
                   t.tag_rank
              FROM gb_platform.platform_values_apps a,
                   gb_tagged_data.platform_tagged pt,
                   gb_tagged_data.tags_v t,
                   gb_platform.def_tags p
             WHERE a.app_id_int = t.app_id_int
               AND t.tag_id_int = p.tag_id_int
               AND pt.app_id_int = a.app_id_int
               AND a.app_id_int IN ({app_ids_int_db})
            """
            self.conn.query(query)
            if commit:
                self.conn.commit()

        if commit:
            self.conn.analyze("platform.platform_values_tags")
