from gembase_server_core.db.db_connection import DbConnection
from src.server.models.tags.tags_constants import TagsConstants


class PlatformValuesTags:

    @staticmethod
    def run_tags_bin(conn: DbConnection, app_ids_int: [int] = None):
        app_ids_q_t = ""
        suffix = ""
        prefix = "platform_rebuild_tmp"
        temporary = ""
        single = False

        if app_ids_int is not None:
            app_ids_int_db = conn.values_arr_to_db_in(app_ids_int, int_values=True)
            app_ids_q_t = f" AND t.app_id_int IN ({app_ids_int_db}) "
            suffix = "__single"
            prefix = "platform"
            temporary = "TEMPORARY"
            single = True

        bin_bytes_cnt = conn.select_one(f"""
        SELECT cnt FROM {prefix}.bin_bytes_cnt
        """)["cnt"]

        conn.query(f"DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_tags_bin{suffix}")

        conn.query(f"""
        CREATE {temporary} TABLE platform_rebuild_tmp.platform_values_tags_bin{suffix} (
            app_id_int MEDIUMINT NOT NULL,
            tags_md5 BINARY(32) not null,
            b BINARY({bin_bytes_cnt}) NOT NULL,
            ranked_genres_b BINARY({bin_bytes_cnt}) NOT NULL,
            subgenres_b BINARY({bin_bytes_cnt}) NOT NULL,
            CONSTRAINT platform_values_tags_bin_pk{suffix}
                PRIMARY KEY (app_id_int)
        )
        SELECT z1.app_id_int, 
               MD5(z1.b) as tags_md5, 
               z1.b,
               z1.ranked_genres_b,
               z1.subgenres_b
          FROM (
        SELECT t.app_id_int, 
               BIT_OR(tb.b) AS b,
               BIT_OR(IF(t.tag_rank IN ({TagsConstants.TAG_RANK_PRIMARY}, {TagsConstants.TAG_RANK_SECONDARY}, {TagsConstants.TAG_RANK_TERTIARY}) AND dd.subcategory_int = {TagsConstants.SUBCATEGORY_GENRE_ID}, tb.b, zero.b)) as ranked_genres_b,
               BIT_OR(IF(dd.subgenre = 1, tb.b, zero.b)) as subgenres_b
          FROM {prefix}.def_tags_bin tb,
               app.def_sheet_platform_product dd,
               {prefix}.platform_values_tags t,
               {prefix}.zero_bin_value zero
         WHERE tb.tag_id_int = t.tag_id_int
           AND dd.tag_id_int = tb.tag_id_int
           {app_ids_q_t}
         GROUP BY t.app_id_int
         ) z1
        """)

        conn.query(f"DROP TABLE if exists platform_rebuild_tmp.platform_values_tags_platform_bin{suffix}")

        conn.query(f"""
        CREATE {temporary} TABLE platform_rebuild_tmp.platform_values_tags_platform_bin{suffix} (
            app_id_int MEDIUMINT NOT NULL,
            b BINARY({bin_bytes_cnt}) NOT NULL,
            potential_downloads bigint default 0 not null,
            CONSTRAINT platform_values_tags_bin_pk{suffix}
                PRIMARY KEY (app_id_int)
        )
        SELECT z1.app_id_int, 
               z1.b,
               platform.get_potential_downloads(z1.b, NULL) as potential_downloads
                  FROM (
                        SELECT t.app_id_int, 
                               BIT_OR(tb.b) AS b
                          FROM {prefix}.def_tags_bin tb,
                               {prefix}.def_tags p,
                               platform_rebuild_tmp.platform_values_tags{suffix} t
                         WHERE tb.tag_id_int = t.tag_id_int
                           AND p.tag_id_int = tb.tag_id_int
                           AND p.subcategory_int = {TagsConstants.SUBCATEGORY_PLATFORMS_ID}
                           {app_ids_q_t}
                         GROUP BY t.app_id_int
                 ) z1
        """)

        if not single:
            conn.analyze(f"platform_rebuild_tmp.platform_values_tags_bin{suffix}")
            conn.analyze(f"platform_rebuild_tmp.platform_values_tags_platform_bin{suffix}")

    @staticmethod
    def run_tags(conn: DbConnection, app_ids_int: [int] = None):

        app_ids_q_a = ""
        suffix = ""
        prefix = "platform_rebuild_tmp"
        temporary = ""
        single = False

        if app_ids_int is not None:
            app_ids_int_db = conn.values_arr_to_db_in(app_ids_int, int_values=True)
            app_ids_q_a = f" AND t.app_id_int IN ({app_ids_int_db}) "
            suffix = "__single"
            prefix = "platform"
            temporary = "TEMPORARY"
            single = True

        conn.query(f"""
        DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_tags{suffix}
        """)

        query = f"""
        CREATE {temporary} TABLE platform_rebuild_tmp.platform_values_tags{suffix}
        (
            app_id_int mediumint not null,
            tag_id_int smallint unsigned not null,
            tag_rank   tinyint unsigned not null,
            constraint platform_values_tags_pk{suffix}
                primary key (app_id_int, tag_id_int)
        )
        SELECT a.app_id_int, 
               t.tag_id_int, 
               t.tag_rank
          FROM platform_rebuild_tmp.platform_values_apps{suffix} a,
               tagged_data.platform_tagged pt,
               tagged_data.tags_v t,
               {prefix}.def_tags p
         WHERE a.app_id_int = t.app_id_int
           AND t.tag_id_int = p.tag_id_int
           AND pt.app_id_int = a.app_id_int
           {app_ids_q_a}
        """
        conn.query(query)

        conn.query(f"""
        DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_tags_platform{suffix}
        """)

        query = f"""
        CREATE {temporary} TABLE platform_rebuild_tmp.platform_values_tags_platform{suffix}
        (
            app_id_int mediumint not null,
            tag_id_int smallint unsigned not null,
            constraint platform_values_tags_pk{suffix}
                primary key (app_id_int, tag_id_int)
        )
        SELECT a.app_id_int, 
               t.tag_id_int
          FROM platform_rebuild_tmp.platform_values_apps{suffix} a,
               tagged_data.platform_tagged pt,
               tagged_data.tags_v t,
               {prefix}.def_tags p
         WHERE a.app_id_int = t.app_id_int
           AND t.tag_id_int = p.tag_id_int
           AND pt.app_id_int = a.app_id_int
           AND p.subcategory_int = {TagsConstants.SUBCATEGORY_PLATFORMS_ID}
           {app_ids_q_a}
        """

        conn.query(query)

        if not single:
            conn.analyze(f"platform_rebuild_tmp.platform_values_tags{suffix}")
            conn.analyze(f"platform_rebuild_tmp.platform_values_tags_platform{suffix}")
