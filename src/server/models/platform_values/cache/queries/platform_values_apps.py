from gembase_server_core.db.db_connection import DbConnection
from src.server.models.platform_values.platform_values_helper import PlatformValuesHelper


class PlatformValuesApps:

    @staticmethod
    def run(conn: DbConnection, app_ids_int: [int] = None):

        app_ids_q_a = ""
        app_ids_q_t = ""
        suffix = ""
        temporary = ""
        single = False

        if app_ids_int is not None:
            app_ids_int_db = conn.values_arr_to_db_in(app_ids_int, int_values=True)
            app_ids_q_a = f" AND a.app_id_int IN ({app_ids_int_db}) "
            app_ids_q_t = f" AND t.app_id_int IN ({app_ids_int_db}) "
            suffix = "__single"
            temporary = "TEMPORARY"
            single = True

        conn.query(f"""
        DROP TABLE IF EXISTS platform_rebuild_tmp.platform_values_apps{suffix}
        """)

        query = f"""
                CREATE {temporary} TABLE platform_rebuild_tmp.platform_values_apps{suffix} (
                    app_id_int int not null,
                    installs bigint not null,
                    loyalty_installs bigint not null,
                    rating tinyint not null,
                    store tinyint not null,
                    tier tinyint not null,
                    released_years tinyint not null,
                    d28_ret_gp decimal(8,4) not null,
                    downloads_12m_gp_us bigint not null,
                    downloads_12m_gp_ww bigint not null,
                    downloads_gp_us bigint not null,
                    downloads_gp_ww bigint not null,
                    revenues_12m_gp_us bigint not null,
                    revenues_12m_gp_ww bigint not null,
                    revenues_gp_us bigint not null,
                    revenues_gp_ww bigint not null,
                    growth bigint not null,
                    constraint platform_values_apps_pk{suffix}
                            primary key (app_id_int)
                )
                SELECT a.app_id_int, 
                       a.installs,
                       ROUND(platform.calc_loyalty_installs(a.installs, TIMESTAMPDIFF(MONTH, a.released, CURDATE()), round(IF(z.app_id_int IS NULL, 0, z.d28_ret_gp) / 100, 4))) as loyalty_installs,
                       a.rating, 
                       a.store, 
                       tiers.tier,
                       TIMESTAMPDIFF(YEAR, a.released, CURDATE()) as released_years,
                       IF(z.app_id_int IS NULL, 0, z.d28_ret_gp) as d28_ret_gp,
                       IF(z.app_id_int IS NULL, 0, downloads_12m_gp_us) as downloads_12m_gp_us,
                       IF(z.app_id_int IS NULL, 0, z.downloads_12m_gp_ww) as downloads_12m_gp_ww,
                       IF(z.app_id_int IS NULL, 0, z.downloads_gp_us) as downloads_gp_us,
                       IF(z.app_id_int IS NULL, 0, z.downloads_gp_ww) as downloads_gp_ww,
                       IF(z.app_id_int IS NULL, 0, z.revenues_12m_gp_us) AS revenues_12m_gp_us,
                       IF(z.app_id_int IS NULL, 0, z.revenues_12m_gp_ww) AS revenues_12m_gp_ww,
                       IF(z.app_id_int IS NULL, 0, z.revenues_gp_us) AS revenues_gp_us,
                       IF(z.app_id_int IS NULL, 0, z.revenues_gp_ww) AS revenues_gp_ww,
                       -1 as growth
                  FROM scraped_data.apps a
                 INNER join app.def_sheet_platform_values_install_tiers tiers
                    ON tiers.store_id = a.store
                   AND a.installs >= tiers.value_from
                   AND a.installs < tiers.value_to
                  LEFT JOIN (
                             SELECT t.app_id_int, 
                                    ROUND(SUM(tm.d28_ret_gp) + MAX(tmi.d28_ret_gp), 4) as d28_ret_gp,
                                    ROUND(EXP(SUM(tm.downloads_12m_gp_us) + MAX(tmi.downloads_12m_gp_us))) as downloads_12m_gp_us,
                                    ROUND(EXP(SUM(tm.downloads_12m_gp_ww) + MAX(tmi.downloads_12m_gp_ww))) as downloads_12m_gp_ww,
                                    ROUND(EXP(SUM(tm.downloads_gp_us) + MAX(tmi.downloads_gp_us))) as downloads_gp_us,
                                    ROUND(EXP(SUM(tm.downloads_gp_ww) + MAX(tmi.downloads_gp_ww))) as downloads_gp_ww,
                                    ROUND(EXP(SUM(tm.revenues_12m_gp_us) + MAX(tmi.revenues_12m_gp_us))) as revenues_12m_gp_us,
                                    ROUND(EXP(SUM(tm.revenues_12m_gp_ww) + MAX(tmi.revenues_12m_gp_ww))) as revenues_12m_gp_ww,
                                    ROUND(EXP(SUM(tm.revenues_gp_us) + MAX(tmi.revenues_gp_us))) as revenues_gp_us,
                                    ROUND(EXP(SUM(tm.revenues_gp_ww) + MAX(tmi.revenues_gp_ww))) as revenues_gp_ww
                               FROM platform.platform_values_tags_model tm,
                                    platform.platform_values_tags_model_intercept tmi,
                                    tagged_data.tags_v t,
                                    tagged_data.platform_tagged pt
                              WHERE t.tag_id_int = tm.tag_id_int
                                AND t.app_id_int = pt.app_id_int
                                {app_ids_q_t}
                              GROUP BY t.app_id_int
                            ) z 
                    ON z.app_id_int = a.app_id_int
                 WHERE a.released IS NOT NULL
                   AND a.installs > 0
                   {app_ids_q_a}
                """
        conn.query(query)

        if not single:
            conn.analyze(f"platform_rebuild_tmp.platform_values_apps{suffix}")

        # growth
        PlatformValuesHelper.recreate_table(
            conn=conn,
            table_name="tmp_apps_growth",
            schema="platform_calc_tmp",
            query=f"""
            CREATE TABLE x__table_name__x (
                app_id_int int not null,
                growth bigint not null,
                CONSTRAINT x__table_name__x_pk 
                    PRIMARY KEY (app_id_int)
            )
            SELECT z1.app_id_int, ROUND(((z1.installs - z1.first_installs) / DATEDIFF(NOW(), z1.first_t)) * 30 * 12) AS growth
              FROM (
            SELECT DISTINCT FIRST_VALUE(h.id) OVER (PARTITION BY h.app_id_int ORDER BY h.t DESC) AS first_id,
                   FIRST_VALUE(h.t) OVER (PARTITION BY h.app_id_int ORDER BY h.t DESC) AS first_t,
                   FIRST_VALUE(h.installs) OVER (PARTITION BY h.app_id_int ORDER BY h.t DESC) AS first_installs,
                   a.installs,
                   a.app_id_int
              FROM platform_rebuild_tmp.platform_values_apps{suffix} a,
                   scraped_data.apps_hist h
             WHERE a.app_id_int = h.app_id_int
               AND h.t <= DATE_SUB(NOW(), INTERVAL 6 MONTH)
               {app_ids_q_a}
            ) z1
        """
        )

        query = f"""
        UPDATE platform_rebuild_tmp.platform_values_apps{suffix} a
        INNER JOIN platform_calc_tmp.tmp_apps_growth g ON g.app_id_int = a.app_id_int
        SET a.growth = g.growth
        """
        conn.query(query)
        conn.commit()

        query = f"""
        UPDATE platform_rebuild_tmp.platform_values_apps{suffix}
           SET growth = IF(released_years = 0, installs, installs / released_years)
         WHERE growth = -1
        """
        conn.query(query)
        conn.commit()

        if not single:
            conn.analyze(f"platform_rebuild_tmp.platform_values_apps{suffix}")
