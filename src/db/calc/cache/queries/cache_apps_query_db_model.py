from typing import Optional

from server_core.db.db_connection import DbConnection
from src.db.calc_db_model import CalcDbModel


class CacheAppsQueryDbModel(CalcDbModel):

    def __init__(self, conn: DbConnection):
        super(CacheAppsQueryDbModel, self).__init__(conn=conn)

    def run(self, app_ids_int: Optional[list[int]] = None, commit=True):

        app_ids_q_a = ""
        app_ids_q_t = ""

        if app_ids_int is not None:
            app_ids_int_db = self.conn.values_arr_to_db_in(app_ids_int, int_values=True)
            self.conn.query(f"""
            DELETE FROM gb_platform.platform_values_apps WHERE app_id_int IN ({app_ids_int_db})
            """)
            app_ids_q_a = f" AND a.app_id_int IN ({app_ids_int_db}) "
            app_ids_q_t = f" AND t.app_id_int IN ({app_ids_int_db}) "
        else:
            self.conn.query("""
            TRUNCATE TABLE gb_platform.platform_values_apps
            """)

        query = f"""
                insert into gb_platform.platform_values_apps (
                    app_id_int,
                    installs,
                    loyalty_installs,
                    growth,
                    rating,
                    store,
                    tier,
                    released_years,
                    d28_ret_gp,
                    downloads_12m_gp_us,
                    downloads_12m_gp_ww,
                    downloads_gp_us,
                    downloads_gp_ww,
                    revenues_12m_gp_us,
                    revenues_12m_gp_ww,
                    revenues_gp_us,
                    revenues_gp_ww
                )
                SELECT a.app_id_int, 
                       a.installs,
                       ROUND(platform.calc_loyalty_installs(a.installs, TIMESTAMPDIFF(MONTH, a.released, gb_app.now()), round(IF(z.app_id_int IS NULL, 0, z.d28_ret_gp) / 100, 4))) as loyalty_installs,
                       -1 as growth,
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
                       IF(z.app_id_int IS NULL, 0, z.revenues_gp_ww) AS revenues_gp_ww
                  FROM gb_scraped_data.apps a
                 INNER JOIN gb_app.def_sheet_platform_values_install_tiers tiers
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
                               FROM gb_platform.platform_values_tags_model tm,
                                    gb_platform.platform_values_tags_model_intercept tmi,
                                    gb_tagged_data.tags_v t,
                                    gb_tagged_data.platform_tagged pt
                              WHERE t.tag_id_int = tm.tag_id_int
                                AND t.app_id_int = pt.app_id_int
                                {app_ids_q_t}
                              GROUP BY t.app_id_int
                            ) z 
                    ON z.app_id_int = a.app_id_int
                 WHERE a.released IS NOT NULL
                   AND a.removed_from_store IS NULL
                   AND a.installs > 0
                   {app_ids_q_a}
                """
        self.conn.query(query)
        if commit:
            self.conn.commit()

        # growth
        all_apps = -1
        app_ids_int_db_2 = -1
        if app_ids_int is not None:
            all_apps = 0
            app_ids_int_db_2 = self.conn.values_arr_to_db_in(app_ids_int, int_values=True)
        self.calc_utils.recreate_table(
            table_name="tmp_apps_growth",
            query=f"""
            CREATE TABLE gb_calc.x__table_name__x (
                app_id_int int not null,
                growth bigint not null,
                CONSTRAINT x__table_name__x_pk 
                    PRIMARY KEY (app_id_int)
            )
            SELECT z1.app_id_int, ROUND(((z1.installs - z1.first_installs) / DATEDIFF(gb_app.now(), z1.first_t)) * 30 * 12) AS growth
              FROM (
            SELECT DISTINCT FIRST_VALUE(h.id) OVER (PARTITION BY h.app_id_int ORDER BY h.t DESC) AS first_id,
                   FIRST_VALUE(h.t) OVER (PARTITION BY h.app_id_int ORDER BY h.t DESC) AS first_t,
                   FIRST_VALUE(h.installs) OVER (PARTITION BY h.app_id_int ORDER BY h.t DESC) AS first_installs,
                   a.installs,
                   a.app_id_int
              FROM gb_platform.platform_values_apps a,
                   gb_scraped_data.apps_hist h
             WHERE a.app_id_int = h.app_id_int
               AND h.t <= DATE_SUB(gb_app.now(), INTERVAL 6 MONTH)
               AND ({all_apps} = -1 OR a.app_id_int IN ({app_ids_int_db_2}))
            ) z1
        """
        )

        query = """
        UPDATE gb_platform.platform_values_apps a
        INNER JOIN gb_calc.tmp_apps_growth g ON g.app_id_int = a.app_id_int
        SET a.growth = g.growth
        """
        self.conn.query(query)
        if commit:
            self.conn.commit()

        query = """
        UPDATE gb_platform.platform_values_apps
           SET growth = IF(released_years = 0, installs, installs / released_years)
         WHERE growth = -1
        """
        self.conn.query(query)
        if commit:
            self.conn.commit()

        if commit:
            self.conn.analyze("platform.platform_values_apps")
