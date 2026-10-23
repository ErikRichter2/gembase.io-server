from server_core.db.db_connection import DbConnection
from src.db.base.base_db_model import BaseDbModel
from src.db.services_db_model import ServicesDbModel


class PlatformValuesDbModel(BaseDbModel):
    def __init__(self, conn: DbConnection = DbConnection()):
        super(PlatformValuesDbModel, self).__init__(conn=conn)

    def recreate_single_app_tags(
            self,
            user_id: int,
            app_id_int: int
    ):
        self.run(apps_ids_int=[app_id_int], commit=False)
        self.run_tags(apps_ids_int=[app_id_int], commit=False)
        self.run_tags_bin(apps_ids_int=[app_id_int], commit=False)

        service_conn = DbConnection()
        service_conn.query("""
            DELETE FROM platform_values.requests
            WHERE user_id = %s
            AND calc IN ('audiences_angles', 'competitors_for_audience_angle')
            """, [user_id])
        service_conn.query("""
                    DELETE FROM platform_values.calc_tam_per_app_on_demand WHERE app_id_int = %s
                    """, [app_id_int])
        service_conn.query("""
                    INSERT INTO platform_values.calc_tam_per_app_on_demand (app_id_int) VALUES (%s)
                    """, [app_id_int])
        service_conn.commit()
        service_conn.close()

        ServicesDbModel().run(s="src/services/platform_values_on_demand_calc.py")
        
    def run(self, apps_ids_int: list[int] = [], commit=True):

        app_ids_q_a = ""
        app_ids_q_t = ""

        if len(apps_ids_int) > 0 is not None:
            app_ids_int_db = self.conn.values_arr_to_db_in(apps_ids_int, int_values=True)
            self.conn.query(f"""
            DELETE FROM platform.platform_values_apps WHERE app_id_int IN ({app_ids_int_db})
            """)
            app_ids_q_a = f" AND a.app_id_int IN ({app_ids_int_db}) "
            app_ids_q_t = f" AND t.app_id_int IN ({app_ids_int_db}) "
        else:
            self.conn.query("""
            TRUNCATE TABLE platform.platform_values_apps
            """)

        query = f"""
                insert into platform.platform_values_apps (
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
                       ROUND(platform.calc_loyalty_installs(a.installs, TIMESTAMPDIFF(MONTH, a.released, CURDATE()), round(IF(z.app_id_int IS NULL, 0, z.d28_ret_gp) / 100, 4))) as loyalty_installs,
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
        if apps_ids_int is not None:
            all_apps = 0
            app_ids_int_db_2 = self.conn.values_arr_to_db_in(apps_ids_int, int_values=True)
        self.recreate_table(
            table_name="tmp_apps_growth",
            query=f"""
            CREATE TABLE platform_values.x__table_name__x (
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
              FROM platform.platform_values_apps a,
                   scraped_data.apps_hist h
             WHERE a.app_id_int = h.app_id_int
               AND h.t <= DATE_SUB(NOW(), INTERVAL 6 MONTH)
               AND ({all_apps} = -1 OR a.app_id_int IN ({app_ids_int_db_2}))
            ) z1
        """
        )

        query = """
        UPDATE platform.platform_values_apps a
        INNER JOIN platform_values.tmp_apps_growth g ON g.app_id_int = a.app_id_int
        SET a.growth = g.growth
        """
        self.conn.query(query)
        if commit:
            self.conn.commit()

        query = """
        UPDATE platform.platform_values_apps
           SET growth = IF(released_years = 0, installs, installs / released_years)
         WHERE growth = -1
        """
        self.conn.query(query)
        if commit:
            self.conn.commit()

        if commit:
            self.conn.analyze("platform.platform_values_apps")        
            
    def recreate_table(self, table_name: str, query: str, temporary=True):
        query = query.replace("x__table_name__x", table_name)
        if temporary:
            query = query.replace("CREATE TABLE", f"CREATE {self.get_temporary()} TABLE")
        self.conn.query(f"drop table if exists platform_values.{table_name}")
        self.conn.query(query)
        self.conn.analyze(f"platform_values.{table_name}")

    def __age_to_def(self, age: int):
        if self.rows_age_groups is None:
            self.rows_age_groups = self.conn.select_all("""
                        SELECT g.age_from, 
                               g.age_to, 
                               g.group_name
                          FROM app.def_sheet_platform_age_groups g
                        """)

        for row in self.rows_age_groups:
            if row["age_from"] <= age <= row["age_to"]:
                return {
                    "from": row["age_from"],
                    "to": row["age_to"],
                    "group_name": row["group_name"]
                }
        return None            
    
    def get_temporary(self) -> str:
        return "TEMPORARY" if self.is_temporary() else ""
    
    def is_temporary(self) -> bool:
        return True
        if rr.ENV == rr.ENV_DEV:
            return False
        return not rr.is_debug()    
    
    def run_tags(self, apps_ids_int: list[int] = [], commit=True):

        if len(apps_ids_int) > 0:
            query = f"""
                    CREATE TABLE platform.platform_values_tags
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
                      FROM platform.platform_values_apps a,
                           tagged_data.platform_tagged pt,
                           tagged_data.tags_v t,
                           platform.def_tags p
                     WHERE a.app_id_int = t.app_id_int
                       AND t.tag_id_int = p.tag_id_int
                       AND pt.app_id_int = a.app_id_int
                    """
            self.conn.query_safe("DROP TABLE IF EXISTS platform.platform_values_tags")
            self.conn.query(query)
        else:
            app_ids_int_db = self.conn.values_arr_to_db_in(apps_ids_int, int_values=True)
            self.conn.query(f"""
            DELETE FROM platform.platform_values_tags WHERE app_id_int IN ({app_ids_int_db})
            """)
            query = f"""
            INSERT INTO platform.platform_values_tags(app_id_int, tag_id_int, tag_rank)
            SELECT a.app_id_int, 
                   t.tag_id_int, 
                   t.tag_rank
              FROM platform.platform_values_apps a,
                   tagged_data.platform_tagged pt,
                   tagged_data.tags_v t,
                   platform.def_tags p
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
            
    def run_tags_bin(self, apps_ids_int: list[int] = [], commit=True):
        if len(apps_ids_int) > 0:
            bin_bytes_cnt = self.conn.select_one("""
            SELECT cnt FROM platform.bin_bytes_cnt
            """)["cnt"]
            self.conn.query("DROP TABLE if exists platform.platform_values_tags_bin")
            self.conn.query(f"""
            CREATE TABLE platform.platform_values_tags_bin (
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
              FROM platform.def_tags_bin tb,
                   platform.platform_values_tags t
             WHERE tb.tag_id_int = t.tag_id_int
             GROUP BY t.app_id_int
             ) z1
            """)
            if commit:
                self.conn.commit()
        else:
            app_ids_int_db = self.conn.values_arr_to_db_in(apps_ids_int, int_values=True)
            self.conn.query(f"""
            DELETE FROM platform.platform_values_tags_bin WHERE app_id_int IN ({app_ids_int_db})
            """)
            self.conn.query(f"""
            INSERT INTO platform.platform_values_tags_bin (
                app_id_int,
                tags_md5,
                b
            )
            SELECT z1.app_id_int, MD5(z1.b) as tags_md5, z1.b
              FROM (
            SELECT t.app_id_int, 
                   BIT_OR(tb.b) AS b
              FROM platform.def_tags_bin tb,
                   platform.platform_values_tags t
             WHERE tb.tag_id_int = t.tag_id_int
               AND t.app_id_int IN ({app_ids_int_db})
             GROUP BY t.app_id_int
             ) z1
            """)
            if commit:
                self.conn.commit()

        if commit:
            self.conn.analyze("platform.platform_values_tags_bin")            