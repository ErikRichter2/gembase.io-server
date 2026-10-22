DROP FUNCTION IF EXISTS scraped_data.get_app_growth;
DELIMITER $$

CREATE FUNCTION scraped_data.get_app_growth(
app_id_int_input int unsigned
)
RETURNS bigint
DETERMINISTIC
BEGIN

    DECLARE app_growth bigint;

    SET app_growth = (
        SELECT IF(p.app_id_int IS NULL, (IF(TIMESTAMPDIFF(YEAR, a.released, CURDATE()) = 0, a.installs, ROUND(a.installs / TIMESTAMPDIFF(YEAR, a.released, CURDATE())))), p.growth) as growth
          FROM scraped_data.apps a
          LEFT JOIN platform.platform_values_apps p ON p.app_id_int = a.app_id_int
         WHERE a.app_id_int = app_id_int_input
        );

    RETURN app_growth;
END $$

DELIMITER ;
