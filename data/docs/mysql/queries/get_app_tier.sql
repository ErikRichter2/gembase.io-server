DROP FUNCTION IF EXISTS scraped_data.get_app_tier;
DELIMITER $$

CREATE FUNCTION scraped_data.get_app_tier(
app_id_int int unsigned
)
RETURNS int
DETERMINISTIC
BEGIN

    DECLARE app_tier int UNSIGNED;

    SET app_tier = (
        SELECT t.tier
          FROM app.def_sheet_platform_values_install_tiers t,
               scraped_data.apps a
         WHERE t.store_id = a.store
           AND a.installs >= t.value_from
           AND a.installs < t.value_to
           AND a.app_id_int = app_id_int
        );

    RETURN app_tier;
END $$

DELIMITER ;
