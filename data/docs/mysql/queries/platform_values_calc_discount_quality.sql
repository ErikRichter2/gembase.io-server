DROP FUNCTION IF EXISTS platform.calc_discount_quality;
DELIMITER $$

CREATE FUNCTION platform.calc_discount_quality(
quality int unsigned
)
RETURNS int unsigned
DETERMINISTIC
BEGIN

    DECLARE quality_ds int UNSIGNED;

    SET quality_ds = 0;

    IF quality > 0 THEN
        SET quality_ds = ROUND(LEAST(quality / 100 * 0.1, 0.1) * 100);
    END IF;

    RETURN quality_ds;
END $$

DELIMITER ;
