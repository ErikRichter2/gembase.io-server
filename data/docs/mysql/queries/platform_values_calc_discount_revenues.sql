DROP FUNCTION IF EXISTS platform.calc_discount_revenues;
DELIMITER $$

CREATE FUNCTION platform.calc_discount_revenues(
yearly_revenues bigint unsigned
)
RETURNS int unsigned
DETERMINISTIC
BEGIN

    DECLARE revenues_ds int UNSIGNED;

    SET revenues_ds = 0;

    IF yearly_revenues > 0 THEN
        SET revenues_ds = ROUND(LEAST(yearly_revenues / (100 * 1000 * 1000) * 0.2, 0.2) * 100);
    END IF;

    RETURN revenues_ds;
END $$

DELIMITER ;
