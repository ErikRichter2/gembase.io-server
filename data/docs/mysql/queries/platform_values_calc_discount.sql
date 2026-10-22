DROP FUNCTION IF EXISTS platform.calc_discount;
DELIMITER $$

CREATE FUNCTION platform.calc_discount(
yearly_revenues bigint unsigned,
quality int unsigned,
released_years int unsigned
)
RETURNS int unsigned
DETERMINISTIC
BEGIN

    DECLARE revenues_ds int UNSIGNED;
    DECLARE quality_ds int UNSIGNED;
    DECLARE experience_ds int UNSIGNED;

    SET revenues_ds = 0;
    SET quality_ds = 0;
    SET experience_ds = 0;

    IF yearly_revenues > 0 THEN
        SET revenues_ds = (SELECT platform.calc_discount_revenues(yearly_revenues));
    END IF;

    IF quality > 0 THEN
        SET quality_ds = (SELECT platform.calc_discount_quality(quality));
    END IF;

    IF released_years > 0 THEN
        SET experience_ds = (SELECT platform.calc_discount_experience(released_years));
    END IF;

    RETURN (revenues_ds + quality_ds + experience_ds);
END $$

DELIMITER ;
