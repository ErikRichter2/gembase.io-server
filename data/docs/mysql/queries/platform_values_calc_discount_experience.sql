DROP FUNCTION IF EXISTS platform.calc_discount_experience;
DELIMITER $$

CREATE FUNCTION platform.calc_discount_experience(
released_years int unsigned
)
RETURNS int unsigned
DETERMINISTIC
BEGIN

    DECLARE experience_ds int UNSIGNED;

    SET experience_ds = 0;

    IF released_years > 0 THEN
        SET experience_ds = ROUND(LEAST(released_years / 10 * 0.1, 0.1) * 100);
    END IF;

    RETURN experience_ds;
END $$

DELIMITER ;
