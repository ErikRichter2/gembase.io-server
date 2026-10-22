DROP FUNCTION IF EXISTS platform.calc_total_audience;
DELIMITER $$

CREATE FUNCTION platform.calc_total_audience(
loved_survey_cnt int unsigned,
total_survey_cnt int unsigned,
rejected_survey_cnt int unsigned,
potential_downloads bigint unsigned,
loyalty_installs bigint unsigned
)
RETURNS bigint
DETERMINISTIC
BEGIN

    DECLARE total_audience BIGINT UNSIGNED;
    DECLARE loved_absolute BIGINT UNSIGNED;
    DECLARE rejected_absolute BIGINT UNSIGNED;
    DECLARE loyal_absolute BIGINT UNSIGNED;

    SET loved_absolute = 0;
    SET rejected_absolute = 0;
    SET loyal_absolute = loyalty_installs;
    SET total_audience = 0;

    IF total_survey_cnt > 0 THEN
        SET loved_absolute = (loved_survey_cnt / total_survey_cnt) * potential_downloads;
    END IF;

    IF loved_survey_cnt > 0 THEN
        SET rejected_absolute = loved_absolute * (rejected_survey_cnt / loved_survey_cnt);
    END IF;

    IF loved_absolute > rejected_absolute + loyal_absolute THEN
        SET total_audience = loved_absolute - (rejected_absolute + loyal_absolute);
    END IF;

    RETURN total_audience;
END $$

DELIMITER ;
