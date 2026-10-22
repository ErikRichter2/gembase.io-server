DROP FUNCTION IF EXISTS platform.calc_loyalty_installs;
DELIMITER $$

CREATE FUNCTION platform.calc_loyalty_installs(
installs BIGINT UNSIGNED,
months SMALLINT UNSIGNED,
retention DECIMAL(10, 4)
)
RETURNS int
DETERMINISTIC
BEGIN
    DECLARE cnt INT;
    DECLARE res DECIMAL(16, 4);
    SET cnt = 0;
    SET res = 0;

    IF retention <= 0 THEN
      RETURN 0;
    END IF;

    IF months = 0 THEN
       RETURN (installs * retention);
    END IF;

    REPEAT
        SET cnt = cnt + 1;
        SET res = res + (installs / months) * POW(retention, cnt);
    UNTIL cnt >= months END REPEAT;

    RETURN round(res);
END $$

DELIMITER ;