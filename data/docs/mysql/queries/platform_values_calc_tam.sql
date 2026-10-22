DROP FUNCTION IF EXISTS platform.calc_tam;
DELIMITER $$

CREATE FUNCTION platform.calc_tam(
    total_audience bigint,
    arpu decimal(8, 4)
)
RETURNS bigint
DETERMINISTIC
BEGIN
    RETURN round(total_audience * arpu);
END $$

DELIMITER ;
