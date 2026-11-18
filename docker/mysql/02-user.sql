-- Local development user (credentials match docker.env.example).
-- Grants access to all Gembase databases (gb_app, gb_platform, gb_calc, ...).
CREATE USER IF NOT EXISTS 'gembase'@'%' IDENTIFIED BY 'gembase';
GRANT ALL PRIVILEGES ON `gb\_%`.* TO 'gembase'@'%';
FLUSH PRIVILEGES;
