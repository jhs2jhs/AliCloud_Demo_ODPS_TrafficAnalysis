-- DDL to store data ingested from MySQL
CREATE TABLE IF NOT EXISTS ods_mysql_s_user (
    uid STRING COMMENT 'user ID',
    gender STRING COMMENT 'gender',
    age_range STRING COMMENT 'age range, e.g. 30-40 year old',
    zodiac STRING COMMENT 'zodiac'
)
PARTITIONED BY (
    dt STRING
);
