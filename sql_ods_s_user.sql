--DROP TABLE IF EXISTS dim_user_dd;
CREATE TABLE IF NOT EXISTS ods_s_user_dd (
    uid STRING COMMENT 'user ID',
    gender STRING COMMENT 'gender',
    age_range STRING COMMENT 'age range, e.g. 30-40 year old',
    zodiac STRING COMMENT 'zodiac'
)
PARTITIONED BY (
    dt STRING
);

-- DML
INSERT OVERWRITE TABLE ods_s_user_dd PARTITION (dt=${bdp.system.bizdate})
SELECT uid, gender, age_range, zodiac
FROM ods_mysql_s_user
WHERE dt = ${bdp.system.bizdate};
