--ddl to store rpt_user_info_d 
CREATE TABLE IF NOT EXISTS dws_event_visit_pv_dd (
    uid STRING COMMENT 'user ID',
    gender STRING COMMENT 'gender',
    age_range STRING COMMENT 'age range',
    zodiac STRING COMMENT 'zodiac',
    ----
    region STRING COMMENT 'region from parsing ip',
    device STRING COMMENT 'client type',
    ----
    pv BIGINT COMMENT 'pv'
) PARTITIONED BY (
    dt STRING
);

INSERT OVERWRITE TABLE dws_event_visit_pv_dd PARTITION (dt='${bdp.system.bizdate}')
SELECT 
    uid, MAX(gender), MAX(age_range), MAX(zodiac), 
    MAX(region), MAX(device), COUNT(0) AS pv
FROM dwd_event_visit_dd
WHERE dt = ${bdp.system.bizdate}
GROUP BY uid;
