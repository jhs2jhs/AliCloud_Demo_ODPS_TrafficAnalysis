--DDL dw_user_info_all_d 
CREATE TABLE IF NOT EXISTS dwd_event_visit_dd (
    uid STRING COMMENT 'user ID',
    gender STRING COMMENT 'gender',
    age_range STRING COMMENT 'age range',
    zodiac STRING COMMENT 'zodiac',
    ----
    region STRING COMMENT 'region from parsing ip',
    ----
    device STRING COMMENT 'client type',
    ----
    host STRING COMMENT 'source url',
    http_method STRING COMMENT 'http request type',
    url STRING COMMENT 'url',
    visit_type STRING COMMENT 'request type crawler feed user unknown',
    time STRING COMMENT 'time yyyymmddhh:mi:ss'
) PARTITIONED BY (
    dt STRING
);

-- comments
INSERT OVERWRITE TABLE dwd_event_visit_dd PARTITION (dt='${bdp.system.bizdate}')
SELECT 
    COALESCE(a.uid, b.uid) AS uid, 
    b.gender, 
    b.age_range, 
    b.zodiac, 
    a.region, 
    a.device, 
    a.host, 
    a.http_method, 
    a.url, 
    a.visit_type, 
    a.time
FROM (
    SELECT * FROM ods_visit_log_dd WHERE dt = ${bdp.system.bizdate}
) a
LEFT OUTER JOIN (
    SELECT * FROM dim_user_dd WHERE dt = ${bdp.system.bizdate}
) b
ON a.uid = b.uid;
