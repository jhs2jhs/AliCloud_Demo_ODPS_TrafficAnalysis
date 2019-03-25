-- DDL to store ods_log_info_d 
CREATE TABLE IF NOT EXISTS ods_tmp_visit_log_dd (
    uid STRING COMMENT 'user ID',
    ip STRING COMMENT 'ip address',
    time STRING COMMENT 'time yyyymmddhh:mi:ss',
    http_status STRING COMMENT 'server responsed status code',
    traffic_bytes STRING COMMENT 'client responsed bite count',
    http_method STRING COMMENT 'http request type',
    url STRING COMMENT 'url',
    http_protocol STRING COMMENT 'http protocal version',
    host STRING COMMENT ' source url',
    device STRING COMMENT 'client type ',
    visit_type STRING COMMENT 'request type crawler feed user unknown'
) PARTITIONED BY (
    dt STRING
);

-- DML

-- Using UDF to parse IP to get region 
-- do in another jobs  , getregion(ip) AS region -- split request into three dicts in regexp

INSERT OVERWRITE TABLE ods_tmp_visit_log_dd PARTITION (dt=${bdp.system.bizdate})
SELECT uid, ip, time, status, bytes, 
    regexp_substr(request, '(^[^ ]+ )') AS method, 
    regexp_extract(request, '^[^ ]+ (.*) [^ ]+$') AS url, 
    regexp_substr(request, '([^ ]+$)') AS protocol, --parse refer to get more accurate url, 
    regexp_extract(referer, '^[^/]+://([^/]+){1}') AS referer, --parse agent to get client infor and request method, 
    CASE
        WHEN TOLOWER(agent) RLIKE 'android' THEN 'android'
        WHEN TOLOWER(agent) RLIKE 'iphone' THEN 'iphone'
        WHEN TOLOWER(agent) RLIKE 'ipad' THEN 'ipad'
        WHEN TOLOWER(agent) RLIKE 'macintosh' THEN 'macintosh'
        WHEN TOLOWER(agent) RLIKE 'windows phone' THEN 'windows_phone'
        WHEN TOLOWER(agent) RLIKE 'windows' THEN 'windows_pc'
        ELSE 'unknown'
    END AS device, 
    CASE
        WHEN TOLOWER(agent) RLIKE '(bot|spider|crawler|slurp)' THEN 'crawler'
        WHEN TOLOWER(agent) RLIKE 'feed' OR regexp_extract(request, '^[^ ]+ (.*) [^ ]+$') RLIKE 'feed' THEN 'feed'
        WHEN TOLOWER(agent) NOT RLIKE '(bot|spider|crawler|feed|slurp)' AND agent RLIKE '^[Mozilla|Opera]' AND regexp_extract(request, '^[^ ]+ (.*) [^ ]+$') NOT RLIKE 'feed' THEN 'user'
        ELSE 'unknown'
    END AS identity
FROM (
    SELECT 
        SPLIT(text, '##@@')[0] AS ip, 
        SPLIT(text, '##@@')[1] AS uid, 
        SPLIT(text, '##@@')[2] AS time, 
        SPLIT(text, '##@@')[3] AS request, 
        SPLIT(text, '##@@')[4] AS status, 
        SPLIT(text, '##@@')[5] AS bytes, 
        SPLIT(text, '##@@')[6] AS referer, 
        SPLIT(text, '##@@')[7] AS agent
    FROM ods_oss_log_dd
    WHERE dt = ${bdp.system.bizdate}
) a;

