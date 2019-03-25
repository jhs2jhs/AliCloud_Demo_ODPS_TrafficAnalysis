-- DROP TABLE IF EXISTS ods_visit_log_dd;
CREATE TABLE IF NOT EXISTS ods_visit_log_dd (
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
    visit_type STRING COMMENT 'request type crawler feed user unknown',
    region STRING COMMENT 'geogrpahical location according to IP address'
) PARTITIONED BY (
    dt STRING
);

INSERT OVERWRITE TABLE ods_visit_log_dd PARTITION (dt=${bdp.system.bizdate})
SELECT 
    uid, ip, time, http_status, traffic_bytes, http_method, url, 
    http_protocol, host, device, visit_type,
    get_region_from_ip(ip) as region
FROM ods_tmp_visit_log_dd
WHERE dt = ${bdp.system.bizdate};
