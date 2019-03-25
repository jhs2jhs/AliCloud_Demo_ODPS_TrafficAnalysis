-- DDL
CREATE TABLE IF NOT EXISTS dim_host_dd (
    host STRING COMMENT 'host',
    description STRING COMMENT 'device description'
)
PARTITIONED BY (
    dt STRING
);

-- DML
INSERT OVERWRITE TABLE dim_host_dd PARTITION (dt=${bdp.system.bizdate})
SELECT host, host AS description 
FROM (
    SELECT 
        DISTINCT(host) as host
    FROM ods_visit_log_dd
    WHERE dt = ${bdp.system.bizdate}
) a;
