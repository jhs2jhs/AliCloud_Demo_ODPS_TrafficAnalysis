-- DDL
CREATE TABLE IF NOT EXISTS dim_device_dd (
    device STRING COMMENT 'device',
    description STRING COMMENT 'device description'
)
PARTITIONED BY (
    dt STRING
);

-- DML
INSERT OVERWRITE TABLE dim_device_dd PARTITION (dt=${bdp.system.bizdate})
SELECT device, device AS description 
FROM (
    SELECT 
        DISTINCT(device) as device
    FROM ods_visit_log_dd
    WHERE dt = ${bdp.system.bizdate}
) a;
