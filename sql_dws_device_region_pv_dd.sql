-- ddl to store rpt_user_info_d 
CREATE TABLE IF NOT EXISTS dws_device_region_pv_dd (
    device STRING COMMENT 'client type',
    region STRING COMMENT 'region from parsing ip',
    pv BIGINT COMMENT 'pv COUNT'
)
PARTITIONED BY (
    dt STRING
);

INSERT OVERWRITE TABLE dws_device_region_pv_dd PARTITION (dt='${bdp.system.bizdate}')
SELECT 
    device, region, COUNT(0) AS pv
FROM dwd_event_visit_dd
WHERE dt = ${bdp.system.bizdate}
GROUP BY device, region;
