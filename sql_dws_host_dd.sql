--ddl to store rpt_user_info_d 
CREATE TABLE IF NOT EXISTS dws_host_pv_dd (
    host STRING COMMENT 'host ',
    pv BIGINT COMMENT 'pv COUNT'
)
PARTITIONED BY (
    dt STRING
);

INSERT OVERWRITE TABLE dws_host_pv_dd PARTITION (dt='${bdp.system.bizdate}')
SELECT 
    host, COUNT(0) AS pv
FROM dwd_event_visit_dd
WHERE dt = ${bdp.system.bizdate}
GROUP BY host;
