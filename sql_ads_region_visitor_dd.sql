-- DDL
CREATE TABLE IF NOT EXISTS ads_region_visitor_dd (
    region STRING COMMENT 'region',
    visitors BIGINT COMMENT 'visitors count'
)
PARTITIONED BY (
    dt STRING
);

-- DML
INSERT OVERWRITE TABLE ads_region_visitor_dd PARTITION (dt=${bdp.system.bizdate})
SELECT region, count(uid) AS visitors 
FROM (
    SELECT region, uid
    FROM dws_event_visit_pv_dd
    WHERE dt = ${bdp.system.bizdate}
    GROUP BY region, uid
) a
GROUP BY region;
