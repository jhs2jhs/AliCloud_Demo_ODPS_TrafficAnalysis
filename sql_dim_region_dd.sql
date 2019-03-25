-- DDL to store data ingested from MySQL
CREATE TABLE IF NOT EXISTS dim_region_dd (
    region STRING COMMENT 'geogrpahical location',
    description STRING COMMENT 'geogrpahical location description'
)
PARTITIONED BY (
    dt STRING
);

-- DML
INSERT OVERWRITE TABLE dim_region_dd PARTITION (dt=${bdp.system.bizdate})
SELECT 
    DISTINCT(region), 
    'desc' AS description  
FROM ods_visit_log_dd
WHERE dt = ${bdp.system.bizdate};
