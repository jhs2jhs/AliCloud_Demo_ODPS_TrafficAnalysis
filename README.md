# AliCloud Dataworks Demo on Web Traffic Analysis

# Dataworks

DataWorks is a Big Data platform product launched by Alibaba Cloud. It provides one-stop Big Data development, data permission management, offline job scheduling, and other features. You can read more on [product page](https://www.alibabacloud.com/product/ide). It includes key features, such as: 

1. Development Visualization: You can drag and drop nodes to create a workflow. You can also edit and debug your code online, and ask other developers to join you.
2. Multiple Task Types: Supports data integration, MaxCompute SQL, MaxCompute MR, machine learning, and shell tasks.
3. Strong Scheduling Capability: Runs millions of tasks concurrently and supports hourly, daily, weekly, and monthly schedules.
4. Task Monitoring and Alarms: Supports task monitoring and sends alarms when errors occur to avoid service interruptions.


# Data Model
![Alt text](/demo_screenshot/data_model.png)


# Workshop
## create Dataworks workspace
* It is recommended to create a workspace in __China East 2__ region (Shanghai). 
* It is recommended to create a workspace in __Standard__ mode. Standard will create a seperate Dev and Prod envionrment and would allow project control. 
![Alt text](/demo_screenshot/dataworks_create_workspace.jpg)
![Alt text](/demo_screenshot/dataworks_standard_mode.jpg)

## configure external data source for ingestion
### connect to mysql: [configuration](/config_mysql_in.sql)
![Alt text](/demo_screenshot/datasource_mysql_in.jpg)
```
Data Source Type: ApasaraDB for RDS
Data Source Name: rds_workshop_log
Description：rds log ingest
RDS Instance ID: rm-bp1z69dodhh85z9qa
RDS Instance Account: 1156529087455811
Database name: workshop
Username: workshop
Password: workshop#2017
```
### connect to oss: [configuration](/datasource_oss_in.jpg)
![Alt text](/demo_screenshot/datasource_oss_in.jpg)
```
Data Source Name：oss_workshop_log
Endpoint：http://oss-cn-shanghai-internal.aliyuncs.com
bucket：dataworks-workshop
AccessKey ID：LTAINEhd4MZ8pX64
AccessKey Key：lXnzUngTSebt3SfLYxZxoSjGAK6IaF
```
## create a virtual node for starting point
* make sure the very first virtual node has setup __root node__ dependency. 
![Alt text](/demo_screenshot/virtual_node_root.jpg)

## data ingestion for mysql [ODS]
### create table to host mysql data ingestion: [sql](sql_dd_e2e_mysql.sql)
* table partition is always recommended. 
```
CREATE TABLE IF NOT EXISTS ods_mysql_s_user (
    uid STRING COMMENT 'user ID',
    gender STRING COMMENT 'gender',
    age_range STRING COMMENT 'age range, e.g. 30-40 year old',
    zodiac STRING COMMENT 'zodiac'
)
PARTITIONED BY (
    dt STRING
);
```
### configure mysql ingestion task
![Alt text](/demo_screenshot/ingest_mysql.jpg)

### ods_user: [sql](/demo_screenshot/sql_ods_s_user_dd.sql)
```
CREATE TABLE IF NOT EXISTS ods_s_user_dd (
    uid STRING COMMENT 'user ID',
    gender STRING COMMENT 'gender',
    age_range STRING COMMENT 'age range, e.g. 30-40 year old',
    zodiac STRING COMMENT 'zodiac'
)
PARTITIONED BY (
    dt STRING
);

INSERT OVERWRITE TABLE ods_s_user_dd PARTITION (dt=${bdp.system.bizdate})
SELECT uid, gender, age_range, zodiac
FROM ods_mysql_s_user
WHERE dt = ${bdp.system.bizdate};
```

## data ingestion for oss [ods]

### create table to host mysql data ingestion: [sql](sql_dd_e2e_mysql.sql)
* table partition is always recommended. 

### configure oss ingestion task
![Alt text](/demo_screenshot/ingest_oss.jpg)

### UDF
* download [udf jar](/udf/ip2region.jar) file and upload into __resource__ folder
![Alt text](/demo_screenshot/udf_get_region_ip.jpg)

### ods_visit_log_tmp: [sql](sql_ods_tmp_visit_log_dd.sql)
```
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
```

### ods_visit_log: [sql](sql_ods_visit_log_dd.sql)
```
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
```

## dimentionaldata : dim

### dim_user: [sql](/sql_dim_user.sql)
```
CREATE TABLE IF NOT EXISTS dim_user_dd (
    uid STRING COMMENT 'user ID',
    gender STRING COMMENT 'gender',
    age_range STRING COMMENT 'age range, e.g. 30-40 year old',
    zodiac STRING COMMENT 'zodiac'
)
PARTITIONED BY (
    dt STRING
);

INSERT OVERWRITE TABLE dim_user_dd PARTITION (dt=${bdp.system.bizdate})
SELECT uid, gender, age_range, zodiac
FROM ods_s_user_dd
WHERE dt = ${bdp.system.bizdate};
```

### dim_region_dd: [sql](/sql_dim_region_dd.sql)
```
CREATE TABLE IF NOT EXISTS dim_region_dd (
    region STRING COMMENT 'geogrpahical location',
    description STRING COMMENT 'geogrpahical location description'
)
PARTITIONED BY (
    dt STRING
);

INSERT OVERWRITE TABLE dim_region_dd PARTITION (dt=${bdp.system.bizdate})
SELECT 
    DISTINCT(region), 
    'desc' AS description  
FROM ods_visit_log_dd
WHERE dt = ${bdp.system.bizdate};
```

### dim_host_dd: [sql](/sql_dim_host_dd.sql)
```
CREATE TABLE IF NOT EXISTS dim_host_dd (
    host STRING COMMENT 'host',
    description STRING COMMENT 'device description'
)
PARTITIONED BY (
    dt STRING
);

INSERT OVERWRITE TABLE dim_host_dd PARTITION (dt=${bdp.system.bizdate})
SELECT host, host AS description 
FROM (
    SELECT 
        DISTINCT(host) as host
    FROM ods_visit_log_dd
    WHERE dt = ${bdp.system.bizdate}
) a;
```

### dim_device_dd: [sql](/sql_dim_device_dd.sql)
```
CREATE TABLE IF NOT EXISTS dim_device_dd (
    device STRING COMMENT 'device',
    description STRING COMMENT 'device description'
)
PARTITIONED BY (
    dt STRING
);

INSERT OVERWRITE TABLE dim_device_dd PARTITION (dt=${bdp.system.bizdate})
SELECT device, device AS description 
FROM (
    SELECT 
        DISTINCT(device) as device
    FROM ods_visit_log_dd
    WHERE dt = ${bdp.system.bizdate}
) a;
```

## dwd_visit_dd: [sql](/sql_dwd_visit_dd.sql)
```
CREATE TABLE IF NOT EXISTS dwd_event_visit_dd (
    uid STRING COMMENT 'user ID',
    gender STRING COMMENT 'gender',
    age_range STRING COMMENT 'age range',
    zodiac STRING COMMENT 'zodiac',
    region STRING COMMENT 'region from parsing ip',
    device STRING COMMENT 'client type',
    host STRING COMMENT 'source url',
    http_method STRING COMMENT 'http request type',
    url STRING COMMENT 'url',
    visit_type STRING COMMENT 'request type crawler feed user unknown',
    time STRING COMMENT 'time yyyymmddhh:mi:ss'
) PARTITIONED BY (
    dt STRING
);

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
```

### dws_device_region_pv_dd: [sql](/sql_dws_device_region_pv_dd.sql)
```
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
```

### dws_host_pv_dd: [sql](/sql_dws_host_pv_dd.sql)
```
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
```

### dws_user_region_device_pv_dd: [sql](/sql_dws_user_region_device_pv_dd.sql)
```
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
```

## ads_region_visitor_dd [ADS]: [sql](/sql_ads_region_visitor_dd.sql)
```
CREATE TABLE IF NOT EXISTS ads_region_visitor_dd (
    region STRING COMMENT 'region',
    visitors BIGINT COMMENT 'visitors count'
)
PARTITIONED BY (
    dt STRING
);

INSERT OVERWRITE TABLE ads_region_visitor_dd PARTITION (dt=${bdp.system.bizdate})
SELECT region, count(uid) AS visitors 
FROM (
    SELECT region, uid
    FROM dws_event_visit_pv_dd
    WHERE dt = ${bdp.system.bizdate}
    GROUP BY region, uid
) a
GROUP BY region;
```

## worktask overview
![Alt text](/demo_screenshot/workflow_overview.jpg)
