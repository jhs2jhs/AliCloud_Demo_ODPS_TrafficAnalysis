# AliCloud_Demo_ODPS_DataWarehouse

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

## data ingestion
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

### configure mysql ingestion task: [sql](/demo_screenshot/sql_ods_s_user_dd.sql)
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

-- DML
INSERT OVERWRITE TABLE ods_s_user_dd PARTITION (dt=${bdp.system.bizdate})
SELECT uid, gender, age_range, zodiac
FROM ods_mysql_s_user
WHERE dt = ${bdp.system.bizdate};
```


## worktask overview
![Alt text](/demo_screenshot/workflow_overview.jpg)
