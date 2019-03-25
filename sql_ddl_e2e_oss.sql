CREATE TABLE IF NOT EXISTS ods_oss_log_dd (
    text STRING
)
PARTITIONED BY (
    dt STRING
);
