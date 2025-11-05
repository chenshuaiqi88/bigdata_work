drop table device_platform_stats;
CREATE TABLE IF NOT EXISTS bigdata_realtime_lululemon_report_v1.device_platform_stats (
    `platform` VARCHAR(20) COMMENT '系统平台：Android/iOS',
    `brand` VARCHAR(50) COMMENT '品牌名称',
    `model` VARCHAR(100) COMMENT '设备型号',
    `stat_date` DATE COMMENT '统计日期',
    `count` BIGINT COMMENT '出现次数',
    `proc_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据处理时间'
    )
    DUPLICATE KEY(`platform`, `brand`, `model`, `stat_date`)
    DISTRIBUTED BY HASH(`platform`) BUCKETS 8
    PROPERTIES (
    "replication_num" = "1"
               );


STOP ROUTINE LOAD FOR bigdata_realtime_lululemon_report_v1.kafka_device_platform_stats;
CREATE ROUTINE LOAD bigdata_realtime_lululemon_report_v1.kafka_device_platform_stats ON device_platform_stats
PROPERTIES
(
    "format" = "json",
    "strict_mode" = "false",
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "20",
    "max_batch_rows" = "200000",
    "max_batch_size" = "209715200",
    "strip_outer_array" = "false",
    "jsonpaths" = "[
        \"$.system\",
        \"$.brand\",
        \"$.model\",
        \"$.stat_date\",
        \"$.count\",
        \"$.proc_time\"
    ]"
)FROM KAFKA
(
    "kafka_broker_list" = "172.24.219.66:9092",
    "kafka_topic" = "realtime_v3_deviceTopStats",
    "property.group.id" = "doris_single_view_consumer",
    "property.offset" = "earliest",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);



select *
from device_platform_stats;