CREATE TABLE IF NOT EXISTS single_view (
    `day` VARCHAR(8) COMMENT '统计日期，格式：YYYYMMDD',
    `page` VARCHAR(50) COMMENT '页面名称',
    `pv` BIGINT COMMENT '页面访问量',
    `user_ids` VARCHAR(10000) COMMENT '用户ID数组JSON字符串',
    `emit_time` DATETIME COMMENT '数据发射时间',
    `proc_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据处理时间'
    )
    DUPLICATE KEY(day, page)
    DISTRIBUTED BY HASH(day) BUCKETS 8
    PROPERTIES (
    "replication_num" = "1"
               );


STOP ROUTINE LOAD FOR bigdata_realtime_lululemon_report_v1.kafka_single_view;
CREATE ROUTINE LOAD bigdata_realtime_lululemon_report_v1.kafka_single_view ON single_view
PROPERTIES
(
    "format" = "json",
    "strict_mode" = "false",
    "max_batch_rows" = "300000",
    "max_batch_interval" = "30",
    "jsonpaths" = "[
        \"$.day\",
        \"$.page\",
        \"$.pv\",
        \"$.user_ids\",
        \"$.emit_time\"
    ]"
)
FROM KAFKA
(
    "kafka_broker_list" = "172.24.219.66:9092",
    "kafka_topic" = "realtime_v3_singleViewAccDs",
    "property.group.id" = "doris_single_view_consumer",
    "property.offset" = "earliest",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);



select *
from single_view;