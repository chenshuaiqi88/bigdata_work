drop table bigdata_realtime_lululemon_report_v1.compute_region;
CREATE TABLE IF NOT EXISTS bigdata_realtime_lululemon_report_v1.compute_region (
    `date` VARCHAR(8) COMMENT '统计日期，格式：YYYYMMDD',
    `region` VARCHAR(100) COMMENT '完整地区信息，格式：省|市|运营商',
    `province` VARCHAR(50) COMMENT '省份',
    `city` VARCHAR(50) COMMENT '城市',
    `district` VARCHAR(50) COMMENT '区域/运营商',
    `count` BIGINT COMMENT '统计数量',
    `abnormal` BOOLEAN COMMENT '是否异常',
    `region_raw` VARCHAR(200) COMMENT '原始地区信息，包含国家',
    `type` VARCHAR(20) COMMENT '数据类型：periodic(周期)/history(历史)',
    `ip` VARCHAR(50) COMMENT 'IP地址',
    `proc_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据处理时间'
    )
    DUPLICATE KEY(date, region, province, city)  -- 使用前4个字段
    DISTRIBUTED BY HASH(date) BUCKETS 8
    PROPERTIES (
    "replication_num" = "1"
               );

SHOW ROUTINE LOAD FOR bigdata_realtime_lululemon_report_v1.kafka_compute_region;
STOP ROUTINE LOAD FOR bigdata_realtime_lululemon_report_v1.kafka_compute_region;

CREATE ROUTINE LOAD bigdata_realtime_lululemon_report_v1.kafka_compute_region ON compute_region
PROPERTIES
(
    "format" = "json",
    "strict_mode" = "false",
    "max_batch_rows" = "300000",
    "max_batch_interval" = "30",
    "jsonpaths" = "[
        \"$.date\",
        \"$.region\",
        \"$.province\",
        \"$.city\",
        \"$.district\",
        \"$.count\",
        \"$.abnormal\",
        \"$.region_raw\",
        \"$.type\",
        \"$.ip\"
    ]"
)
FROM KAFKA
(
    "kafka_broker_list" = "172.24.219.66:9092",
    "kafka_topic" = "realtime_v3_computeRegionDs",
    "property.group.id" = "doris_search_topn_consumer",
    "property.offset" = "earliest",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);


select *
from compute_region;
