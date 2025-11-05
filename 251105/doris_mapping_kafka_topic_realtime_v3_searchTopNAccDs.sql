show databases ;
use bigdata_realtime_lululemon_report_v1;
show tables ;

drop table bigdata_realtime_lululemon_report_v1.search_topn;
CREATE TABLE IF NOT EXISTS search_topn (
    `day` VARCHAR(10) COMMENT '统计日期',
    `word` VARCHAR(100) COMMENT '热词',
    `cnt` BIGINT COMMENT '出现次数',
    `emit_time` DATETIME COMMENT '数据发射时间',
    `proc_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据处理时间'
    )
    DUPLICATE KEY(day, word)
    DISTRIBUTED BY HASH(day) BUCKETS 8
    PROPERTIES (
    "replication_num" = "1"
               );

SHOW ROUTINE LOAD FOR bigdata_realtime_lululemon_report_v1.kafka_search_topn;
STOP ROUTINE LOAD FOR bigdata_realtime_lululemon_report_v1.kafka_search_topn;

CREATE ROUTINE LOAD bigdata_realtime_lululemon_report_v1.kafka_search_topn ON search_topn
PROPERTIES
(
    "format" = "json",
    "strict_mode" = "false",
    "max_batch_rows" = "300000",
    "max_batch_interval" = "30",
    "jsonpaths" = "[
        \"$.day\",
        \"$.top10[0].word\",
        \"$.top10[0].cnt\",
        \"$.emit_time\"
    ]"
)
FROM KAFKA
(
    "kafka_broker_list" = "172.24.219.66:9092",
    "kafka_topic" = "realtime_v3_searchTopNAccDs",
    "property.group.id" = "doris_search_topn_consumer",
    "property.offset" = "earliest",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);


select *
from search_topn;