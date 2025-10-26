package com.stream.realtime.lululemon;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shuaiqi.chen
 * @create 2025-10-26-19:57
 */
public class DbusSyncPgCdc2Kafka {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        System.setProperty("HADOOP_USER_NAME","root");

        DebeziumDeserializationSchema<String> deserializer = new JsonDebeziumDeserializationSchema();
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("172.24.219.66")
                        .port(5432)
                        .database("spider_db")
                        .schemaList("public")
                        .tableList("public.spider_lululemon_jd_product_dtl")
                        .username("postgres")
                        .password("Csq0573,./")
                        .slotName("flink_etl_cdc_test")
                        .deserializer(deserializer)
                        .decodingPluginName("pgoutput")
                        .includeSchemaChanges(false)
                        .startupOptions(StartupOptions.initial())
                        .build();

        // 原始CDC数据流
        DataStream<String> cdcStream = env.fromSource(
                postgresIncrementalSource,
                WatermarkStrategy.noWatermarks(),
                "PostgresParallelSource"
        ).setParallelism(2);

        // 使用MapMergeJsonData进行转换
        DataStream<JSONObject> transformedStream = cdcStream
                .map(jsonString -> JSON.parseObject(jsonString))  // 将String转换为JSONObject
                .map(new MapMergeJsonData());  // 应用转换逻辑

        // 打印转换后的结果
        transformedStream.print();

        env.execute("DbusSyncPgCdc2Kafka");

    }
}
