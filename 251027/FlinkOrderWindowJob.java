package com.stream.realtime.lululemon;

import com.stream.core.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author shuaiqi.chen
 * @create 2025-10-27-22:18
 */
public class FlinkOrderWindowJob {

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStateBackend(new HashMapStateBackend());
//        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        // ====== 1. Kafka 源表 DDL ======
        String source_kafka_order_info_ddl = "CREATE TABLE IF NOT EXISTS t_kafka_oms_order_info (\n" +
                "    id STRING,\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    user_name STRING,\n" +
                "    phone_number STRING,\n" +
                "    product_link STRING,\n" +
                "    product_id STRING,\n" +
                "    color STRING,\n" +
                "    size STRING,\n" +
                "    item_id STRING,\n" +
                "    material STRING,\n" +
                "    sale_num STRING,\n" +
                "    sale_amount STRING,\n" +
                "    total_amount STRING,\n" +
                "    product_name STRING,\n" +
                "    is_online_sales STRING,\n" +
                "    shipping_address STRING,\n" +
                "    recommendations_product_ids STRING,\n" +
                "    ds STRING,\n" +
                "    ts STRING,\n" +
                "    ts_ms AS TO_TIMESTAMP_LTZ(\n" +
                "       CASE WHEN CAST(ts AS BIGINT) < 100000000000 THEN CAST(ts AS BIGINT) * 1000 ELSE CAST(ts AS BIGINT) END, 3\n" +
                "    ),\n" +
                "    insert_time STRING,\n" +
                "    table_name STRING,\n" +
                "    op STRING,\n" +
                "    WATERMARK FOR ts_ms AS ts_ms - INTERVAL '1' DAY\n" +   // ✅ 放宽到 1 天
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'realtime_v3_order_info',\n" +
                "    'properties.bootstrap.servers' = '172.24.219.66:9092',\n" +
                "    'properties.group.id' = 'order-analysis-final',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'false'\n" +
                ");";

        tenv.executeSql(source_kafka_order_info_ddl);

        // ====== 2. 定义 Print Sink ======
        String sinkDDL = "CREATE TABLE print_sink (\n" +
                "  window_start TIMESTAMP(3),\n" +
                "  window_end TIMESTAMP(3),\n" +
                "  total_sum DECIMAL(10,2)\n" +
                ") WITH ('connector' = 'print');";
        tenv.executeSql(sinkDDL);

        // ====== 3. 定义窗口聚合逻辑（持续流式任务） ======
        String hopQuery = "INSERT INTO print_sink\n" +
                "SELECT \n" +
                "    window_start, \n" +
                "    window_end, \n" +
                "    SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) AS total_sum\n" +
                "FROM TABLE(\n" +
                "    HOP(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '10' MINUTES)\n" +
                ")\n" +
                "GROUP BY window_start, window_end;";

        // ✅ 启动持续执行的流式查询
        tenv.executeSql(hopQuery);
    }
}
