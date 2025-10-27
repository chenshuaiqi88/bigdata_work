package com.stream.realtime.lululemon;

import com.stream.core.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

import java.time.ZoneId;

/**
 * @author shuaiqi.chen
 * @create 2025-10-27-19:00
 */
public class FlinkSqlTest {
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 在调用 defaultParameter 之前或之后显式设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        EnvironmentSettingUtils.defaultParameter(env);

        // 再次确认设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        String source_kafka_order_info_ddl = "create table if not exists t_kafka_oms_order_info (\n" +
                "    id string,\n" +
                "    order_id string,\n" +
                "    user_id string,\n" +
                "    user_name string,\n" +
                "    phone_number string,\n" +
                "    product_link string,\n" +
                "    product_id string,\n" +
                "    color string,\n" +
                "    size string,\n" +
                "    item_id string,\n" +
                "    material string,\n" +
                "    sale_num string,\n" +
                "    sale_amount string,\n" +
                "    total_amount string,\n" +
                "    product_name string,\n" +
                "    is_online_sales string,\n" +
                "    shipping_address string,\n" +
                "    recommendations_product_ids string,\n" +
                "    ds string,\n" +
                "    ts string,\n" +   // <- string to be safe
                "    ts_ms AS TO_TIMESTAMP_LTZ(\n" +
                "       CASE WHEN CAST(ts AS BIGINT) < 100000000000 THEN CAST(ts AS BIGINT) * 1000 ELSE CAST(ts AS BIGINT) END, 3\n" +
                "    ),\n" +
                "    insert_time string,\n" +
                "    table_name string,\n" +
                "    op string,\n" +
                "    WATERMARK FOR ts_ms AS ts_ms - INTERVAL '1' DAY\n" +
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'realtime_v3_order_info',\n" +
                "    'properties.bootstrap.servers' = '172.24.219.66:9092',\n" +
                "    'properties.group.id' = 'order-analysis1',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'false'\n" +
                ")";

        tenv.executeSql(source_kafka_order_info_ddl);
//        tenv.executeSql("select ts_ms, CURRENT_WATERMARK(ts_ms) as water from t_kafka_oms_order_info;").print();

        tenv.executeSql("SELECT \n" +
                "    window_start, \n" +
                "    window_end, \n" +
                "    SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) as total_sum\n" +
                "  FROM TABLE(\n" +
                "    HOP(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '10' MINUTES)\n" +
                ")\n" +
                "WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-27'\n" +
                "GROUP BY window_start, window_end").print();


//
//        String sinkDDL = "CREATE TABLE print_sink (\n" +
//                "  window_start TIMESTAMP(3),\n" +
//                "  window_end TIMESTAMP(3),\n" +
//                "  total_sum DECIMAL(10,2)\n" +
//                ") WITH ('connector' = 'print')";
//
//        tenv.executeSql(sinkDDL);
//
//        String hopQuery = "INSERT INTO print_sink\n" +
//                "SELECT window_start, window_end,\n" +
//                "       SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) AS total_sum\n" +
//                "FROM TABLE(\n" +
//                "    HOP(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '10' MINUTES)\n" +
//                ")\n" +
//                "GROUP BY window_start, window_end";
//
//        tenv.executeSql(hopQuery);


//        env.execute("order_window_aggregation");

    }
}