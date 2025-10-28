package com.stream.realtime.lululemon;

import com.stream.core.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author shuaiqi.chen
 * @create 2025-10-27-19:00
 */
public class FlinkSqlTest {

    public static String source_kafka_order_info_ddl(){
        return "create table if not exists t_kafka_oms_order_info (\n" +
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
                "    ts bigint,\n" +
                "    ts_ms as case when ts < 100000000000 then to_timestamp_ltz(ts * 1000, 3) else to_timestamp_ltz(ts, 3) end,\n" +
                "    insert_time string,\n" +

                "    op string,\n" +
                "    watermark for ts_ms as ts_ms - interval '5' second\n" +
                ")\n" +
                "with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'realtime_v3_order_info',\n" +
                "    'properties.bootstrap.servers'= '172.24.219.66:9092',\n" +
                "    'properties.group.id' = 'order-analysis1',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";
    }

    public static String cumulativeSalesSql (String stepInterval,String maxInterval,String targetDate){
        return "SELECT                                                                                     \n" +
                "    window_start,                                                                  \n" +
                "    window_end,                                                                    \n" +
                "    SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) as total_cumulative_amount        \n" +
                "FROM TABLE(                                                                        \n" +
                "    CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms),                      \n" +
                "    INTERVAL '" + stepInterval + "' Minutes,                                       \n" +
                "    INTERVAL '" + maxInterval + "' day)                                            \n" +
                ")                                                                                  \n" +
                "WHERE CAST(ts_ms AS DATE) = DATE '" + targetDate + "'                              \n" +
                "  AND total_amount IS NOT NULL                                                     \n" +
                "  AND total_amount <> ''                                                           \n" +
                "GROUP BY window_start, window_end";
    }

    public static String top5CumulativeSalesSql (String stepInterval,String maxInterval,String targetDate){
        return "WITH RankedData AS (                                                                   \n" +
                " SELECT                                                                                                        \n" +
                "  window_start,                                                                                                        \n" +
                "  window_end,                                                                                                  \n" +
                "  TRY_CAST(total_amount AS DECIMAL(10,2)) as total_amount,                                                     \n" +
                "  id,                                                                                                          \n" +
                "  TRY_CAST(sale_num AS INT) as sale_num,                                                                       \n" +
                "  ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY TRY_CAST(sale_num AS INT) DESC) as rn      \n" +
                " FROM TABLE(                                                                                                   \n" +
                "  CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms),                                                    \n" +
                "  INTERVAL '" + stepInterval + "' MINUTES,                                                                     \n" +
                "  INTERVAL '" + maxInterval + "' DAY)                                                                          \n" +
                " )                                                                                                             \n" +
                " WHERE CAST(ts_ms AS DATE) = DATE '"+targetDate+"'                                                             \n" +
                "  AND total_amount IS NOT NULL                                                                                 \n" +
                "  AND total_amount <> ''                                                                                       \n" +
                "), Top5Data AS (                                                                                               \n" +
                " SELECT                                                                                                        \n" +
                "  window_start,                                                                                                \n" +
                "  window_end,                                                                                                  \n" +
                "  total_amount,                                                                                                \n" +
                "  id,                                                                                                          \n" +
                "  rn                                                                                                           \n" +
                " FROM RankedData                                                                                               \n" +
                " WHERE rn <= 5                                                                                                 \n" +
                " ORDER BY window_start, window_end, rn                                                                             \n" +
                ")                                                                                                              \n" +
                "SELECT                                                                                                         \n" +
                " window_start,                                                                                                 \n" +
                " window_end,                                                                                                   \n" +
                " SUM(total_amount) as total_cumulative_amount,                                                                 \n" +
                " '[' || LISTAGG(id, ',') || ']' as top5_ids                                                                    \n" +
                "FROM Top5Data                                                                                                  \n" +
                "GROUP BY window_start, window_end                                                                              \n";
    }

    public static String top5PerWindowSql (String stepInterval,String targetDate){
        return "SELECT                                                                  \n" +
                "    window_start,                                                                          \n" +
                "    window_end,                                                                            \n" +
                "    CONCAT(                                                                                \n" +
                "        MAX(CASE WHEN rank_num = 1 THEN id ELSE '' END), ',',                              \n" +
                "        MAX(CASE WHEN rank_num = 2 THEN id ELSE '' END), ',',                              \n" +
                "        MAX(CASE WHEN rank_num = 3 THEN id ELSE '' END), ',',                              \n" +
                "        MAX(CASE WHEN rank_num = 4 THEN id ELSE '' END), ',',                              \n" +
                "        MAX(CASE WHEN rank_num = 5 THEN id ELSE '' END)                                    \n" +
                "    ) as top5_ids                                                                          \n" +
                "FROM (                                                                                     \n" +
                "    SELECT                                                                                 \n" +
                "        window_start,                                                                      \n" +
                "        window_end,                                                                        \n" +
                "        id,                                                                                \n" +
                "        SUM(TRY_CAST(sale_num AS INT)) as total_sale_num,                                  \n" +
                "        ROW_NUMBER() OVER (                                                                \n" +
                "            PARTITION BY window_start, window_end                                          \n" +
                "            ORDER BY SUM(TRY_CAST(sale_num AS INT)) DESC                                   \n" +
                "        ) as rank_num                                                                      \n" +
                "    FROM TABLE(                                                                            \n" +
                "        TUMBLE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms)," +
                "        INTERVAL '"+stepInterval+"' Minutes)" +
                "    )                                                                                      \n" +
                "    WHERE CAST(ts_ms AS DATE) = DATE '"+targetDate+"'                                      \n" +
                "      AND sale_num IS NOT NULL                                                             \n" +
                "      AND sale_num <> ''                                                                   \n" +
                "      AND id IS NOT NULL                                                                   \n" +
                "      AND id <> ''                                                                         \n" +
                "    GROUP BY window_start, window_end, id                                                  \n" +
                ")                                                                                          \n" +
                "WHERE rank_num <= 5                                                                        \n" +
                "GROUP BY window_start, window_end";
    }

    public static String cumulativeSalesWithProcessSql (String stepInterval,String maxInterval,String targetDate){
        return "SELECT                                                                                         \n" +
                "    window_start,                                                                      \n" +
                "    window_end,                                                                        \n" +
                "    SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) as period_amount,                     \n" +
                "    LISTAGG(                                                                           \n" +
                "        id || '(' || CAST(TRY_CAST(total_amount AS DECIMAL(10,2)) AS VARCHAR) || ')',  \n" +
                "        '+'                                                                            \n" +
                "    ) as cumulative_process                                                            \n" +
                "FROM TABLE(                                                                            \n" +
                "    CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms),                          \n" +
                "        INTERVAL '" + stepInterval + "' MINUTES,                                       \n" +
                "        INTERVAL '" + maxInterval + "' DAY)                                            \n" +
                ")                                                                                      \n" +
                "WHERE CAST(ts_ms AS DATE) = DATE '"+targetDate+"'                                      \n" +
                "  AND total_amount IS NOT NULL                                                         \n" +
                "  AND total_amount <> ''                                                               \n" +
                "GROUP BY window_start, window_end";
    }



    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tenv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "30s");


        tenv.executeSql(source_kafka_order_info_ddl());

        // 定义变量
        String stepInterval = "10"; // 分钟
        String maxInterval = "1";   // 天
        String targetDate = "2025-10-27";

        // TODO: 2025/10/28 基于累计窗口的销售额统计
        tenv.executeSql(cumulativeSalesSql(stepInterval,maxInterval,targetDate)).print();

        // TODO: 2025/10/28 在累计窗口内统计Top5商品ID和累计销售额
        tenv.executeSql(top5CumulativeSalesSql(stepInterval,maxInterval,targetDate)).print();

        // TODO: 2025/10/28 按时间窗口统计销售额Top5商品ID
        tenv.executeSql(top5PerWindowSql(stepInterval,targetDate)).print();

        // TODO: 2025/10/28 每个时间窗口内所有订单ID及其对应金额的累加
        tenv.executeSql(cumulativeSalesWithProcessSql(stepInterval,maxInterval,targetDate)).print();

        //  执行流作业
        env.execute("order_window_aggregation");

    }

}