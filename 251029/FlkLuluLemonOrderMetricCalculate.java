package com.stream.realtime.lululemon;


import com.stream.core.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.LocalDate;
import java.time.ZoneId;

/**
 * @author shuaiqi.chen
 * @create 2025-10-29-20:11
 */
public class FlkLuluLemonOrderMetricCalculate {


    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.getConfig().getConfiguration().setString("table.local-time-zone", "Asia/Shanghai");
        tenv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "30 s");

        ZoneId sh = ZoneId.of("Asia/Shanghai");
        long today0Millis = LocalDate.now(ZoneId.of("Asia/Shanghai")).atStartOfDay(sh).toInstant().toEpochMilli();

        String ddl_kafka_oms_order_info = String.format(
                "create table if not exists kafka_oms_order_dtl (                                                                   \n" +
                "    id bigint,                                                                                                     \n" +
                "    order_id string,                                                                                               \n" +
                "    user_id string,                                                                                                \n" +
                "    user_name string,                                                                                              \n" +
                "    phone_number string,                                                                                           \n" +
                "    product_link string,                                                                                           \n" +
                "    product_id string,                                                                                             \n" +
                "    color string,                                                                                                  \n" +
                "    size string,                                                                                                   \n" +
                "    item_id string,                                                                                                \n" +
                "    material string,                                                                                               \n" +
                "    sale_num string,                                                                                               \n" +
                "    sale_amount string,                                                                                            \n" +
                "    total_amount string,                                                                                           \n" +
                "    product_name string,                                                                                           \n" +
                "    is_online_sales string,                                                                                        \n" +
                "    shipping_address string,                                                                                       \n" +
                "    recommendations_product_ids  string,                                                                           \n" +
                "    ds  string,                                                                                                    \n" +
                "    ts bigint,                                                                                                     \n" +
                "    ts_ms as case when ts < 100000000000 then to_timestamp_ltz(ts * 1000, 3) else to_timestamp_ltz(ts, 3) end,     \n" +
                "    insert_time string,                                                                                            \n" +
                "    table_name string,                                                                                             \n" +
                "    op string,                                                                                                     \n" +
                "    proc_time as proctime(),                                                                                       \n" +
                "    watermark for ts_ms as ts_ms - interval '5' second                                                             \n" +
                ")                                                                                                                  \n" +
                "with (                                                                                                             \n" +
                "    'connector' = 'kafka',                                                                                         \n" +
                "    'topic' = 'realtime_v3_order_info',                                                                            \n" +
                "    'properties.bootstrap.servers'= '172.24.219.66:9092',                                            \n" +
                "    'properties.group.id' = 'order-analysis1',                                                                     \n" +
                "    'scan.startup.mode' = 'timestamp',                                                                             \n" +
                "    'scan.startup.timestamp-millis' = '%d',                                                                        \n" +
                "    'format' = 'json',                                                                                             \n" +
                "    'json.fail-on-missing-field' = 'false',                                                                        \n" +
                "    'json.ignore-parse-errors' = 'true'                                                                            \n" +
                ");",today0Millis);

        String dorisSinkDDL = String.format(
                "CREATE TABLE IF NOT EXISTS report_lululemon_window_gmv_topN (                                              \n" +
                "    ds DATE,                                                                                               \n" +
                "    window_start STRING,                                                                                   \n" +
                "    window_end STRING,                                                                                     \n" +
                "    win_gmv DECIMAL(18,2),                                                                                 \n" +
                "    win_gmv_ids STRING,                                                                                    \n" +
                "    top5_product_ids STRING                                                                                \n" +
                ") WITH (                                                                                                   \n" +
                "    'connector' = 'doris',                                                                                 \n" +
                "    'fenodes' = '172.24.219.66:8030',                                                                       \n" +
                "    'table.identifier' = 'bigdata_realtime_lululemon_report_v1.report_lululemon_window_gmv_topN',                    \n" +
                "    'username' = 'root',                                                                                   \n" +
                "    'password' = '123456',                                                                              \n" +
                "    'sink.label-prefix' = 'lululemon_gmv_%d',                                                              \n" +
                "    'sink.enable-2pc' = 'true',                                                                            \n" +
                "    'sink.properties.format' = 'json',                                                                     \n" +
                "    'sink.properties.read_json_by_line' = 'true'                                                           \n" +
                ")", System.currentTimeMillis());


        tenv.executeSql(ddl_kafka_oms_order_info);
        tenv.executeSql(dorisSinkDDL);

        String compute0ToCurrentDaySaleAmountGMV =
                "with base_t as (                                                                                                   \n" +
                "    select                                                                                                         \n" +
                "        trim(replace(product_id, '\\n', '')) as clean_product_id,                                                  \n" +
                "        cast(total_amount as decimal(18,2)) as total_amount,                                                       \n" +
                "        ts_ms                                                                                                      \n" +
                "    from kafka_oms_order_dtl                                                                                       \n" +
//                "    where ts_ms >= floor(current_timestamp to day)                                                                 \n" +
                "    where CAST(ts_ms AS DATE) IN (\n" +
                        "  DATE '2025-10-24', \n" +
                        "  DATE '2025-10-25', \n" +
                        "  DATE '2025-10-26', \n" +
                        "  DATE '2025-10-28'\n" +
                        ")                                                               \n" +
                "),                                                                                                                 \n" +
                "win_product_agg as (                                                                                               \n" +
                "    select                                                                                                         \n" +
                "        window_start,                                                                                              \n" +
                "        window_end,                                                                                                \n" +
                "        clean_product_id,                                                                                          \n" +
                "        sum(total_amount) as product_gmv                                                                           \n" +
                "    from table(                                                                                                    \n" +
                "        cumulate(                                                                                                  \n" +
                "            table base_t,                                                                                          \n" +
                "            descriptor(ts_ms),                                                                                     \n" +
                "            interval '10' minutes,                                                                                 \n" +
                "            interval '1' day                                                                                       \n" +
                "        )                                                                                                          \n" +
                "    )                                                                                                              \n" +
                "    group by window_start, window_end, clean_product_id                                                            \n" +
                "),                                                                                                                 \n" +
                "top5_products as (                                                                                                 \n" +
                "    select window_start,                                                                                           \n" +
                "           window_end,                                                                                             \n" +
                "           clean_product_id,                                                                                       \n" +
                "           product_gmv                                                                                             \n" +
                "    from (                                                                                                         \n" +
                "        select *,                                                                                                  \n" +
                "               row_number() over (                                                                                 \n" +
                "                   partition by window_start, window_end                                                           \n" +
                "                   order by product_gmv desc                                                                       \n" +
                "               ) as rn                                                                                             \n" +
                "        from win_product_agg                                                                                       \n" +
                "    )                                                                                                              \n" +
                "    where rn <= 5                                                                                                  \n" +
                ")                                                                                                                  \n" +
                "select  CAST(DATE_FORMAT(window_start, 'yyyy-MM-dd') AS DATE) AS ds,                                               \n" +
                "        DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start,                                          \n" +
                "        DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end,                                              \n" +
                "        CAST(SUM(product_gmv) AS DECIMAL(18,2)) AS win_gmv,                                                        \n" +
                "        listagg(clean_product_id, ',') as win_gmv_ids,                                                             \n" +
                "        listagg(clean_product_id, ',') as top5_product_ids                                                         \n" +
                "from top5_products                                                                                                 \n" +
                "group by window_start, window_end;";



        tenv.executeSql(compute0ToCurrentDaySaleAmountGMV);
        tenv.executeSql("insert into report_lululemon_window_gmv_topN " + compute0ToCurrentDaySaleAmountGMV);
        System.out.println("插入成功----------------------");

    }
}