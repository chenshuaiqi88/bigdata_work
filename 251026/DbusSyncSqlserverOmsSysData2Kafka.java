package com.stream.realtime.lululemon;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author shuaiqi.chen
 * @create 2025-10-25-10:44
 */
public class DbusSyncSqlserverOmsSysData2Kafka {

    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC="realtime_v3_order_info";

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("172.24.219.66")
                .port(1433)
                .username("sa")
                .password("Csq0573,./")
                .database("realtime_v3")
                .tableList("dbo.oms_order_dtl")
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");
//        dataStreamSource.print().setParallelism(1);

        SingleOutputStreamOperator<JSONObject> convertStr2JsonDS = dataStreamSource.map(JSON::parseObject)
                .uid("convertStr2JsonDS")
                .name("convertStr2JsonDS");

//        convertStr2JsonDS.print("convertStr2JsonDS -> ");

        convertStr2JsonDS.map(new MapMergeJsonData()).print();


        env.execute("DbusSyncSqlserverOmsSysData2Kafka");
    }


}
