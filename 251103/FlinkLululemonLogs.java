package com.stream.realtime.lululemon;



import com.stream.realtime.lululemon.func.UserBehavior;
import com.stream.realtime.lululemon.func.UserPathAggregator;
import com.stream.realtime.lululemon.func.UserProfileAggregator;
import com.stream.realtime.lululemon.model.UserPath;
import com.stream.realtime.lululemon.model.UserProfile;
import com.stream.realtime.lululemon.utils.PathAnalysisUtils;
import com.stream.realtime.lululemon.utils.TimeSlotUtils;
import org.apache.flink.configuration.Configuration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.core.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * @author shuaiqi.chen
 * @create 2025-11-01-8:54
 */
public class FlinkLululemonLogs {

    public static class PageLog {
        public String log_type;
        public String formatted_time;

        // Jackson 需要默认构造函数
        public PageLog() {}
    }

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ==================== 添加内存配置 ====================
        // 设置较低的并行度
        env.setParallelism(1);  // 先设置为1测试

        // 禁用链式操作，减少缓冲区需求
        env.disableOperatorChaining();

        // 设置缓冲区超时时间
        env.setBufferTimeout(100);  // 100ms

        // 使用配置对象设置内存参数
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();

        // 设置网络缓冲区数量（直接设置数量而不是大小）
        config.setInteger("taskmanager.network.memory.buffers-per-channel", 2);
        config.setInteger("taskmanager.network.memory.floating-buffers-per-gate", 8);

        // 设置内存大小
        config.setString("taskmanager.memory.network.min", "256mb");
        config.setString("taskmanager.memory.network.max", "512mb");
        config.setString("taskmanager.memory.managed.size", "512mb");

        env.configure(config);

        EnvironmentSettingUtils.defaultParameter(env);



        EnvironmentSettingUtils.defaultParameter(env);

        String bootstrapServers = "172.24.219.66:9092"; // 替换为实际的 Kafka 地址
        String topic = "realtime_v3_logs_data"; // 替换为要消费的 topic
        String groupId = "flink-kafka-logs-group"; // 消费组 ID

        // 创建 Kafka Source
        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(
                bootstrapServers,
                topic,
                groupId,
                OffsetsInitializer.earliest() // 从最早开始消费
        );

        // 从 Kafka 读取数据
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        ObjectMapper mapper = new ObjectMapper();


        // todo ==================== 历史天 + 当天 每个页面的总体访问量 ====================

        SingleOutputStreamOperator<String> pvStream = kafkaStream
                .map(json -> {
                    JsonNode node = mapper.readTree(json);
                    String logType = node.get("log_type").asText();
                    String date = node.get("formatted_time").asText().split(" ")[0]; // yyyy-MM-dd
                    return new Tuple2<>(date + "-" + logType, 1L);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                // 转成可读字符串输出
                .map(tuple -> {
                    int lastDash = tuple.f0.lastIndexOf("-");
                    String date = tuple.f0.substring(0, lastDash);
                    String page = tuple.f0.substring(lastDash + 1);
                    Long pv = tuple.f1;
                    return "日期: " + date + ", 页面: " + page + ", PV: " + pv;
                });

//        pvStream.print();


        // todo ==================== 历史天 + 当天 共计搜索词TOP10 ====================
        DataStream<Tuple2<String, Long>> keywordStream = kafkaStream
                .flatMap((String json, Collector<Tuple2<String, Long>> out) -> {
                    JsonNode node = mapper.readTree(json);
                    if (node.has("keywords")) {
                        String keywords = node.get("keywords").asText();
                        String[] split = keywords.split("[,，]"); // 支持中英文逗号
                        for (String k : split) {
                            k = k.trim();
                            if (!k.isEmpty()) {
                                out.collect(new Tuple2<>(k, 1L));
                            }
                        }
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

        //  累加每个搜索词的总次数
        DataStream<Tuple2<String, Long>> countStream = keywordStream
                .keyBy(t -> t.f0)
                .sum(1);


        SingleOutputStreamOperator<String> keyWorksTop10 = countStream
                .keyBy(t -> 0) // 全局排序
                .process(new KeyedProcessFunction<Integer, Tuple2<String, Long>, String>() {

                    private final Map<String, Long> counts = new HashMap<>();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) {
                        counts.put(value.f0, value.f1);

                        // 取前 10
                        List<Map.Entry<String, Long>> top10 = counts.entrySet()
                                .stream()
                                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                                .limit(10)
                                .collect(Collectors.toList());

                        StringBuilder sb = new StringBuilder();
                        sb.append("当前TOP10搜索词:\n");
                        for (Map.Entry<String, Long> e : top10) {
                            sb.append("搜索词: ").append(e.getKey()).append(", 次数: ").append(e.getValue()).append("\n");
                        }

                        out.collect(sb.toString());
                    }
                });
//        keyWorksTop10.print();


        // todo ==================== 史天 + 当天 登陆区域的全国热力情况 ====================

        // 过滤 login 日志并提取 region
        DataStream<Tuple2<String, Long>> regionStream = kafkaStream
                .flatMap((String json, Collector<Tuple2<String, Long>> out) -> {
                    JsonNode node = mapper.readTree(json);
                    if (node.has("log_type") && "login".equals(node.get("log_type").asText())
                            && node.has("region")) {
                        String region = node.get("region").asText().trim();
                        if (!region.isEmpty()) {
                            out.collect(new Tuple2<>(region, 1L));
                        }
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

        // 按地区累加
        DataStream<Tuple2<String, Long>> regionCountStream = regionStream
                .keyBy(t -> t.f0)
                .sum(1);

        // 输出全国热力情况（每条数据更新一次）
        SingleOutputStreamOperator<String> process = regionCountStream
                .keyBy(t -> 0) // 全局排序/输出
                .process(new KeyedProcessFunction<Integer, Tuple2<String, Long>, String>() {

                    private final Map<String, Long> counts = new HashMap<>();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) {
                        counts.put(value.f0, value.f1);

                        // 构建输出，可直接用于热力图
                        StringBuilder sb = new StringBuilder();
                        sb.append("全国登录热力统计:\n");
                        for (Map.Entry<String, Long> e : counts.entrySet()) {
                            sb.append("地区: ").append(e.getKey())
                                    .append(", 访问量: ").append(e.getValue()).append("\n");
                        }

                        out.collect(sb.toString());
                    }
                });

//        process.print();

        // todo ==================== 历史天 + 当天 用户设备统计（iOS & Android） ====================

        // 提取设备平台信息
        DataStream<Tuple3<String, String, Long>> deviceStream = kafkaStream
                .flatMap((String json, Collector<Tuple3<String, String, Long>> out) -> {
                    try {
                        JsonNode node = mapper.readTree(json);

                        // 获取日期
                        String date = "";
                        if (node.has("formatted_time")) {
                            String formattedTime = node.get("formatted_time").asText();
                            if (!formattedTime.isEmpty()) {
                                date = formattedTime.split(" ")[0]; // yyyy-MM-dd
                            }
                        }

                        // 获取平台信息
                        String platform = "unknown";
                        if (node.has("plat")) {
                            String plat = node.get("plat").asText().toLowerCase();
                            if (plat.contains("iphone") || plat.contains("ios")) {
                                platform = "iOS";
                            } else if (plat.contains("android")) {
                                platform = "Android";
                            }
                        }

                        // 获取品牌信息
                        String brand = "unknown";
                        if (node.has("brand")) {
                            brand = parseBrand(node.get("brand").asText());
                        }

                        if (!date.isEmpty() && !platform.equals("unknown")) {
                            // 按日期+平台统计
                            out.collect(new Tuple3<>(date, platform, 1L));
                            // 按日期+品牌统计
                            out.collect(new Tuple3<>(date, brand, 1L));
                        }

                    } catch (Exception e) {
                        // 忽略解析错误
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {}));

        // 按日期+平台统计设备数量
        SingleOutputStreamOperator<String> devicePlatformStats = deviceStream
                .filter(tuple -> {
                    String category = tuple.f1;
                    // 只统计iOS和Android平台
                    return "iOS".equals(category) || "Android".equals(category) ||
                            // 或者主要品牌
                            "Apple".equals(category) || "Huawei".equals(category) ||
                            "Xiaomi".equals(category) || "OPPO".equals(category) ||
                            "Vivo".equals(category) || "Samsung".equals(category);
                })
                .keyBy(tuple -> tuple.f0 + "-" + tuple.f1) // 按日期+平台/品牌分组
                .sum(2)
                .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        return String.format("设备统计 - 日期: %s, 类型: %s, 数量: %d",
                                value.f0, value.f1, value.f2);
                    }
                });

        // 输出设备统计结果
//        devicePlatformStats.print("device-stats");

        // 实时设备分布TOP统计
        SingleOutputStreamOperator<String> deviceTopStats = deviceStream
                .keyBy(tuple -> tuple.f1) // 按平台/品牌分组
                .sum(2)
                .keyBy(tuple -> 0) // 全局排序
                .process(new KeyedProcessFunction<Integer, Tuple3<String, String, Long>, String>() {

                    private final Map<String, Long> deviceCounts = new HashMap<>();

                    @Override
                    public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) {
                        deviceCounts.put(value.f1, value.f2);

                        // 计算平台分布
                        long iosCount = deviceCounts.getOrDefault("iOS", 0L);
                        long androidCount = deviceCounts.getOrDefault("Android", 0L);
                        long total = iosCount + androidCount;

                        double iosPercentage = total > 0 ? (iosCount * 100.0 / total) : 0;
                        double androidPercentage = total > 0 ? (androidCount * 100.0 / total) : 0;

                        // 品牌TOP10
                        List<Map.Entry<String, Long>> brandTop10 = deviceCounts.entrySet()
                                .stream()
                                .filter(entry -> !"iOS".equals(entry.getKey()) && !"Android".equals(entry.getKey()))
                                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                                .limit(10)
                                .collect(Collectors.toList());

                        StringBuilder sb = new StringBuilder();
                        sb.append("=== 实时设备统计 ===\n");
                        sb.append(String.format("平台分布 - iOS: %d (%.2f%%), Android: %d (%.2f%%)\n",
                                iosCount, iosPercentage, androidCount, androidPercentage));
                        sb.append("品牌TOP10:\n");
                        for (Map.Entry<String, Long> entry : brandTop10) {
                            sb.append(String.format("  %s: %d\n", entry.getKey(), entry.getValue()));
                        }

                        out.collect(sb.toString());
                    }
                });

        // 输出实时设备统计
//        deviceTopStats.print("device-top-stats");


        // todo ==================== 用户画像分析 ====================

        // 提取用户行为数据
        DataStream<Tuple2<String, UserBehavior>> userBehaviorStream = kafkaStream
                .flatMap((String json, Collector<Tuple2<String, UserBehavior>> out) -> {
                    try {
                        JsonNode node = mapper.readTree(json);

                        if (node.has("user_id") && node.has("log_type") && node.has("formatted_time")) {
                            String userId = node.get("user_id").asText();
                            String logType = node.get("log_type").asText();
                            String formattedTime = node.get("formatted_time").asText();

                            // 提取日期和时间段
                            String date = TimeSlotUtils.extractDate(formattedTime);
                            String timeSlot = TimeSlotUtils.parseTimeSlot(formattedTime);

                            UserBehavior behavior = new UserBehavior(userId, logType, date, timeSlot);
                            out.collect(new Tuple2<>(userId, behavior));
                        }
                    } catch (Exception e) {
                        // 忽略解析错误
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, UserBehavior>>() {}));

        // 按用户ID聚合，生成用户画像
        SingleOutputStreamOperator<UserProfile> userProfileStream = userBehaviorStream
                .keyBy(tuple -> tuple.f0)  // 按用户ID分组
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))  // 5分钟窗口更新一次
                .aggregate(new UserProfileAggregator());

        // 输出用户画像详情
        SingleOutputStreamOperator<String> userProfileOutput = userProfileStream
                .map(new MapFunction<UserProfile, String>() {
                    @Override
                    public String map(UserProfile profile) throws Exception {
                        return "用户画像详情: " + profile.toString();
                    }
                });

//        userProfileOutput.print("user-profile");

        // 实时用户行为统计摘要
        SingleOutputStreamOperator<String> userBehaviorStats = userBehaviorStream
                .keyBy(tuple -> 0)  // 全局统计
                .process(new KeyedProcessFunction<Integer, Tuple2<String, UserBehavior>, String>() {

                    private final Map<String, UserProfile> userProfiles = new HashMap<>();
                    private long lastOutputTime = 0;

                    @Override
                    public void processElement(Tuple2<String, UserBehavior> value, Context ctx, Collector<String> out) throws Exception {
                        String userId = value.f0;
                        UserBehavior behavior = value.f1;

                        // 更新用户画像
                        UserProfile profile = userProfiles.getOrDefault(userId, new UserProfile(userId));
                        if (behavior.getLoginDate() != null) {
                            profile.addLoginDate(behavior.getLoginDate());
                        }
                        if (behavior.getLoginTimeSlot() != null) {
                            profile.addLoginTimeSlot(behavior.getLoginTimeSlot());
                        }
                        if (behavior.getLogType() != null) {
                            profile.updateBehavior(behavior.getLogType());
                        }
                        userProfiles.put(userId, profile);

                        // 每30秒输出一次统计摘要
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastOutputTime > 30000) {
                            outputUserStats(out);
                            lastOutputTime = currentTime;
                        }
                    }

                    private void outputUserStats(Collector<String> out) {
                        int totalUsers = userProfiles.size();
                        if (totalUsers == 0) return;

                        long usersWithPurchase = userProfiles.values().stream().filter(UserProfile::isHasPurchase).count();
                        long usersWithSearch = userProfiles.values().stream().filter(UserProfile::isHasSearch).count();
                        long usersWithBrowse = userProfiles.values().stream().filter(UserProfile::isHasBrowse).count();

                        // 计算平均登录天数
                        double avgLoginDays = userProfiles.values().stream()
                                .mapToInt(p -> p.getLoginDates().size())
                                .average()
                                .orElse(0.0);

                        // 统计登录时间段分布
                        Map<String, Long> timeSlotStats = userProfiles.values().stream()
                                .flatMap(p -> p.getLoginTimeSlots().stream())
                                .collect(Collectors.groupingBy(slot -> slot, Collectors.counting()));

                        StringBuilder sb = new StringBuilder();
                        sb.append("\n=== 用户行为统计摘要 ===\n");
                        sb.append(String.format("总用户数: %d\n", totalUsers));
                        sb.append(String.format("有购买行为用户: %d (%.1f%%)\n", usersWithPurchase, (usersWithPurchase * 100.0 / totalUsers)));
                        sb.append(String.format("有搜索行为用户: %d (%.1f%%)\n", usersWithSearch, (usersWithSearch * 100.0 / totalUsers)));
                        sb.append(String.format("有浏览行为用户: %d (%.1f%%)\n", usersWithBrowse, (usersWithBrowse * 100.0 / totalUsers)));
                        sb.append(String.format("平均登录天数: %.1f天\n", avgLoginDays));

                        sb.append("登录时间段分布:\n");
                        timeSlotStats.entrySet().stream()
                                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                                .forEach(entry -> {
                                    sb.append(String.format("  %s: %d用户\n", entry.getKey(), entry.getValue()));
                                });

                        sb.append("======================\n");

                        out.collect(sb.toString());
                    }
                });

//        userBehaviorStats.print("user-stats");


        // todo ==================== 用户路径分析 ====================

        // 提取用户路径数据
        DataStream<Tuple2<String, UserPath>> userPathStream = kafkaStream
                .flatMap((String json, Collector<Tuple2<String, UserPath>> out) -> {
                    try {
                        JsonNode node = mapper.readTree(json);

                        if (node.has("user_id") && node.has("log_type") && node.has("ts")) {
                            String userId = node.get("user_id").asText();
                            String logType = node.get("log_type").asText();
                            String timestamp = node.get("ts").asText();

                            // 只分析关键页面
                            if (PathAnalysisUtils.isKeyPage(logType)) {
                                // 提取日期
                                String date = PathAnalysisUtils.extractDateFromTimestamp(timestamp);

                                // 转换时间戳
                                long timestampMs;
                                double timestampDouble = Double.parseDouble(timestamp);
                                if (timestampDouble >= 1e12) {
                                    timestampMs = (long) timestampDouble;
                                } else {
                                    timestampMs = (long) (timestampDouble * 1000);
                                }

                                // 创建路径对象
                                UserPath userPath = new UserPath(userId, date);
                                userPath.addPage(logType, timestampMs);

                                out.collect(new Tuple2<>(userId + "-" + date, userPath));
                            }
                        }
                    } catch (Exception e) {
                        // 忽略解析错误
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, UserPath>>() {}));

        // 按用户+日期聚合路径
        SingleOutputStreamOperator<UserPath> aggregatedPathStream = userPathStream
                .keyBy(tuple -> tuple.f0)  // 按用户ID+日期分组
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))  // 2分钟窗口聚合一次路径
                .aggregate(new UserPathAggregator());

        // 输出用户路径详情
        SingleOutputStreamOperator<String> pathDetailOutput = aggregatedPathStream
                .filter(path -> path.getPathLength() >= 2)  // 只输出长度>=2的路径
                .map(new MapFunction<UserPath, String>() {
                    @Override
                    public String map(UserPath path) throws Exception {
                        return path.toString();
                    }
                });

        pathDetailOutput.print("user-path-detail");

        // 路径模式分析 - 统计常见路径
        SingleOutputStreamOperator<String> pathPatternStats = aggregatedPathStream
                .keyBy(path -> 0)  // 全局统计
                .process(new KeyedProcessFunction<Integer, UserPath, String>() {

                    private final Map<String, Long> pathPatternCounts = new HashMap<>();
                    private final Map<String, Long> conversionStats = new HashMap<>();
                    private long lastOutputTime = 0;

                    @Override
                    public void processElement(UserPath path, Context ctx, Collector<String> out) throws Exception {
                        if (path.getPathSequence().size() >= 2) {
                            // 统计路径模式
                            String pathPattern = String.join("->", path.getPathSequence());
                            pathPatternCounts.put(pathPattern, pathPatternCounts.getOrDefault(pathPattern, 0L) + 1);

                            // 统计转化路径
                            if (path.isHasPurchase()) {
                                String conversionKey = "总转化路径";
                                conversionStats.put(conversionKey, conversionStats.getOrDefault(conversionKey, 0L) + 1);

                                // 统计以支付结束的路径
                                if (path.getPathSequence().get(path.getPathLength() - 1).equals("payment")) {
                                    String paymentPathKey = "支付结束路径";
                                    conversionStats.put(paymentPathKey, conversionStats.getOrDefault(paymentPathKey, 0L) + 1);
                                }
                            }

                            // 每30秒输出一次路径统计
                            long currentTime = System.currentTimeMillis();
                            if (currentTime - lastOutputTime > 30000) {
                                outputPathStats(out);
                                lastOutputTime = currentTime;
                            }
                        }
                    }

                    private void outputPathStats(Collector<String> out) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("\n=== 路径分析统计 ===\n");

                        // 输出最常见路径TOP5
                        sb.append("最常见路径TOP5:\n");
                        pathPatternCounts.entrySet().stream()
                                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                                .limit(5)
                                .forEach(entry -> {
                                    sb.append(String.format("  %s: %d次\n", entry.getKey(), entry.getValue()));
                                });

                        // 输出转化统计
                        sb.append("转化统计:\n");
                        conversionStats.forEach((key, value) -> {
                            sb.append(String.format("  %s: %d\n", key, value));
                        });

                        // 计算平均路径长度
                        if (!pathPatternCounts.isEmpty()) {
                            double avgPathLength = pathPatternCounts.keySet().stream()
                                    .mapToInt(path -> path.split("->").length)
                                    .average()
                                    .orElse(0.0);
                            sb.append(String.format("平均路径长度: %.1f\n", avgPathLength));
                        }

                        sb.append("==================\n");
                        out.collect(sb.toString());
                    }
                });

        pathPatternStats.print("path-pattern-stats");

        // 实时路径流监控
        SingleOutputStreamOperator<String> realtimePathMonitor = userPathStream
                .keyBy(tuple -> 0)
                .process(new KeyedProcessFunction<Integer, Tuple2<String, UserPath>, String>() {

                    private long pathCount = 0;
                    private long lastOutputTime = 0;

                    @Override
                    public void processElement(Tuple2<String, UserPath> value, Context ctx, Collector<String> out) throws Exception {
                        pathCount++;

                        UserPath path = value.f1;
                        String userId = path.getUserId();
                        String page = path.getPathSequence().isEmpty() ? "未知" :
                                path.getPathSequence().get(path.getPathSequence().size() - 1);

                        // 每10秒输出一次实时监控
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastOutputTime > 10000) {
                            out.collect(String.format("实时路径监控 - 总路径数: %d, 最新: 用户%s访问%s页面",
                                    pathCount, userId, page));
                            lastOutputTime = currentTime;
                        }

                        // 特别关注转化路径
                        if (path.isHasPurchase()) {
                            out.collect(String.format("🔥 转化路径提醒 - 用户%s完成购买，路径: %s",
                                    userId, String.join("->", path.getPathSequence())));
                        }
                    }
                });

        realtimePathMonitor.print("realtime-path-monitor");



        env.execute("FlinkLululemonLogs");
    }

    /**
     * 解析设备品牌
     */
    private static String parseBrand(String brand) {
        if (brand == null) {
            return "unknown";
        }

        String brandLower = brand.toLowerCase();
        if (brandLower.contains("iphone")) {
            return "Apple";
        } else if (brandLower.contains("huawei")) {
            return "Huawei";
        } else if (brandLower.contains("honor")) {
            return "Honor";
        } else if (brandLower.contains("xiaomi") || brandLower.contains("mi")) {
            return "Xiaomi";
        } else if (brandLower.contains("oppo")) {
            return "OPPO";
        } else if (brandLower.contains("vivo")) {
            return "Vivo";
        } else if (brandLower.contains("samsung")) {
            return "Samsung";
        } else if (brandLower.contains("realme")) {
            return "Realme";
        } else {
            return "Other";
        }
    }
}