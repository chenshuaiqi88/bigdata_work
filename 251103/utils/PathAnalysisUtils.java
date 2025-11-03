package com.stream.realtime.lululemon.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

/**
 * @author shuaiqi.chen
 * @create 2025-11-03-10:35
 */
public class PathAnalysisUtils {

    /**
     * 从时间戳提取日期
     */
    public static String extractDateFromTimestamp(String timestampStr) {
        try {
            double timestampDouble = Double.parseDouble(timestampStr);
            long timestampMs;

            if (timestampDouble >= 1e12) {
                timestampMs = (long) timestampDouble;
            } else {
                timestampMs = (long) (timestampDouble * 1000);
            }

            LocalDate date = Instant.ofEpochMilli(timestampMs)
                    .atZone(ZoneId.of("UTC"))
                    .toLocalDate();

            return date.toString(); // yyyy-MM-dd
        } catch (Exception e) {
            return "unknown";
        }
    }

    /**
     * 判断是否为关键页面（用于路径分析）
     */
    public static boolean isKeyPage(String logType) {
        return logType != null && (
                "home".equals(logType) ||
                        "product_list".equals(logType) ||
                        "product_detail".equals(logType) ||
                        "search".equals(logType) ||
                        "payment".equals(logType) ||
                        "login".equals(logType) ||
                        "cart".equals(logType)
        );
    }

    /**
     * 计算路径转化率相关指标
     */
    public static String calculatePathMetrics(List<String> path) {
        if (path == null || path.isEmpty()) {
            return "空路径";
        }

        boolean hasSearch = path.contains("search");
        boolean hasProductDetail = path.contains("product_detail");
        boolean hasPayment = path.contains("payment");
        String startPage = path.get(0);
        String endPage = path.get(path.size() - 1);

        StringBuilder metrics = new StringBuilder();
        metrics.append("起始页: ").append(startPage);
        metrics.append(", 结束页: ").append(endPage);
        metrics.append(", 搜索: ").append(hasSearch ? "有" : "无");
        metrics.append(", 商品详情: ").append(hasProductDetail ? "有" : "无");
        metrics.append(", 支付: ").append(hasPayment ? "有" : "无");

        return metrics.toString();
    }

}
