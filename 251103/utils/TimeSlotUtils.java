package com.stream.realtime.lululemon.utils;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author shuaiqi.chen
 * @create 2025-11-03-9:58
 */
public class TimeSlotUtils {

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * 根据时间字符串分析时间段
     */
    public static String parseTimeSlot(String formattedTime) {
        try {
            String timePart = formattedTime.split(" ")[1]; // 获取时间部分 HH:mm:ss
            String hour = timePart.split(":")[0];
            int hourInt = Integer.parseInt(hour);

            if (hourInt >= 0 && hourInt < 6) {
                return "凌晨(0-6点)";
            } else if (hourInt >= 6 && hourInt < 9) {
                return "早晨(6-9点)";
            } else if (hourInt >= 9 && hourInt < 12) {
                return "上午(9-12点)";
            } else if (hourInt >= 12 && hourInt < 14) {
                return "中午(12-14点)";
            } else if (hourInt >= 14 && hourInt < 18) {
                return "下午(14-18点)";
            } else if (hourInt >= 18 && hourInt < 22) {
                return "晚上(18-22点)";
            } else {
                return "深夜(22-24点)";
            }
        } catch (Exception e) {
            return "未知时段";
        }
    }

    /**
     * 提取日期部分
     */
    public static String extractDate(String formattedTime) {
        try {
            return formattedTime.split(" ")[0]; // yyyy-MM-dd
        } catch (Exception e) {
            return "未知日期";
        }
    }


}
