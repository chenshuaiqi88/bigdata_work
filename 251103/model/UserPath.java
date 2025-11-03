package com.stream.realtime.lululemon.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shuaiqi.chen
 * @create 2025-11-03-10:34
 */
public class UserPath {

    private String userId;
    private String date;
    private List<String> pathSequence = new ArrayList<>();  // 路径序列
    private int pathLength;                                 // 路径长度
    private boolean hasPurchase;                            // 是否包含购买
    private boolean hasSearch;                              // 是否包含搜索
    private long startTime;                                 // 路径开始时间
    private long endTime;                                   // 路径结束时间
    private long duration;                                  // 路径持续时间(毫秒)

    public UserPath() {}

    public UserPath(String userId, String date) {
        this.userId = userId;
        this.date = date;
    }

    // 添加页面到路径
    public void addPage(String page, long timestamp) {
        if (pathSequence.isEmpty()) {
            startTime = timestamp;
        }
        pathSequence.add(page);
        pathLength = pathSequence.size();
        endTime = timestamp;
        duration = endTime - startTime;

        // 更新行为标志
        if ("payment".equals(page)) {
            hasPurchase = true;
        } else if ("search".equals(page)) {
            hasSearch = true;
        }
    }

    // 合并路径
    public void merge(UserPath other) {
        if (other.pathSequence != null) {
            this.pathSequence.addAll(other.pathSequence);
            this.pathLength = this.pathSequence.size();
            this.hasPurchase = this.hasPurchase || other.hasPurchase;
            this.hasSearch = this.hasSearch || other.hasSearch;
            this.endTime = Math.max(this.endTime, other.endTime);
            this.duration = this.endTime - this.startTime;
        }
    }

    // Getters and Setters
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    public List<String> getPathSequence() { return pathSequence; }
    public void setPathSequence(List<String> pathSequence) { this.pathSequence = pathSequence; }

    public int getPathLength() { return pathLength; }
    public void setPathLength(int pathLength) { this.pathLength = pathLength; }

    public boolean isHasPurchase() { return hasPurchase; }
    public void setHasPurchase(boolean hasPurchase) { this.hasPurchase = hasPurchase; }

    public boolean isHasSearch() { return hasSearch; }
    public void setHasSearch(boolean hasSearch) { this.hasSearch = hasSearch; }

    public long getStartTime() { return startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }

    public long getEndTime() { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }

    public long getDuration() { return duration; }
    public void setDuration(long duration) { this.duration = duration; }

    @Override
    public String toString() {
        return String.format(
                "用户路径 - 用户ID: %s, 日期: %s, 路径: %s, 长度: %d, 购买: %s, 搜索: %s, 时长: %d秒",
                userId, date, String.join(" -> ", pathSequence), pathLength,
                hasPurchase ? "是" : "否", hasSearch ? "是" : "否", duration / 1000
        );
    }

}
