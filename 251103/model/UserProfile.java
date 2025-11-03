package com.stream.realtime.lululemon.model;

import java.util.HashSet;
import java.util.Set;

/**
 * @author shuaiqi.chen
 * @create 2025-11-03-9:59
 */
public class UserProfile {

    private String userId;
    private Set<String> loginDates = new HashSet<>();  // 登录日期集合
    private boolean hasPurchase = false;               // 是否有购买行为
    private boolean hasSearch = false;                 // 是否有搜索行为
    private boolean hasBrowse = false;                 // 是否有浏览行为
    private Set<String> loginTimeSlots = new HashSet<>(); // 登录时间段

    public UserProfile() {}

    public UserProfile(String userId) {
        this.userId = userId;
    }

    // 添加登录日期
    public void addLoginDate(String date) {
        this.loginDates.add(date);
    }

    // 添加登录时间段
    public void addLoginTimeSlot(String timeSlot) {
        this.loginTimeSlots.add(timeSlot);
    }

    // 更新行为标志
    public void updateBehavior(String logType) {
        switch (logType) {
            case "payment":
                this.hasPurchase = true;
                break;
            case "search":
                this.hasSearch = true;
                break;
            case "product_list":
            case "product_detail":
            case "home":
                this.hasBrowse = true;
                break;
        }
    }

    // Getters and Setters
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public Set<String> getLoginDates() { return loginDates; }
    public void setLoginDates(Set<String> loginDates) { this.loginDates = loginDates; }

    public boolean isHasPurchase() { return hasPurchase; }
    public void setHasPurchase(boolean hasPurchase) { this.hasPurchase = hasPurchase; }

    public boolean isHasSearch() { return hasSearch; }
    public void setHasSearch(boolean hasSearch) { this.hasSearch = hasSearch; }

    public boolean isHasBrowse() { return hasBrowse; }
    public void setHasBrowse(boolean hasBrowse) { this.hasBrowse = hasBrowse; }

    public Set<String> getLoginTimeSlots() { return loginTimeSlots; }
    public void setLoginTimeSlots(Set<String> loginTimeSlots) { this.loginTimeSlots = loginTimeSlots; }

    @Override
    public String toString() {
        return String.format(
                "用户ID: %s, 登录天数: %d天(%s), 购买: %s, 搜索: %s, 浏览: %s, 登录时段: %s",
                userId, loginDates.size(), String.join(",", loginDates),
                hasPurchase ? "是" : "否", hasSearch ? "是" : "否", hasBrowse ? "是" : "否",
                String.join(",", loginTimeSlots)
        );
    }

}
