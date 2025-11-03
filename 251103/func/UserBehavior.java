package com.stream.realtime.lululemon.func;

/**
 * @author shuaiqi.chen
 * @create 2025-11-03-10:02
 */
public class UserBehavior {

    private String userId;
    private String logType;
    private String loginDate;
    private String loginTimeSlot;

    public UserBehavior() {}

    public UserBehavior(String userId, String logType, String loginDate, String loginTimeSlot) {
        this.userId = userId;
        this.logType = logType;
        this.loginDate = loginDate;
        this.loginTimeSlot = loginTimeSlot;
    }

    // Getters and Setters
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getLogType() { return logType; }
    public void setLogType(String logType) { this.logType = logType; }

    public String getLoginDate() { return loginDate; }
    public void setLoginDate(String loginDate) { this.loginDate = loginDate; }

    public String getLoginTimeSlot() { return loginTimeSlot; }
    public void setLoginTimeSlot(String loginTimeSlot) { this.loginTimeSlot = loginTimeSlot; }

}
