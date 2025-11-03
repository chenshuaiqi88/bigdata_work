package com.stream.realtime.lululemon.func;

import com.stream.realtime.lululemon.model.UserProfile;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author shuaiqi.chen
 * @create 2025-11-03-10:00
 */
public class UserProfileAggregator implements AggregateFunction<
        Tuple2<String, UserBehavior>,  // 输入: (userId, userBehavior)
        UserProfile,// 中间状态
        UserProfile                    // 输出
        > {

    @Override
    public UserProfile createAccumulator() {
        return new UserProfile();
    }

    @Override
    public UserProfile add(Tuple2<String, UserBehavior> value, UserProfile accumulator) {
        String userId = value.f0;
        UserBehavior behavior = value.f1;

        // 设置用户ID（第一次时设置）
        if (accumulator.getUserId() == null) {
            accumulator.setUserId(userId);
        }

        // 更新登录日期
        if (behavior.getLoginDate() != null) {
            accumulator.addLoginDate(behavior.getLoginDate());
        }

        // 更新登录时间段
        if (behavior.getLoginTimeSlot() != null) {
            accumulator.addLoginTimeSlot(behavior.getLoginTimeSlot());
        }

        // 更新行为标志
        if (behavior.getLogType() != null) {
            accumulator.updateBehavior(behavior.getLogType());
        }

        return accumulator;
    }

    @Override
    public UserProfile getResult(UserProfile accumulator) {
        return accumulator;
    }

    @Override
    public UserProfile merge(UserProfile a, UserProfile b) {
        // 合并登录日期
        a.getLoginDates().addAll(b.getLoginDates());

        // 合并登录时间段
        a.getLoginTimeSlots().addAll(b.getLoginTimeSlots());

        // 合并行为标志（使用或运算）
        a.setHasPurchase(a.isHasPurchase() || b.isHasPurchase());
        a.setHasSearch(a.isHasSearch() || b.isHasSearch());
        a.setHasBrowse(a.isHasBrowse() || b.isHasBrowse());

        return a;
    }
}
