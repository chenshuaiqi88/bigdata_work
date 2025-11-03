package com.stream.realtime.lululemon.func;

import com.stream.realtime.lululemon.model.UserPath;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author shuaiqi.chen
 * @create 2025-11-03-10:46
 */
public class UserPathAggregator implements AggregateFunction<
        Tuple2<String, UserPath>,  // 输入: (userId, userPath)
        UserPath,                  // 中间状态
        UserPath                   // 输出
        >  {

    @Override
    public UserPath createAccumulator() {
        return new UserPath();
    }

    @Override
    public UserPath add(Tuple2<String, UserPath> value, UserPath accumulator) {
        String userId = value.f0;
        UserPath newPath = value.f1;

        // 设置用户ID和日期（第一次时设置）
        if (accumulator.getUserId() == null) {
            accumulator.setUserId(userId);
            accumulator.setDate(newPath.getDate());
        }

        // 合并路径
        accumulator.merge(newPath);

        return accumulator;
    }

    @Override
    public UserPath getResult(UserPath accumulator) {
        return accumulator;
    }

    @Override
    public UserPath merge(UserPath a, UserPath b) {
        a.merge(b);
        return a;
    }

}
