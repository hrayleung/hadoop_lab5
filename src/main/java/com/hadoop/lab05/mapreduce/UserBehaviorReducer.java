package com.hadoop.lab05.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 用户行为分析Reducer
 * 统计各种维度的行为数据
 */
public class UserBehaviorReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // 统计不同类型行为的数量
        Map<Integer, Integer> behaviorCount = new HashMap<>();
        int totalBehaviors = 0;

        for (Text value : values) {
            String[] parts = value.toString().split(":");
            if (parts.length == 2 && parts[0].equals("behavior")) {
                try {
                    int behaviorType = Integer.parseInt(parts[1]);
                    behaviorCount.put(behaviorType,
                            behaviorCount.getOrDefault(behaviorType, 0) + 1);
                    totalBehaviors++;
                } catch (NumberFormatException e) {
                    // 忽略无效的行为类型
                }
            }
        }

        if (totalBehaviors > 0) {
            // 构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("total:").append(totalBehaviors);

            // 添加各类型行为统计
            for (Map.Entry<Integer, Integer> entry : behaviorCount.entrySet()) {
                result.append(",type").append(entry.getKey()).append(":").append(entry.getValue());
            }

            // 计算行为类型分布
            result.append(",distribution:");
            for (int i = 1; i <= 6; i++) {
                int count = behaviorCount.getOrDefault(i, 0);
                double percentage = totalBehaviors > 0 ? (double) count / totalBehaviors * 100 : 0;
                result.append("type").append(i).append(":").append(String.format("%.2f%%", percentage));
                if (i < 6)
                    result.append(",");
            }

            outputValue.set(result.toString());
            context.write(key, outputValue);
        }
    }
}
