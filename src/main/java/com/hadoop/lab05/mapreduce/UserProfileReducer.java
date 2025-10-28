package com.hadoop.lab05.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 用户画像分析Reducer
 * 统计用户分布特征
 */
public class UserProfileReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String keyStr = key.toString();

        if (keyStr.startsWith("user_detail_")) {
            // 用户详细信息直接输出
            for (Text value : values) {
                context.write(key, value);
            }
        } else {
            // 统计分布数据
            Map<String, Integer> distribution = new HashMap<>();
            int totalCount = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts.length >= 2) {
                    String category = parts[0];
                    String valueStr = parts[1];

                    if (category.equals("age") || category.equals("sex") ||
                            category.equals("level") || category.equals("month")) {
                        distribution.put(valueStr, distribution.getOrDefault(valueStr, 0) + 1);
                        totalCount++;
                    }
                }
            }

            if (totalCount > 0) {
                // 构建统计结果
                StringBuilder result = new StringBuilder();
                result.append("total:").append(totalCount);

                // 添加各分类统计
                for (Map.Entry<String, Integer> entry : distribution.entrySet()) {
                    double percentage = (double) entry.getValue() / totalCount * 100;
                    result.append(",").append(entry.getKey()).append(":")
                            .append(entry.getValue()).append("(")
                            .append(String.format("%.2f%%", percentage)).append(")");
                }

                outputValue.set(result.toString());
                context.write(key, outputValue);
            }
        }
    }
}
