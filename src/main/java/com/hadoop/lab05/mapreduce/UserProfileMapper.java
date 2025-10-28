package com.hadoop.lab05.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 用户画像分析Mapper
 * 分析用户档案数据，统计用户分布特征
 */
public class UserProfileMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("user_id")) {
            return; // 跳过空行和标题行
        }

        try {
            String[] fields = line.split(",");
            if (fields.length >= 5) {
                String userId = fields[0];
                String age = fields[1];
                int sex = Integer.parseInt(fields[2]);
                int userLvCd = Integer.parseInt(fields[3]);
                String userRegTm = fields[4];

                // 输出1: 按年龄段统计
                outputKey.set("age_group");
                outputValue.set("age:" + age);
                context.write(outputKey, outputValue);

                // 输出2: 按性别统计
                outputKey.set("gender");
                outputValue.set("sex:" + sex);
                context.write(outputKey, outputValue);

                // 输出3: 按用户等级统计
                outputKey.set("user_level");
                outputValue.set("level:" + userLvCd);
                context.write(outputKey, outputValue);

                // 输出4: 按注册时间统计（按月份）
                if (!userRegTm.isEmpty()) {
                    String month = userRegTm.substring(0, 7); // 提取年月
                    outputKey.set("reg_month");
                    outputValue.set("month:" + month);
                    context.write(outputKey, outputValue);
                }

                // 输出5: 用户详细信息
                outputKey.set("user_detail_" + userId);
                outputValue.set("profile:" + age + "," + sex + "," + userLvCd + "," + userRegTm);
                context.write(outputKey, outputValue);

            }
        } catch (Exception e) {
            // 记录解析错误，但继续处理其他记录
            context.getCounter("UserProfileMapper", "PARSE_ERRORS").increment(1);
        }
    }
}
