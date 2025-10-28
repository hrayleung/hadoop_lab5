package com.hadoop.lab05.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.hadoop.lab05.model.UserAction;

/**
 * 用户行为分析Mapper
 * 分析用户行为模式，统计不同类型的行为数量
 */
public class UserBehaviorMapper extends Mapper<LongWritable, Text, Text, Text> {

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
            if (fields.length >= 7) {
                String userId = fields[0];
                String skuId = fields[1];
                String time = fields[2];
                String modelId = fields[3];
                int type = Integer.parseInt(fields[4]);
                String cate = fields[5];
                String brand = fields[6];

                // 输出1: 按用户统计行为类型
                outputKey.set(userId);
                outputValue.set("behavior:" + type);
                context.write(outputKey, outputValue);

                // 输出2: 按品类统计行为
                if (!cate.isEmpty() && !cate.equals("-1")) {
                    outputKey.set("cate_" + cate);
                    outputValue.set("behavior:" + type);
                    context.write(outputKey, outputValue);
                }

                // 输出3: 按品牌统计行为
                if (!brand.isEmpty() && !brand.equals("-1")) {
                    outputKey.set("brand_" + brand);
                    outputValue.set("behavior:" + type);
                    context.write(outputKey, outputValue);
                }

                // 输出4: 按时间统计行为（提取日期部分）
                if (!time.isEmpty()) {
                    String date = time.split(" ")[0]; // 提取日期部分
                    outputKey.set("date_" + date);
                    outputValue.set("behavior:" + type);
                    context.write(outputKey, outputValue);
                }

                // 输出5: 用户-商品行为统计
                outputKey.set("user_sku_" + userId + "_" + skuId);
                outputValue.set("behavior:" + type);
                context.write(outputKey, outputValue);

            }
        } catch (Exception e) {
            // 记录解析错误，但继续处理其他记录
            context.getCounter("UserBehaviorMapper", "PARSE_ERRORS").increment(1);
        }
    }
}
