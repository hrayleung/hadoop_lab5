package com.hadoop.lab05.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 商品分析Mapper
 * 分析商品数据，统计商品特征分布
 */
public class ProductAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("sku_id")) {
            return; // 跳过空行和标题行
        }

        try {
            String[] fields = line.split(",");
            if (fields.length >= 6) {
                String skuId = fields[0];
                String a1 = fields[1];
                String a2 = fields[2];
                String a3 = fields[3];
                String cate = fields[4];
                String brand = fields[5];

                // 输出1: 按品类统计
                if (!cate.isEmpty() && !cate.equals("-1")) {
                    outputKey.set("category");
                    outputValue.set("cate:" + cate);
                    context.write(outputKey, outputValue);
                }

                // 输出2: 按品牌统计
                if (!brand.isEmpty() && !brand.equals("-1")) {
                    outputKey.set("brand");
                    outputValue.set("brand:" + brand);
                    context.write(outputKey, outputValue);
                }

                // 输出3: 按属性1统计
                if (!a1.isEmpty() && !a1.equals("-1")) {
                    outputKey.set("attribute1");
                    outputValue.set("a1:" + a1);
                    context.write(outputKey, outputValue);
                }

                // 输出4: 按属性2统计
                if (!a2.isEmpty() && !a2.equals("-1")) {
                    outputKey.set("attribute2");
                    outputValue.set("a2:" + a2);
                    context.write(outputKey, outputValue);
                }

                // 输出5: 按属性3统计
                if (!a3.isEmpty() && !a3.equals("-1")) {
                    outputKey.set("attribute3");
                    outputValue.set("a3:" + a3);
                    context.write(outputKey, outputValue);
                }

                // 输出6: 商品详细信息
                outputKey.set("product_detail_" + skuId);
                outputValue.set("product:" + a1 + "," + a2 + "," + a3 + "," + cate + "," + brand);
                context.write(outputKey, outputValue);

            }
        } catch (Exception e) {
            // 记录解析错误，但继续处理其他记录
            context.getCounter("ProductAnalysisMapper", "PARSE_ERRORS").increment(1);
        }
    }
}
