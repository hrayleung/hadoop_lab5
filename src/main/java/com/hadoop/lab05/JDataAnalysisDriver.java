package com.hadoop.lab05;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.hadoop.lab05.mapreduce.*;

/**
 * JData分析主驱动程序
 * 执行多个MapReduce作业来分析JData数据集
 */
public class JDataAnalysisDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JDataAnalysisDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JDataAnalysisDriver <input_path> <output_path>");
            return -1;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // 创建作业链
        return handleJobChain(inputPath, outputPath);
    }

    /**
     * 处理作业链
     * 执行用户行为分析、用户画像分析、商品分析等多个作业
     */
    public static int handleJobChain(String inputPath, String outputPath)
            throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        // 作业1: 用户行为分析
        Job job1 = createUserBehaviorJob(conf, inputPath, outputPath + "/user_behavior");
        if (!job1.waitForCompletion(true)) {
            System.err.println("用户行为分析作业失败");
            return 1;
        }

        // 作业2: 用户画像分析
        Job job2 = createUserProfileJob(conf, inputPath, outputPath + "/user_profile");
        if (!job2.waitForCompletion(true)) {
            System.err.println("用户画像分析作业失败");
            return 1;
        }

        // 作业3: 商品分析
        Job job3 = createProductAnalysisJob(conf, inputPath, outputPath + "/product_analysis");
        if (!job3.waitForCompletion(true)) {
            System.err.println("商品分析作业失败");
            return 1;
        }

        System.out.println("所有分析作业完成！");
        return 0;
    }

    /**
     * 创建用户行为分析作业
     */
    private static Job createUserBehaviorJob(Configuration conf, String inputPath, String outputPath)
            throws IOException {

        Job job = Job.getInstance(conf, "UserBehaviorAnalysis");
        job.setJarByClass(JDataAnalysisDriver.class);

        // 设置Mapper和Reducer
        job.setMapperClass(UserBehaviorMapper.class);
        job.setReducerClass(UserBehaviorReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(inputPath + "/JData_Action_*.csv"));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    /**
     * 创建用户画像分析作业
     */
    private static Job createUserProfileJob(Configuration conf, String inputPath, String outputPath)
            throws IOException {

        Job job = Job.getInstance(conf, "UserProfileAnalysis");
        job.setJarByClass(JDataAnalysisDriver.class);

        // 设置Mapper和Reducer
        job.setMapperClass(UserProfileMapper.class);
        job.setReducerClass(UserProfileReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(inputPath + "/JData_User.csv"));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    /**
     * 创建商品分析作业
     */
    private static Job createProductAnalysisJob(Configuration conf, String inputPath, String outputPath)
            throws IOException {

        Job job = Job.getInstance(conf, "ProductAnalysis");
        job.setJarByClass(JDataAnalysisDriver.class);

        // 设置Mapper和Reducer
        job.setMapperClass(ProductAnalysisMapper.class);
        job.setReducerClass(ProductAnalysisReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(inputPath + "/JData_Product.csv"));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }
}
