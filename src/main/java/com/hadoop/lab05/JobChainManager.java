package com.hadoop.lab05;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import com.hadoop.lab05.mapreduce.*;

/**
 * 作业链管理器
 * 使用ControlledJob和JobControl来管理多个MapReduce作业的执行顺序和依赖关系
 */
public class JobChainManager {

    /**
     * 处理作业链
     * 使用ControlledJob来管理作业依赖关系
     */
    public static int handleJobChain(String inputPath, String outputPath)
            throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        // 创建作业1: 用户行为分析
        Job job1 = createUserBehaviorJob(conf, inputPath, outputPath + "/user_behavior");
        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
        controlledJob1.setJob(job1);

        // 创建作业2: 用户画像分析
        Job job2 = createUserProfileJob(conf, inputPath, outputPath + "/user_profile");
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
        controlledJob2.setJob(job2);

        // 创建作业3: 商品分析
        Job job3 = createProductAnalysisJob(conf, inputPath, outputPath + "/product_analysis");
        ControlledJob controlledJob3 = new ControlledJob(job3.getConfiguration());
        controlledJob3.setJob(job3);

        // 创建作业4: 综合分析（依赖于前三个作业）
        Job job4 = createComprehensiveAnalysisJob(conf, outputPath + "/comprehensive_analysis");
        ControlledJob controlledJob4 = new ControlledJob(job4.getConfiguration());
        controlledJob4.setJob(job4);

        // 设置作业依赖关系
        controlledJob4.addDependingJob(controlledJob1);
        controlledJob4.addDependingJob(controlledJob2);
        controlledJob4.addDependingJob(controlledJob3);

        // 创建作业控制
        JobControl jc = new JobControl("JDataAnalysisChain");
        jc.addJob(controlledJob1);
        jc.addJob(controlledJob2);
        jc.addJob(controlledJob3);
        jc.addJob(controlledJob4);

        // 在单独线程中运行作业链
        Thread jcThread = new Thread(jc);
        jcThread.start();

        // 监控作业执行状态
        while (true) {
            if (jc.allFinished()) {
                System.out.println("所有作业完成！");
                System.out.println("成功完成的作业: " + jc.getSuccessfulJobList());
                jc.stop();
                return 0;
            }

            if (jc.getFailedJobList().size() > 0) {
                System.out.println("有作业失败！");
                System.out.println("失败的作业: " + jc.getFailedJobList());
                jc.stop();
                return 1;
            }

            // 等待一段时间再检查
            Thread.sleep(5000);
        }
    }

    /**
     * 创建用户行为分析作业
     */
    private static Job createUserBehaviorJob(Configuration conf, String inputPath, String outputPath)
            throws IOException {

        Job job = Job.getInstance(conf, "UserBehaviorAnalysis");
        job.setJarByClass(JobChainManager.class);

        job.setMapperClass(UserBehaviorMapper.class);
        job.setReducerClass(UserBehaviorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
        job.setJarByClass(JobChainManager.class);

        job.setMapperClass(UserProfileMapper.class);
        job.setReducerClass(UserProfileReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
        job.setJarByClass(JobChainManager.class);

        job.setMapperClass(ProductAnalysisMapper.class);
        job.setReducerClass(ProductAnalysisReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath + "/JData_Product.csv"));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    /**
     * 创建综合分析作业
     * 基于前面三个作业的结果进行综合分析
     */
    private static Job createComprehensiveAnalysisJob(Configuration conf, String outputPath)
            throws IOException {

        Job job = Job.getInstance(conf, "ComprehensiveAnalysis");
        job.setJarByClass(JobChainManager.class);

        // 这里可以添加一个综合分析Mapper和Reducer
        // 用于整合前面三个作业的结果

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }
}
