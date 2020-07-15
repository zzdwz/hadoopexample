package com.datasoft.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 排序
 *
 * 思路
 *
 * 序列化的bean实现WritableComparable接口,覆写compareTo方法
 */

public class SortFlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:/tmp/input_sumphone","D:/tmp/output2"};

        System.out.println("输入参数:" + args[0]);
        System.out.println("输出参数:" + args[1]);

        //获取job对象
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);



        //设置jar存储位置
        job.setJarByClass(SortFlowDriver.class);

        //关联mapper和reducer类
        job.setMapperClass(SortFlowMapper.class);
        job.setReducerClass(SortFlowRedurer.class);

        //设置mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(SumFlow.class);
        job.setMapOutputValueClass(Text.class);

        //设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SumFlow.class);

        //设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        //提交job
        System.exit(result ? 1 : 0);
    }
}
