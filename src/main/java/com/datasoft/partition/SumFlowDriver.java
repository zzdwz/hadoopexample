package com.datasoft.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 将电话产生的上行流量，下行流量按电话号码求和,并分文件输出
 *
 * 号码          访问的网址(可能为空)    上行流量  下行流量  状态
 * 13613881356	www.baidu.com	        12	   34	   200
 *
 *
 * 分区测试只能在hadoop集群上,本地无法进行分区测试
 * 打包jar时将pom文件中的mainclass改为  <mainClass>com.datasoft.partition.SumFlowDriver</mainClass>
 */

public class SumFlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        //args = new String[]{"D:/tmp/input_phone","D:/tmp/output_phone"};

        System.out.println("输入参数:" + args[0]);
        System.out.println("输出参数:" + args[1]);

        //获取job对象
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);



        //设置jar存储位置
        job.setJarByClass(SumFlowDriver.class);

        //关联mapper和reducer类
        job.setMapperClass(SumFlowMapper.class);
        job.setReducerClass(SumFlowReducer.class);

        //设置mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Sumflow.class);

        //设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Sumflow.class);

        job.setPartitionerClass(SumFlowPartioner.class);
        job.setNumReduceTasks(5);

        //设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        //提交job
        System.exit(result ? 1 : 0);
    }
}
