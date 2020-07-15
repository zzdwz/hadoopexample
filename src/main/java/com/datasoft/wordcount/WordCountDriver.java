package com.datasoft.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计文本中单词出现的个数
 */

public class WordCountDriver{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        args = new String[]{"d:/tmp/input_wordcount", "d:/tmp/output_wordcount"};

        System.out.println("输入参数:" + args[0]);
        System.out.println("输出参数:" + args[1]);

        //获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置jar存储位置
        job.setJarByClass(WordCountDriver.class);

        //关联mapper和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setCombinerClass(WordCountReducer.class);

        //设置mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交job
        boolean result = job.waitForCompletion(true);
        //提交job
        System.exit(result ? 1 : 0);
    }
}
