package com.datasoft.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计总流量的的前N名 （最后一个数值为总流量)
 *
 * 13613881356	12	34	46
 * 13677878888	11	21	32
 * 13713954544	33	87	120
 * 13726736377	11	34	45
 * 13813881356	21	98	119
 * 13877738987	104	54	158
 * 13977878888	55	87	142
 * 88767662	98	11	109
 *
 * 思路
 * mapper阶段统计各个切片的前N名
 * reducer阶段统计所有归并过的数据的前N名
 *
 * 利用TreeMap的有序性
 * 循环读一行数据将总流量作为TreeMap的key,其它的数据做为value插入到TreeMap中
 *
 * 如果TreeMap的size超过N ，将按照升序（降序）的规则删除 TreeMap的第一个键值（最后一个键值)
 *
 */

public class TopnDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"d:/tmp/input_topn", "d:/tmp/output_topn"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(TopnDriver.class);

        job.setMapperClass(TopnMapper.class);
        job.setReducerClass(TopnReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 1 : 0);
    }
}
