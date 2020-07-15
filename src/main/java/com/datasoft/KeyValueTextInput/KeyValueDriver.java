package com.datasoft.KeyValueTextInput;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
 * 设定分隔符
 *
 *
 * job.setInputFormatClass(KeyValueTextInputFormat.class);
 * 设置文件读取方式为键值对读取
 *
 * mapper读取一行,将第一个分隔符前的字符串作为KEY,之后的字符串作为VALUE
 *
 *
 * wangzhen ni hao
 * hua bu hao
 * wangzhen ni hao ma wangzhen's test
 * wangzhen hello
 * wubo hui jia chi fa
 * hua la a
 *
 * 统计第一个字符开头的字符数
 */

public class KeyValueDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:/tmp/input_keyvalue","D:/tmp/output3"};

        System.out.println("输入参数:" + args[0]);
        System.out.println("输出参数:" + args[1]);

        //获取job对象
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        Job job = Job.getInstance(conf);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setJarByClass(KeyValueDriver.class);

        job.setMapperClass(KeyValueMapper.class);
        job.setReducerClass(KeyValueReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        //提交job
        System.exit(result ? 1 : 0);

    }

}
