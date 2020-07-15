package com.datasoft.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 按规则将数据输出到不同的文件中
 * www.baidu.com
 * www.sian.com
 * www.toutiao.com
 * www.tencetn.com
 * www.datasoft.com
 *
 * 将含有datasoft的行输出到d:/tmp/datasoft.log
 * 否则输出到d:/tmp/other.log
 */

public class OutputDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"d:/tmp/input_outputformt", "d:/tmp/output4"};

        Configuration conf = new Configuration();
        Job job = new Job(conf);

        job.setJarByClass(OutputDriver.class);

        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(OutputReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(FilterOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 1 : 0);
    }
}
