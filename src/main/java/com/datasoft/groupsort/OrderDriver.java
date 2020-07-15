package com.datasoft.groupsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 1000001	P101	22.98
 * 1000002	P009	298
 * 1000003	P033	890
 * 1000001	P102	33.12
 * 1000001	P305	67
 * 1000002	P309	11
 * 1000003	P009	88
 * 1000003	P200	1890
 *
 * 求每个订单的最大值
 *
 * 思路
 *
 * 先按订单号和价格排序 （设计orderbean实现WritableComparable接口)后做为组合的key输出到reduce
 *
 * 在reduce端利用groupingcomparator将订单id相同的kv聚合成组，然后取第一个即是最大值
 *
 * job.setGroupingComparatorClass(OrderGroupComp.class)作用就是将key中orderid相同作为一组
 *
 *
 */

public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"d:/tmp/input_order", "d:/tmp/output3"};

        Configuration conf = new Configuration();
        Job job = new Job(conf);

        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(OrderGroupComp.class);



        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 1 : 0);

    }
}
