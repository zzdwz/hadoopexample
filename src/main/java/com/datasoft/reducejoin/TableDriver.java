package com.datasoft.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 类似于表间关联（一个业务表，一个标准代码表，将业务表中的标准代码对应的值转为显示值）
 * order订单表
 * 1,100,3
 * 2,200,33
 * 3,200,78
 * 4,100,66
 * 5,300,88
 *
 * pd商品表
 * 100,野战蓝鲫
 * 200,纸飞机
 * 300,途酷三件套
 * 400,空名称
 *
 * 思路
 * 设计一个bean将两个表中的所有字段都装进去,bean中增加一个标志属性，标识为哪个表的数据
 * 利用切片信息对应的文件名为标志属性赋值
 * 将关联字段作为key, bean作为value写出mapper,输出的结果如下
 *
 * 100 1 3           order
 * 100 4 66          order
 * 100      野战蓝鲫  pd
 *
 * 在reduce阶段pd的数据只会是一条, 这条数据不需要输出,只作为填充order数据的pdname的来源
 * 只需要输出标志是order的数据即可
 */

public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"d:/tmp/input_reducejoin", "d:/tmp/output_reducejoin"};

        Configuration conf = new Configuration();
        Job job = new Job(conf);

        job.setJarByClass(TableDriver.class);

        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b? 1: 0);
    }
}
