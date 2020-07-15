package com.datasoft.mapjoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 只能在hdfs集群模式下运行，本地报错
 *
 * 思路
 * 在mapper的setup阶段将driver中缓存的商品表加载到一个hashmap中,商品编号为key,商品名称为value
 *
 * 在mapper输出时,通过商品编号到hashmap中按key取值,替换订单中的商品编号为商品名称即可
 *
 */

public class MapjoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //缓存的表"d:/tmp/cache"
        //args = new String[]{"d:/tmp/input_mapjoin", "d:/tmp/output_mapjoin"};



        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new URI("hdfs:///user/cache/pd.txt"), conf);

        System.out.println("DistributedCache.addCacheFile success");


        Job job = Job.getInstance(conf);



        job.setJarByClass(MapjoinDriver.class);



        job.setMapperClass(CacheMapper.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);




        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //不需要 reduce阶段
        job.setNumReduceTasks(0);


        boolean b = job.waitForCompletion(true);

        System.exit(b? 1: 0);
    }
}
