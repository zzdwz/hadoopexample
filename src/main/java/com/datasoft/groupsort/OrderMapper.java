package com.datasoft.groupsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, Text> {
    OrderBean k = new OrderBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        k.setOrderId(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[2]));
        v.set(fields[1]);
        context.write(k, v);
    }
}
