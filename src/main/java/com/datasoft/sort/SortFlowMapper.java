package com.datasoft.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortFlowMapper extends Mapper<LongWritable, Text, SumFlow, Text> {
    SumFlow k = new SumFlow();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phoneNum = fields[0];
        Long upFlow = Long.parseLong(fields[1]);
        Long downFlow = Long.parseLong(fields[2]);
        Long sumFlow = Long.parseLong(fields[3]);

        v.set(phoneNum);
        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(sumFlow);

        context.write(k, v);

    }
}
