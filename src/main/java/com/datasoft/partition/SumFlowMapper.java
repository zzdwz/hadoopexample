package com.datasoft.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumFlowMapper extends Mapper<LongWritable, Text, Text, Sumflow> {

    Text key = new Text();
    Sumflow sumflow = new Sumflow();

    @Override
    protected void map(LongWritable k, Text v, Context context) throws IOException, InterruptedException {
        String line = v.toString();
        String[] values = line.split("\t");
        key.set(values[0]);
        sumflow.setUpFlow(Long.parseLong(values[values.length - 3]));
        sumflow.setDownFlow(Long.parseLong(values[values.length - 2]));

        context.write(key, sumflow);
    }
}
