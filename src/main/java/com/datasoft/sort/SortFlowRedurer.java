package com.datasoft.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortFlowRedurer extends Reducer<SumFlow, Text, Text, SumFlow> {
    @Override
    protected void reduce(SumFlow key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value: values){
            context.write(value, key);
        }
    }
}
