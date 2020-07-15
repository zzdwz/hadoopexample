package com.datasoft.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumFlowReducer extends Reducer<Text, Sumflow, Text, Sumflow> {
    Sumflow sumflow = new Sumflow();
    @Override
    protected void reduce(Text key, Iterable<Sumflow> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;


        for(Sumflow value: values){
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }

        sumflow.setUpFlow(sumUpFlow);
        sumflow.setDownFlow(sumDownFlow);
        sumflow.setSumFlow(sumUpFlow + sumDownFlow);

        context.write(key, sumflow);

    }
}
