package com.datasoft.KeyValueTextInput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KeyValueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable v = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (LongWritable value: values){
            i++;
        }
        if (i > 1){
            v.set(i);
            context.write(key, v);
        }
    }
}
