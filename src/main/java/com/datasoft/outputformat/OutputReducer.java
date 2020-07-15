package com.datasoft.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OutputReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String s = key.toString() + "\r\n";
        k.set(s);
        for(NullWritable value: values){

            context.write(k, NullWritable.get());
        }
    }
}
