package com.datasoft.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class TopnReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    TreeMap<Integer, String> tm = new TreeMap<Integer, String>(new Comparator<Integer>() {
        @Override
        public int compare(Integer a, Integer b) {
            return b - a;
        }
    });

    Text k = new Text();
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value: values){
            tm.put(key.get(), value.toString());
            if (tm.size() > 3){
                tm.remove(tm.lastKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Iterator iter = tm.entrySet().iterator();



        while (iter.hasNext()){
            Map.Entry entry = (Map.Entry)iter.next();
            v.set((Integer) entry.getKey());
            k.set((String)entry.getValue());
            context.write(k, v);
        }
    }
}
