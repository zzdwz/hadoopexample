package com.datasoft.topn;

import org.apache.commons.io.input.TeeInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class TopnMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    TreeMap<Integer, String> tm = new TreeMap<Integer, String>();
    IntWritable k = new IntWritable();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields1 = value.toString().split("\t");


        tm.put(Integer.parseInt(fields1[3]), fields1[0] + "\t" + fields1[1] + "\t" + fields1[2]);
        if (tm.size() > 3){
            tm.remove(tm.firstKey());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator iter = tm.entrySet().iterator();


        while (iter.hasNext()){
            Map.Entry entry = (Map.Entry)iter.next();
            k.set((Integer) entry.getKey());
            v.set((String)entry.getValue());
            context.write(k, v);
        }
    }
}
