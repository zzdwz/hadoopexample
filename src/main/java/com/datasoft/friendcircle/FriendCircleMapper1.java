package com.datasoft.friendcircle;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendCircleMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    Text v = new Text();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //A:B,C,D,F,E,O
        String[] line = value.toString().split(":");

        v.set(line[0]);

        String[] fans = line[1].split(",");
        for(String fan: fans){
            k.set(fan);
            context.write(k, v);
        }

    }
}
