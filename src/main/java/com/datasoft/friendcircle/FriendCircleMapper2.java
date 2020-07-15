package com.datasoft.friendcircle;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FriendCircleMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //A	F,D,O,I,H,B,K,G,C
        String[] line = value.toString().split("\t");

        v.set(line[0]);

        String[] persons = line[1].split(",");
        Arrays.sort(persons);

        for (int i = 0 ; i < persons.length - 1 ; i++){
            for (int j = i + 1; j < persons.length; j++){
                k.set(persons[i] + " " + persons[j]);
                context.write(k, v);
            }
        }

    }
}
