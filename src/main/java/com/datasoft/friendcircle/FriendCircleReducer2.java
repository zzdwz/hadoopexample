package com.datasoft.friendcircle;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendCircleReducer2 extends Reducer<Text, Text, Text, Text> {

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer("");
        for (Text value: values){
            sb.append(value).append(",");
        }
        String fans = sb.toString().substring(0, sb.length() - 1);
        v.set(fans);
        context.write(key, v);
    }
}
