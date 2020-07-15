package com.datasoft.groupsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, Text, OrderBean, Text> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        int i = 0;
        for (Text value: values){


            if (i < 1){
                context.write(key, value);

            }
            i++;

            if (i == 1){
                break;
            }
        }




    }
}
