package com.datasoft.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区测试只能在hdfs集群环境上，本地无法做分区测试
 */

public class SumFlowPartioner extends Partitioner<Text, Sumflow> {
    @Override
    public int getPartition(Text text, Sumflow sumflow, int i) {
        int idx = 4;
        String key = text.toString();
        key = key.substring(0, 3);
        if ("136".equals(key)){
            return 0;
        }else if("137".equals(key)){
            return 1;
        }else if("138".equals(key)){
            return 2;
        }else if("139".equals(key)){
            return 3;
        }
        return idx;
    }
}
