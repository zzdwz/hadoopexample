package com.datasoft.friendcircle;

import com.datasoft.topn.TopnDriver;
import com.datasoft.topn.TopnMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 查找相同共同粉丝
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * C:F,A,D,I
 * D:A,E,F,L
 * E:B,C,D,M,L
 * F:A,B,C,D,E,O,M
 * G:A,C,D,E,F
 * H:A,C,D,E,O
 * I:A,O
 * J:B,O
 * K:A,C,D
 * L:D,E,F
 * M:E,F,G
 * O:A,H,I,J
 * P:R
 *
 * 类似 A和B 的共同粉丝 C E
 *     A和C  的共同粉丝 F D
 *
 * 思路
 *
 * 第一遍 mapper 先以粉丝为KEY, 以粉丝粉的人为VALUE输出
 *
 * 第一遍reducer 以粉丝为KEY , 以粉丝粉的人的拼接字符串为输出
 *
 * 结果就是
 * KEY      A
 * VALUE    B,C,D,F,G,H,I,K,O
 *
 * 第二遍mapper
 *
 * 选将粉丝粉的人的字符串分割到字符串数组中,再用Arrays.sort对这个数组进行排序
 *
 * 然后依次将数组中的人组合,所以的组合都具有共同的粉丝A
 *
 * 类似于下面
 * B=C    A
 * B=D    A
 * B=F    A
 * B=G    A
 * B=H    A
 * B=I    A
 * B=K    A
 * B=O    A
 * C=D    A
 * C=F    A
 * ........
 * K=O    A
 *
 * 类似于一个冒泡排序
 *
 * 然后以所有的组合为 K   粉丝为VALUE输出到reducer
 *
 * 第二个reducer接到数据后 KEY即为人的组合， 集合中为人的组合的共同粉丝, 循环输出即可
 *
 *
 * 第二个mapperreduce需以第一个mapperreduce的输出结果为输入
 *
 */

public class FriendCircleDriver1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"d:/tmp/input_friendcircle", "d:/tmp/outputfriend1"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FriendCircleDriver1.class);

        job.setMapperClass(FriendCircleMapper1.class);
        job.setReducerClass(FriendCircleReducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 1 : 0);
    }
}
