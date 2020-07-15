package com.datasoft.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class CacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap pdMap = new HashMap<String, String>();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(conf);

        System.out.println("localCacheFiles cnt:" + localCacheFiles.length);

        readCacheFile(localCacheFiles[0]);
    }

    private void readCacheFile(Path cacheFilePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(cacheFilePath.toUri().getPath()));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");
            pdMap.put(fields[0], fields[1]);
        }
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        line = fields[0] + "\t" + pdMap.get(fields[1]) + "\t" + fields[2];
        k.set(line);
        context.write(k, NullWritable.get());
    }
}
