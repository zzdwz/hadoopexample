package com.datasoft.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fsdatasoftlog;
    FSDataOutputStream fsotherlog;

    public FilterRecordWriter(TaskAttemptContext taskAttemptContext) {
        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());

            fsdatasoftlog = fs.create(new Path("d:/tmp/datasoft.log"));
            fsotherlog = fs.create(new Path("d:/tmp/other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if (text.toString().contains("datasoft")){
            fsdatasoftlog.write(text.toString().getBytes());
        }else{
            fsotherlog.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fsdatasoftlog);
        IOUtils.closeStream(fsotherlog);
    }
}
