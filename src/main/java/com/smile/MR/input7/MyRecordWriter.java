package com.smile.MR.input7;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/4 16:50
 * Description
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    public MyRecordWriter(TaskAttemptContext taskAttemptContext) {
        try {
            //获得文件系统
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            //创建输出流
            atguigu = fs.create(new Path("D:\\BigData\\output\\output7\\atguigu.log"));
            other = fs.create(new Path("D:\\BigData\\output\\output7\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        String line = key.toString();
        if (line.contains("atguigu")) {
            atguigu.write(line.getBytes());
        } else {
            other.write(line.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
