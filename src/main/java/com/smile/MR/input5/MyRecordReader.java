package com.smile.MR.input5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 17:50
 * Description
 */
public class MyRecordReader extends RecordReader<Text, BytesWritable> {
    private FileSplit split;
    private Configuration configuration;
    private Text k = new Text();
    private BytesWritable v = new BytesWritable();
    private boolean isReady = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        split = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isReady) {
            FSDataInputStream fis = null;
            try {
                byte[] buf = new byte[(int) split.getLength()];
                //获取文件路径
                Path path = split.getPath();
                //获取文件系统
                FileSystem fileSystem = path.getFileSystem(configuration);
                //获取输入流
                fis = fileSystem.open(path);
                //读取
                IOUtils.readFully(fis, buf, 0, buf.length);
                k.set(path.toString());
                v.set(buf, 0, buf.length);
            } finally {
                fis.close();
                isReady = false;
                return true;
            }
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
