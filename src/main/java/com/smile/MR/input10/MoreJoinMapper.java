package com.smile.MR.input10;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/5 15:32
 * Description
 */
public class MoreJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String name;
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
        v.set(name);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        for (String s : split) {
            k.set(s);
            context.write(k, v);
        }
    }
}
