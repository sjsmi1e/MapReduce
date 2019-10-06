package com.smile.MR.input7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/4 16:45
 * Description
 */
public class MyOutputFormatMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        k.set(value.toString());
        context.write(k, NullWritable.get());
    }
}
