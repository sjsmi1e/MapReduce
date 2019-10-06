package com.smile.MR.input3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 17:19
 * Description
 */
public class KeyValueTextInputFormatMapper extends Mapper<Text, Text, Text, IntWritable> {
    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, v);
    }
}
