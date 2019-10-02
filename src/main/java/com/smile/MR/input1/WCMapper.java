package com.smile.MR.input1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/2 17:12
 * Description
 */
public class WCMapper extends Mapper<LongWritable,Text,Text, IntWritable> {
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //输入行数切分
        String[] split = value.toString().split(" ");
        //遍历循环
        for (String s : split) {
            k.set(s);
            context.write(k,v);
        }
    }
}
