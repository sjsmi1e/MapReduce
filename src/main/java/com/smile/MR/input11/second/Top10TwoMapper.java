package com.smile.MR.input11.second;

import com.smile.MR.input11.Flow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/5 16:48
 * Description
 */
public class Top10TwoMapper extends Mapper<LongWritable, Text, Flow, NullWritable> {
    private Flow k = new Flow();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        k.setPhoneNum(fields[0]);
        k.setUpflow(Long.parseLong(fields[1]));
        k.setDownflow(Long.parseLong(fields[2]));
        k.setSumflow(Long.parseLong(fields[3]));
        context.write(k, NullWritable.get());
    }
}
