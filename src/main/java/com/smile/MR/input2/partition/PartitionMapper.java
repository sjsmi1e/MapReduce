package com.smile.MR.input2.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 19:19
 * Description
 */
public class PartitionMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
