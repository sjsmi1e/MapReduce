package com.smile.MR.input5;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 18:05
 * Description
 */
public class MyInputFormatMapper extends Mapper<Text, BytesWritable,Text, BytesWritable> {
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
