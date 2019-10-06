package com.smile.MR.input7;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/4 16:46
 * Description
 */
public class MyOutputFormatReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    private Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        k.set(key.toString()+"\r\n");
        context.write(k, NullWritable.get());
    }
}
