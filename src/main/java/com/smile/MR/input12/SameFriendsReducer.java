package com.smile.MR.input12;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/5 17:23
 * Description
 */
public class SameFriendsReducer extends Reducer<Text, Text, Text, Text> {

    private Text val = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer v = new StringBuffer();
        for (Text value : values) {
            v.append(value.toString() + " ");
        }
        val.set(v.toString());
        context.write(key, val);
    }
}
