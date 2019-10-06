package com.smile.MR.input10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author smi1e
 * Date 2019/10/5 15:40
 * Description
 */
public class MoreJoinReducer extends Reducer<Text, Text, Text, Text> {
    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> k_num = new HashMap<>();
        for (Text value : values) {
            Integer t;
            if ((t = k_num.get(value.toString())) == null) {
                k_num.put(value.toString(), 1);
            } else {
                k_num.replace(value.toString(), t + 1);
            }
        }
        StringBuffer tv = new StringBuffer();
        k_num.forEach((o1, o2) -> tv.append(o1 + "-->" + o2 + " "));
        v.set(tv.toString());
        context.write(key, v);
    }
}
