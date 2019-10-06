package com.smile.MR.input11.second;

import com.smile.MR.input11.Flow;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/5 16:57
 * Description
 */
public class Top10TwoReducer extends Reducer<Flow, NullWritable, Flow, NullWritable> {
    private static int sum = 10;

    @Override
    protected void reduce(Flow key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        if (sum > 0) {
            sum--;
            context.write(key, NullWritable.get());
        } else {
            return;
        }
    }
}
