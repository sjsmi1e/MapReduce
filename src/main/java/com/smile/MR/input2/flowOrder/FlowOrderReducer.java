package com.smile.MR.input2.flowOrder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 19:47
 * Description
 */
public class FlowOrderReducer extends Reducer<OrderBean, Text, Text, OrderBean> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), key);
    }
}
