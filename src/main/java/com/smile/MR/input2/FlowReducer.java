package com.smile.MR.input2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/2 17:53
 * Description
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int up = 0;
        int down = 0;
        for (FlowBean value : values) {
            up += value.getUpflow();
            down += value.getDownflow();
        }
        flowBean.setUpflow(up);
        flowBean.setDownflow(down);
        flowBean.setSumflow(up + down);
        context.write(key, flowBean);
    }
}
