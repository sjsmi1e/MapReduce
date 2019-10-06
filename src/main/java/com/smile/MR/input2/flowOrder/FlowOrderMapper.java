package com.smile.MR.input2.flowOrder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 19:43
 * Description
 */
public class FlowOrderMapper extends Mapper<Text, Text, OrderBean, Text> {
    private OrderBean orderBean = new OrderBean();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split("\t");
        orderBean.setDownflow(Long.parseLong(s[0]));
        orderBean.setUpflow(Long.parseLong(s[1]));
        orderBean.setSumflow(Long.parseLong(s[2]));
        context.write(orderBean, key);
    }
}
