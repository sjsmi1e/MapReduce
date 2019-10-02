package com.smile.MR.input2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/2 17:45
 * Description
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private FlowBean flowBean = new FlowBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String up = split[split.length - 3];
        String down = split[split.length - 2];
        flowBean.setUpflow(Long.parseLong(up));
        flowBean.setDownflow(Long.parseLong(down));
        k.set(split[1]);
        context.write(k, flowBean);
    }
}
