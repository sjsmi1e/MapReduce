package com.smile.MR.input6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 20:24
 * Description
 */
public class GroupingComparatorMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        k.setOrderId(Integer.parseInt(split[0]));
        k.setPrice(Double.parseDouble(split[2]));
        context.write(k, NullWritable.get());
    }
}
