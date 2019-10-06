package com.smile.MR.input8.ReducerJoin;

import com.smile.MR.input8.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/4 17:16
 * Description
 */
public class ReducerJoinMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
    private FileSplit split;
    private String name;
    private OrderBean v = new OrderBean();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //标记
        String[] fields = value.toString().split("\t");
        if (name.startsWith("order")) {
            v.setId(fields[0]);
            v.setPid(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setPname("");
            k.set(fields[1]);
        } else {
            v.setPid(fields[0]);
            v.setPname(fields[1]);
            v.setId("");
            v.setAmount(0);
            k.set(fields[0]);
        }
        context.write(k, v);
    }
}
