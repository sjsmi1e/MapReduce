package com.smile.MR.input11;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/5 16:32
 * Description
 */
public class Top10OneMapper extends Mapper<Text, Text, Text, Flow> {
    private Flow v = new Flow();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        v.setUpflow(Long.parseLong(fields[fields.length - 3]));
        v.setDownflow(Long.parseLong(fields[fields.length - 2]));
        v.setSumflow(Long.parseLong(fields[fields.length - 1]));
        v.setPhoneNum(key.toString());
        context.write(key, v);
    }
}
