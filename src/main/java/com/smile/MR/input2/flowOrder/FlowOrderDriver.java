package com.smile.MR.input2.flowOrder;

import com.smile.MR.input2.FlowBean;
import com.smile.MR.input2.FlowMapper;
import com.smile.MR.input2.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 19:35
 * Description 按总流量大小升序排序
 */
public class FlowOrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\BigData\\output\\output2", "D:\\BigData\\output\\output2+\\flowOrder"};
        Configuration configuration = new Configuration();
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
        Job job = Job.getInstance(configuration);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setJarByClass(FlowOrderDriver.class);

        job.setMapperClass(FlowOrderMapper.class);
        job.setReducerClass(FlowOrderReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(OrderBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
