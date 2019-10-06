package com.smile.MR.input3;

import com.smile.MR.input2.FlowBean;
import com.smile.MR.input2.FlowMapper;
import com.smile.MR.input2.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 17:18
 * Description KeyValueTextInputFormat分割案例
 */
public class KeyValueTextInputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\BigData\\input\\input3", "D:\\BigData\\output\\output3"};

        Configuration configuration = new Configuration();
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        Job job = Job.getInstance(configuration);
        //设置KeyValueTextInputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setJarByClass(KeyValueLineRecordReader.class);

        job.setMapperClass(KeyValueTextInputFormatMapper.class);
        job.setReducerClass(KeyValueTextInputFormatReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
