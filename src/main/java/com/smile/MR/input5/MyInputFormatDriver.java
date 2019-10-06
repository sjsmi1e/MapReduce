package com.smile.MR.input5;

import com.smile.MR.input4.NLineInputFormatDriver;
import com.smile.MR.input4.NLineInputFormatMapper;
import com.smile.MR.input4.NLineInputFormatReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 18:04
 * Description 自定义文件输入
 */
public class MyInputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\BigData\\input\\input5", "D:\\BigData\\output\\output5"};

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //设置文件输入输出形式
        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setJarByClass(MyInputFormatDriver.class);

        job.setMapperClass(MyInputFormatMapper.class);
        job.setReducerClass(MyInputFormatReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
