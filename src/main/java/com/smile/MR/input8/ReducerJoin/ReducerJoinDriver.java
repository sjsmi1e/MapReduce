package com.smile.MR.input8.ReducerJoin;

import com.smile.MR.input7.MyOutputFormat;
import com.smile.MR.input7.MyOutputFormatDriver;
import com.smile.MR.input7.MyOutputFormatMapper;
import com.smile.MR.input7.MyOutputFormatReducer;
import com.smile.MR.input8.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/4 17:13
 * Description reducerJoin
 */
public class ReducerJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\BigData\\input\\input8", "D:\\BigData\\output\\output8\\reduceJoin"};

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ReducerJoinDriver.class);

        job.setMapperClass(ReducerJoinMapper.class);
        job.setReducerClass(ReducerJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
