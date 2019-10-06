package com.smile.MR.input6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 20:24
 * Description
 */
public class GroupingComparatorDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\BigData\\input\\input6", "D:\\BigData\\output\\output6"};

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(GroupingComparatorDriver.class);

        job.setMapperClass(GroupingComparatorMapper.class);
        job.setReducerClass(GroupingComparatorReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置分组
        job.setGroupingComparatorClass(GroupingComparatorGroupingComparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
