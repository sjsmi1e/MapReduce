package com.smile.MR.input7;

import com.smile.MR.input6.GroupingComparatorDriver;
import com.smile.MR.input6.GroupingComparatorGroupingComparator;
import com.smile.MR.input6.GroupingComparatorMapper;
import com.smile.MR.input6.GroupingComparatorReducer;
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
 * Date 2019/10/4 16:44
 * Description 自定义输出
 */
public class MyOutputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\BigData\\input\\input7", "D:\\BigData\\output\\output7"};

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(MyOutputFormatDriver.class);

        job.setMapperClass(MyOutputFormatMapper.class);
        job.setReducerClass(MyOutputFormatReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //设置输出
        job.setOutputFormatClass(MyOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
