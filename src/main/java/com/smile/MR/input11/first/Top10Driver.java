package com.smile.MR.input11.first;

import com.smile.MR.input11.Flow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/5 16:05
 * Description
 */
public class Top10Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\BigData\\input\\input11", "D:\\BigData\\output\\output11\\output1"};
        // 1 获取job信息
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
        Job job = Job.getInstance(conf);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 2 加载jar包
        job.setJarByClass(Top10Driver.class);

        // 3 关联map
        job.setMapperClass(Top10OneMapper.class);
        //关联reduce
        job.setReducerClass(Top10OneReducer.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flow.class);

        // 4 设置最终输出类型
        job.setOutputKeyClass(Flow.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 提交
        job.waitForCompletion(true);
    }
}
