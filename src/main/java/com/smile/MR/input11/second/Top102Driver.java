package com.smile.MR.input11.second;

import com.smile.MR.input11.Flow;
import com.smile.MR.input11.first.Top10Driver;
import com.smile.MR.input11.first.Top10OneMapper;
import com.smile.MR.input11.first.Top10OneReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/5 16:46
 * Description
 */
public class Top102Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\BigData\\output\\output11\\output1", "D:\\BigData\\output\\output11\\output2"};
        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(Top102Driver.class);

        // 3 关联map
        job.setMapperClass(Top10TwoMapper.class);
        //关联reducer
        job.setReducerClass(Top10TwoReducer.class);

        job.setMapOutputKeyClass(Flow.class);
        job.setMapOutputValueClass(NullWritable.class);

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
