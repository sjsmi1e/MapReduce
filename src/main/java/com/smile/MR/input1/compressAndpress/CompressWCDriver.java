package com.smile.MR.input1.compressAndpress;

import com.smile.MR.input1.WCDriver;
import com.smile.MR.input1.WCMapper;
import com.smile.MR.input1.WCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/5 10:01
 * Description
 */
public class CompressWCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\BigData\\input\\input1", "D:\\BigData\\output\\output1+"};
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();

        //开启map输出压缩
        configuration.setBooleanIfUnset("mapreduce.map.output.compress",true);
        //设置压缩格式
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(WCDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置combiner
        job.setCombinerClass(WCReducer.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //开启reduce输出压缩
        FileOutputFormat.setCompressOutput(job,true);
        //设置压缩格式
        FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);


        // 7 提交
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
