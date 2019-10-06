package com.smile.MR.input2.partition;

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
 * Date 2019/10/3 19:16
 * Description 分区
 */
public class PartitionDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\BigData\\output\\output2", "D:\\BigData\\output\\output2+\\partition"};
        Configuration configuration = new Configuration();
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
        Job job = Job.getInstance(configuration);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置分区类
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(4);

        job.setJarByClass(PartitionDriver.class);

        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
