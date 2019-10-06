package com.smile.MR.input8.MapperJoin;

import com.smile.MR.input8.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author smi1e
 * Date 2019/10/4 19:29
 * Description mapperJoin
 */
public class MapperJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        args = new String[]{"D:\\BigData\\input\\input8", "D:\\BigData\\output\\output8\\mapperJoin"};

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(MapperJoinDriver.class);

        job.setMapperClass(MapperJoinMapper.class);
        //设置不需要reducer
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置缓存（小表）路径
        job.addCacheFile(new URI("file:///D:/BigData/input/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
