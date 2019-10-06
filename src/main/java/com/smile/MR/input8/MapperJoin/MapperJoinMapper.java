package com.smile.MR.input8.MapperJoin;

import com.smile.MR.input8.OrderBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author smi1e
 * Date 2019/10/4 19:31
 * Description
 */
public class MapperJoinMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private Map<String, String> pid_pname = new HashMap<String, String>();
    private Text k = new Text();
    private OrderBean v = new OrderBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存文件(小表)
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;
        while (StringUtils.isNotEmpty(line = br.readLine())) {
            String[] split = line.split("\t");
            pid_pname.put(split[0], split[1]);
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        v.setId(fields[0]);
        v.setPid(fields[1]);
        v.setAmount(Integer.parseInt(fields[2]));
        v.setPname(pid_pname.get(fields[1]));
        context.write(v, NullWritable.get());
    }
}
