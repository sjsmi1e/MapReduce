package com.smile.MR.input2.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author smi1e
 * Date 2019/10/3 19:20
 * Description
 */
public class ProvincePartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text text, Text text2, int i) {
        String pre = text.toString().substring(0, 3);
        if ("136".equals(pre)) {
            return 0;
        } else if ("137".equals(pre)) {
            return 1;
        } else if ("138".equals(pre)) {
            return 2;
        } else {
            return 3;
        }
    }
}
