package com.smile.MR.input11.first;

import com.smile.MR.input11.Flow;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/5 16:35
 * Description
 */
public class Top10OneReducer extends Reducer<Text, Flow, Flow, NullWritable> {
    private Flow k = new Flow();

    @Override
    protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
        k.setPhoneNum(key.toString());
        long upflow = 0;
        long downflow = 0;
        for (Flow value : values) {
            upflow += value.getUpflow();
            downflow += value.getDownflow();
        }
        k.setUpflow(upflow);
        k.setDownflow(downflow);
        k.setSumflow(upflow + downflow);
        context.write(k, NullWritable.get());
    }
}
