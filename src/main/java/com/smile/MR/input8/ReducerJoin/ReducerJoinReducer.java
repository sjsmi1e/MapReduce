package com.smile.MR.input8.ReducerJoin;

import com.smile.MR.input8.OrderBean;
import net.sf.cglib.beans.BeanCopier;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author smi1e
 * Date 2019/10/4 17:16
 * Description
 */
public class ReducerJoinReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {
    private final BeanCopier beanCopier = BeanCopier.create(OrderBean.class, OrderBean.class, false);

    @Override
    protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
        List<OrderBean> list = new ArrayList<OrderBean>();
        String pname = "";
        for (OrderBean value : values) {
            OrderBean t = null;
            if ("".equals(value.getPname())) {
                t = new OrderBean();
                beanCopier.copy(value, t, null);
                list.add(t);
            } else {
                pname = value.getPname();
            }
        }
        for (OrderBean orderBean : list) {
            orderBean.setPname(pname);
            context.write(orderBean, NullWritable.get());
        }
    }
}
